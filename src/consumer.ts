import {
  TEventArgs,
  TEventName,
  TTypedEventEmitterOptions,
  TypedEventEmitter,
} from '@prismamedia/ts-typed-event-emitter';
import type { ConfirmChannel, Message, Options } from 'amqplib';
import { errorMonitor } from 'events';
import { SignalConstants } from 'os';
import { clearTimeout, setTimeout } from 'timers';
import type { Client, TQueueName } from './client';

function computeSpentTimeInMs(since: bigint): number {
  return Math.round(Number(process.hrtime.bigint() - since) / 1000000);
}

export type TSignal = keyof SignalConstants;

export type TConsumerTag = string;

export type TConsumerCallbackArgs<TPayload> = {
  /**
   * The payload
   */
  payload: TPayload;

  /**
   * The whole message
   */
  message: Message;
};

export type TConsumerCallback<TPayload, TResult> = (
  args: TConsumerCallbackArgs<TPayload>,
) => Promise<TResult>;

export enum ConsumerStatus {
  Idle = 'IDLE',
  Consuming = 'CONSUMING',
}

export enum ConsumerEventKind {
  Stopped = 'STOPPED',
  Started = 'STARTED',
  ChannelError = 'CHANNEL_ERROR',
  ChannelClosed = 'CHANNEL_CLOSED',
  ChannelOpened = 'CHANNEL_OPENED',
  MessageParsed = 'MESSAGE_PARSED',
  MessageRejected = 'MESSAGE_REJECTED',
  MessageRequeued = 'MESSAGE_REQUEUED',
  MessageAcknowledged = 'MESSAGE_ACKNOWLEDGED',
  MessageCallbackError = 'MESSAGE_CALLBACK_ERROR',
}

export type TConsumerReply = {
  replyTo: string;
  correlationId: string;
};

export type TConsumerEvents<TPayload, TResult> = {
  [ConsumerEventKind.Stopped]: [];
  [ConsumerEventKind.Started]: [];
  [ConsumerEventKind.ChannelError]: [Error];
  [ConsumerEventKind.ChannelClosed]: [];
  [ConsumerEventKind.ChannelOpened]: [ConfirmChannel];
  [ConsumerEventKind.MessageParsed]: [
    {
      message: Message;
      payload: TPayload;
      tookInMs: number;
    },
  ];
  [ConsumerEventKind.MessageRejected]: [
    {
      message: Message;
      payload?: TPayload;
      error: Error;
      tookInMs: number;
    },
  ];
  [ConsumerEventKind.MessageRequeued]: [
    {
      message: Message;
      payload: TPayload;
      error: Error;
      tookInMs: number;
    },
  ];
  [ConsumerEventKind.MessageAcknowledged]: [
    {
      message: Message;
      payload: TPayload;
      result: TResult;
      reply?: TConsumerReply;
      tookInMs: number;
    },
  ];
  [ConsumerEventKind.MessageCallbackError]: [
    {
      message: Message;
      payload: TPayload;
      error: Error;
      tookInMs: number;
    },
  ];
};

export type TConsumerOptions<TPayload, TResult> = Omit<
  Options.Consume,
  'noLocal'
> & {
  /**
   * @see https://www.rabbitmq.com/consumer-prefetch.html
   */
  prefetch?: number;

  /**
   * After "consumeInMs" ms, the consumer will stop
   */
  consumeInMs?: number;

  /**
   * After "idleInMs" ms with no message, the consumer will stop
   */
  idleInMs?: number;

  /**
   * In case of uncaught "callback error", either requeue (true) or reject (false) the message
   * Default: false
   */
  requeueOnError?: boolean;

  /**
   * In case of uncaught "callback error", either stop (true) or continues (false) the consumer
   * Default: false
   */
  stopOnError?: boolean;

  /**
   * We can try to gracefully stop the consumer on "known" signals
   * - true = ['SIGINT', 'SIGTERM', 'SIGQUIT']
   * - false = []
   *
   * Default: false / none
   */
  stopOnSignals?: boolean | TSignal[];

  /**
   * Add some event listeners
   */
  on?: TTypedEventEmitterOptions<TConsumerEvents<TPayload, TResult>>;
};

export class Consumer<TPayload = any, TResult = any> extends TypedEventEmitter<
  TConsumerEvents<TPayload, TResult>
> {
  #status: ConsumerStatus = ConsumerStatus.Idle;
  #tag: TConsumerTag | null = null;
  #channel: Promise<ConfirmChannel> | null = null;
  #consumeTimeoutId: ReturnType<typeof setTimeout> | null = null;
  #idleTimeoutId: ReturnType<typeof setTimeout> | null = null;
  #signalHandler = (async () => this.stop()).bind(this);

  public readonly prefetch: number;
  public readonly consumeInMs: number | null;
  public readonly idleInMs: number | null;
  public readonly requeueOnError: boolean;
  public readonly stopOnError: boolean;
  public readonly stopOnSignalSet: ReadonlySet<TSignal>;
  public readonly options: Options.Consume;

  public constructor(
    public readonly client: Client,
    public readonly queueName: TQueueName,
    public readonly callback: TConsumerCallback<TPayload, TResult>,
    {
      prefetch = 1,
      consumeInMs,
      idleInMs,
      requeueOnError = false,
      stopOnError = false,
      stopOnSignals = false,
      on,
      ...options
    }: TConsumerOptions<TPayload, TResult> = {},
  ) {
    super(on);

    this.prefetch = prefetch;
    this.consumeInMs = consumeInMs ?? null;
    this.idleInMs = idleInMs ?? null;
    this.requeueOnError = requeueOnError;
    this.stopOnError = stopOnError;
    this.stopOnSignalSet = new Set(
      typeof stopOnSignals === 'boolean'
        ? stopOnSignals
          ? ['SIGINT', 'SIGTERM', 'SIGQUIT']
          : []
        : stopOnSignals,
    );
    this.options = options;

    this.on(ConsumerEventKind.Stopped, () => {
      this.#status = ConsumerStatus.Idle;
      this.#tag = null;
      this.clearConsumeTimeout();
      this.clearIdleTimeout();
      this.clearSignalHandler();
    })
      .on(errorMonitor, () => {
        if (this.isConsuming()) {
          this.emit(ConsumerEventKind.Stopped);
        }
      })
      .on(ConsumerEventKind.ChannelClosed, () => {
        if (this.isConsuming()) {
          this.emit(ConsumerEventKind.Stopped);
        }
      });
  }

  public getStatus(): ConsumerStatus {
    return this.#status;
  }

  public isConsuming(): boolean {
    return this.#status === ConsumerStatus.Consuming;
  }

  public getTag(): TConsumerTag | null {
    return this.#tag;
  }

  public async getChannel(): Promise<ConfirmChannel> {
    if (!this.#channel) {
      const channel = (
        await (this.#channel = new Promise(async (resolve, reject) => {
          try {
            const connection = await this.client.getConnection();
            const channel = await connection.createConfirmChannel();

            resolve(channel);
          } catch (error) {
            reject(error);
          }
        }))
      )
        // The "close" event is always fired after the "error" one
        .on('error', (error: Error) =>
          this.emit(ConsumerEventKind.ChannelError, error),
        )
        .on('close', () => {
          this.#channel = null;

          this.emit(ConsumerEventKind.ChannelClosed);
        });

      await channel.prefetch(this.prefetch);

      this.emit(ConsumerEventKind.ChannelOpened, channel);
    }

    return this.#channel;
  }

  protected ack(
    channel: ConfirmChannel,
    ...args: TEventArgs<
      TConsumerEvents<TPayload, TResult>,
      ConsumerEventKind.MessageAcknowledged
    >
  ): void {
    const [{ message }] = args;

    try {
      channel.ack(message);
      this.emit(ConsumerEventKind.MessageAcknowledged, ...args);
    } catch (error) {
      if (error instanceof Error && error.name === 'IllegalOperationError') {
        this.isConsuming() && this.emit(ConsumerEventKind.Stopped);
      } else {
        throw error;
      }
    }
  }

  protected requeue(
    channel: ConfirmChannel,
    ...args: TEventArgs<
      TConsumerEvents<TPayload, TResult>,
      ConsumerEventKind.MessageRequeued
    >
  ): void {
    const [{ message }] = args;

    try {
      channel.nack(message, false, true);
      this.emit(ConsumerEventKind.MessageRequeued, ...args);
    } catch (error) {
      if (error instanceof Error && error.name === 'IllegalOperationError') {
        this.isConsuming() && this.emit(ConsumerEventKind.Stopped);
      } else {
        throw error;
      }
    }
  }

  protected reject(
    channel: ConfirmChannel,
    ...args: TEventArgs<
      TConsumerEvents<TPayload, TResult>,
      ConsumerEventKind.MessageRejected
    >
  ): void {
    const [{ message }] = args;

    try {
      channel.nack(message, false, false);
      this.emit(ConsumerEventKind.MessageRejected, ...args);
    } catch (error) {
      if (error instanceof Error && error.name === 'IllegalOperationError') {
        this.isConsuming() && this.emit(ConsumerEventKind.Stopped);
      } else {
        throw error;
      }
    }
  }

  protected clearConsumeTimeout(): void {
    if (this.#consumeTimeoutId) {
      clearTimeout(this.#consumeTimeoutId);
      this.#consumeTimeoutId = null;
    }
  }

  protected setConsumeTimeout(): void {
    this.clearConsumeTimeout();

    this.#consumeTimeoutId =
      this.consumeInMs !== null
        ? setTimeout(
            () =>
              this.stop().catch(() => {
                // Do nothing, it's already stopped
              }),
            this.consumeInMs,
          )
        : null;
  }

  protected clearIdleTimeout(): void {
    if (this.#idleTimeoutId) {
      clearTimeout(this.#idleTimeoutId);
      this.#idleTimeoutId = null;
    }
  }

  protected setIdleTimeout(): void {
    this.clearIdleTimeout();

    this.#idleTimeoutId =
      this.idleInMs !== null
        ? setTimeout(
            () =>
              this.stop().catch(() => {
                // Do nothing, it's already stopped
              }),
            this.idleInMs,
          )
        : null;
  }

  protected clearSignalHandler(): void {
    this.stopOnSignalSet.forEach((signal) =>
      process.removeListener(signal, this.#signalHandler),
    );
  }

  protected setSignalHandler(): void {
    this.clearSignalHandler();

    this.stopOnSignalSet.forEach((signal) =>
      process.on(signal, this.#signalHandler),
    );
  }

  public async start(): Promise<void> {
    if (this.isConsuming()) {
      throw new Error(
        `AMQP consumer workflow error: cannot start a "${
          this.#status
        }" consumer`,
      );
    }

    const channel = await this.getChannel();

    const { consumerTag: tag } = await channel.consume(
      this.queueName,
      async (message) => {
        // If the consumer has been cancelled by RabbitMQ, the message callback will be invoked with null.
        if (message === null) {
          this.emit(ConsumerEventKind.Stopped);
        } else {
          const start = process.hrtime.bigint();

          this.setIdleTimeout();

          try {
            if (message.properties.contentType !== 'application/json') {
              // The message's "Content-Type" is not supported, reject it
              return this.reject(channel, {
                message,
                error: new Error(
                  `The Content-Type "${String(
                    message.properties.contentType,
                  )}" is not supported, the message has been rejected`,
                ),
                tookInMs: computeSpentTimeInMs(start),
              });
            }

            let payload: TPayload;

            try {
              payload = JSON.parse(message.content.toString());
            } catch (error: any) {
              // The message is not valid JSON, reject it
              return this.reject(channel, {
                message,
                error,
                tookInMs: computeSpentTimeInMs(start),
              });
            }

            this.emit(ConsumerEventKind.MessageParsed, {
              message,
              payload,
              tookInMs: computeSpentTimeInMs(start),
            });

            try {
              const result = await this.callback({ message, payload });

              let reply: TConsumerReply | undefined;
              if (
                message.properties.replyTo &&
                message.properties.correlationId
              ) {
                await this.client.publish(
                  '',
                  message.properties.replyTo,
                  result,
                  { correlationId: message.properties.correlationId },
                );

                reply = {
                  replyTo: message.properties.replyTo,
                  correlationId: message.properties.correlationId,
                };
              }

              return this.ack(channel, {
                message,
                payload,
                result,
                reply,
                tookInMs: computeSpentTimeInMs(start),
              });
            } catch (error: any) {
              this.emit(ConsumerEventKind.MessageCallbackError, {
                message,
                payload,
                error,
                tookInMs: computeSpentTimeInMs(start),
              });

              if (this.stopOnError) {
                await this.stop();
              }

              // The callback has failed, requeue or reject the message
              return this.requeueOnError
                ? this.requeue(channel, {
                    message,
                    payload,
                    error,
                    tookInMs: computeSpentTimeInMs(start),
                  })
                : this.reject(channel, {
                    message,
                    payload,
                    error,
                    tookInMs: computeSpentTimeInMs(start),
                  });
            }
          } catch (error: any) {
            // Despite our best efforts, an error has not been caught "properly"
            this.emit('error', error);
          }
        }
      },
      this.options,
    );

    this.#status = ConsumerStatus.Consuming;
    this.#tag = tag;
    this.setConsumeTimeout();
    this.setIdleTimeout();
    this.setSignalHandler();

    this.emit(ConsumerEventKind.Started);
  }

  public async stop(): Promise<void> {
    if (!this.isConsuming()) {
      throw new Error(
        `AMQP consumer workflow error: cannot stop a "${
          this.#status
        }" consumer`,
      );
    }

    if (!this.#tag) {
      throw new Error(
        `AMQP consumer workflow error: cannot stop a "${
          this.#status
        }" consumer without a "tag"`,
      );
    }

    const channel = await this.getChannel();
    await channel.cancel(this.#tag);

    this.emit(ConsumerEventKind.Stopped);
  }

  public async startAndWait<
    TName extends TEventName<TConsumerEvents<TPayload, TResult>>,
  >(...names: TName[]) {
    await this.start();

    return this.wait(...names);
  }
}
