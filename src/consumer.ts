import type { ConfirmChannel, Message, Options } from 'amqplib';
import { clearTimeout, setTimeout } from 'timers';
import type { Client, TMessagePayload, TQueueName } from './client';
import {
  TEventListenerOptions,
  TEventName,
  TypedEventEmitter,
} from './typed-event-emitter';

function computeSpentTimeInMs(since: bigint): number {
  return Math.round(Number(process.hrtime.bigint() - since) / 1000000);
}

export type TConsumerTag = string;

export type TConsumerCallbackArgs<TPayload extends TMessagePayload> = {
  /**
   * The payload
   */
  payload: TPayload;

  /**
   * The whole message
   */
  message: Message;

  /**
   * Requeue the current message in case an Error is thrown in the callback,
   * the default behavior is to reject it.
   *
   * The method is available only if the message has not been already redelivered
   */
  requeueOnError?: () => void;
};

export type TConsumerCallback<
  TPayload extends TMessagePayload,
  TReply extends TMessagePayload
> = (
  args: TConsumerCallbackArgs<TPayload>,
) => Promise<TReply | void> | (TReply | void);

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

export type TConsumerEventMap = {
  [ConsumerEventKind.Stopped]: undefined;
  [ConsumerEventKind.Started]: undefined;
  [ConsumerEventKind.ChannelError]: Error;
  [ConsumerEventKind.ChannelClosed]: undefined;
  [ConsumerEventKind.ChannelOpened]: ConfirmChannel;
  [ConsumerEventKind.MessageParsed]: {
    message: Message;
    payload: TMessagePayload;
    tookInMs: number;
  };
  [ConsumerEventKind.MessageRejected]: {
    message: Message;
    payload?: TMessagePayload;
    error: Error;
    tookInMs: number;
  };
  [ConsumerEventKind.MessageRequeued]: {
    message: Message;
    payload: TMessagePayload;
    error: Error;
    tookInMs: number;
  };
  [ConsumerEventKind.MessageAcknowledged]: {
    message: Message;
    payload: TMessagePayload;
    tookInMs: number;
  };
  [ConsumerEventKind.MessageCallbackError]: {
    message: Message;
    payload: TMessagePayload;
    error: Error;
    tookInMs: number;
  };
};

export type TConsumerOptions = Omit<Options.Consume, 'noLocal'> & {
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
   * Stop on message's callback error"
   * Default is false, the message is rejected and the consumer continues
   */
  stopOnMessageCallbackError?: boolean;

  /**
   * Add some event listeners
   */
  on?: TEventListenerOptions<TConsumerEventMap>;
};

export class Consumer<
  TPayload extends TMessagePayload = any,
  TReply extends TMessagePayload = any
> extends TypedEventEmitter<TConsumerEventMap> {
  #status: ConsumerStatus = ConsumerStatus.Idle;
  #tag: TConsumerTag | null = null;
  #channel: Promise<ConfirmChannel> | null = null;
  #consumeTimeoutId: ReturnType<typeof setTimeout> | null = null;
  #idleTimeoutId: ReturnType<typeof setTimeout> | null = null;

  public readonly prefetch: number;
  public readonly consumeInMs: number | null;
  public readonly idleInMs: number | null;
  public readonly stopOnMessageCallbackError: boolean;
  public readonly options: Options.Consume;

  public constructor(
    public readonly client: Client,
    public readonly queueName: TQueueName,
    public readonly callback: TConsumerCallback<TPayload, TReply>,
    {
      prefetch = 1,
      consumeInMs,
      idleInMs,
      stopOnMessageCallbackError = false,
      on,
      ...options
    }: TConsumerOptions = {},
  ) {
    super(on);

    this.prefetch = prefetch;
    this.consumeInMs = consumeInMs ?? null;
    this.idleInMs = idleInMs ?? null;
    this.stopOnMessageCallbackError = stopOnMessageCallbackError;
    this.options = options;

    this.on(ConsumerEventKind.Stopped, () => {
      this.#status = ConsumerStatus.Idle;
      this.#tag = null;
      this.clearConsumeTimeout();
      this.clearIdleTimeout();
    }).on(ConsumerEventKind.ChannelClosed, () => {
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
    data: TConsumerEventMap[ConsumerEventKind.MessageAcknowledged],
  ): void {
    try {
      channel.ack(data.message);
      this.emit(ConsumerEventKind.MessageAcknowledged, data);
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
    data: TConsumerEventMap[ConsumerEventKind.MessageRequeued],
  ): void {
    try {
      channel.nack(data.message, false, true);
      this.emit(ConsumerEventKind.MessageRequeued, data);
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
    data: TConsumerEventMap[ConsumerEventKind.MessageRejected],
  ): void {
    try {
      channel.nack(data.message, false, false);
      this.emit(ConsumerEventKind.MessageRejected, data);
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
            } catch (error) {
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

            let requeueOnError: boolean = false;

            try {
              const result = await this.callback({
                message,
                payload,
                requeueOnError: message.fields.redelivered
                  ? undefined
                  : () => (requeueOnError = true),
              });

              if (
                message.properties.replyTo &&
                message.properties.correlationId &&
                typeof result !== 'undefined'
              ) {
                await this.client.publish(
                  '',
                  message.properties.replyTo,
                  result,
                  { correlationId: message.properties.correlationId },
                );
              }
            } catch (error) {
              this.emit(ConsumerEventKind.MessageCallbackError, {
                message,
                payload,
                error,
                tookInMs: computeSpentTimeInMs(start),
              });

              if (this.stopOnMessageCallbackError) {
                await this.stop();
              }

              // The callback has failed, requeue or reject the message
              return requeueOnError
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

            return this.ack(channel, {
              message,
              payload,
              tookInMs: computeSpentTimeInMs(start),
            });
          } catch (error) {
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

  public async startAndWait<TName extends TEventName<TConsumerEventMap>>(
    ...names: TName[]
  ) {
    await this.start();

    return this.wait(...names);
  }
}
