import type { ConfirmChannel, Message, Options } from 'amqplib';
import { clearTimeout, setTimeout } from 'timers';
import type { Client, TMessagePayload, TQueueName } from './client';
import { AMQPError } from './error';
import { TypedEventEmitter } from './typed-event-emitter';

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
   * Reject the current message
   */
  reject: (error: Error) => void;
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
  };
  [ConsumerEventKind.MessageRejected]: {
    message: Message;
    payload?: TMessagePayload;
    error: Error;
  };
  [ConsumerEventKind.MessageRequeued]: {
    message: Message;
    payload: TMessagePayload;
    error: Error;
  };
  [ConsumerEventKind.MessageAcknowledged]: {
    message: Message;
    payload: TMessagePayload;
  };
};

export type TConsumerOptions = Omit<Options.Consume, 'noLocal'> & {
  prefetch?: number;

  /**
   * After "consumeInMs" ms, the consumer will stop
   */
  consumeInMs?: number;

  /**
   * After "idleInMs" ms with no message, the consumer will stop
   */
  idleInMs?: number;
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
  public readonly options: Options.Consume;

  public constructor(
    public readonly client: Client,
    public readonly queueName: TQueueName,
    public readonly callback: TConsumerCallback<TPayload, TReply>,
    { prefetch = 1, consumeInMs, idleInMs, ...options }: TConsumerOptions = {},
  ) {
    super();

    this.prefetch = prefetch;
    this.consumeInMs = consumeInMs ?? null;
    this.idleInMs = idleInMs ?? null;
    this.options = options;

    this.on(ConsumerEventKind.Stopped, () => {
      this.#status = ConsumerStatus.Idle;
      this.#tag = null;
      this.clearConsumeTimeout();
      this.clearIdleTimeout();
    })
      .on(ConsumerEventKind.ChannelClosed, () =>
        this.emit(ConsumerEventKind.Stopped),
      )
      .on('error', (error) => client.emit('error', error));
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
          this.emit(ConsumerEventKind.ChannelClosed);

          this.#channel = null;
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
        if (this.isConsuming()) {
          this.emit(ConsumerEventKind.Stopped);
        }
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
        if (this.isConsuming()) {
          this.emit(ConsumerEventKind.Stopped);
        }
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
        if (this.isConsuming()) {
          this.emit(ConsumerEventKind.Stopped);
        }
      } else {
        throw error;
      }
    }
  }

  protected clearConsumeTimeout(): void {
    this.#consumeTimeoutId && clearTimeout(this.#consumeTimeoutId);
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
    this.#idleTimeoutId && clearTimeout(this.#idleTimeoutId);
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
      throw new AMQPError(
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
          this.setIdleTimeout();

          if (message.properties.contentType !== 'application/json') {
            // The message's "Content-Type" is not supported, reject it
            return this.reject(channel, {
              message,
              error: new AMQPError(
                `The Content-Type "${String(
                  message.properties.contentType,
                )}" is not supported, the message has been rejected`,
              ),
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
            });
          }

          try {
            let rejectedReason: Error | undefined;

            const result = await this.callback({
              message,
              payload,
              reject: (error: Error) => (rejectedReason = error),
            });

            if (rejectedReason) {
              // The message has been rejected by the callback
              return this.reject(channel, {
                message,
                payload,
                error: rejectedReason,
              });
            }

            if (
              message.properties.replyTo &&
              message.properties.correlationId &&
              typeof result !== 'undefined'
            ) {
              await this.client.publish(
                '',
                message.properties.replyTo,
                result as TMessagePayload,
                { correlationId: message.properties.correlationId },
              );
            }

            this.ack(channel, {
              message,
              payload,
            });
          } catch (error) {
            // The callback has failed, either it has to be rejected or requeued depends of if it already have been "redelivered" or not
            if (message.fields.redelivered) {
              this.reject(channel, {
                message,
                payload,
                error,
              });
            } else {
              this.requeue(channel, {
                message,
                payload,
                error,
              });
            }
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
      throw new AMQPError(
        `AMQP consumer workflow error: cannot stop a "${
          this.#status
        }" consumer`,
      );
    }

    if (!this.#tag) {
      throw new AMQPError(
        `AMQP consumer workflow error: cannot stop a "${
          this.#status
        }" consumer without a "tag"`,
      );
    }

    const channel = await this.getChannel();
    await channel.cancel(this.#tag);

    this.emit(ConsumerEventKind.Stopped);
  }

  /**
   * Returns a Promise resolved only when the consumer is stopped
   */
  public async wait(): Promise<void> {
    if (!this.isConsuming()) {
      throw new AMQPError(
        `AMQP consumer workflow error: cannot wait a "${
          this.#status
        }" consumer`,
      );
    }

    return new Promise((resolve) =>
      this.on(ConsumerEventKind.Stopped, resolve),
    );
  }

  public async startAndWait(): Promise<void> {
    await this.start();

    return this.wait();
  }
}
