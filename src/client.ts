import { ConfirmChannel, connect, Connection, Options } from 'amqplib';
import crypto from 'crypto';
import { clearTimeout, setTimeout } from 'timers';
import { URL } from 'url';
import {
  Consumer,
  TConsumerCallback,
  TConsumerEventMap,
  TConsumerOptions,
} from './consumer';
import { TEventName, TypedEventEmitter } from './typed-event-emitter';

export type TMessagePayload = { [key: string]: any };

export type TQueueName = string;

export type TCorrelationId = string;

export enum ClientEventKind {
  ConnectionError = 'CONNECTION_ERROR',
  ConnectionClosed = 'CONNECTION_CLOSED',
  ConnectionOpened = 'CONNECTION_OPENED',
  ChannelError = 'CHANNEL_ERROR',
  ChannelClosed = 'CHANNEL_CLOSED',
  ChannelOpened = 'CHANNEL_OPENED',
}

export type TClientEventMap = {
  [ClientEventKind.ConnectionError]: Error;
  [ClientEventKind.ConnectionClosed]: undefined;
  [ClientEventKind.ConnectionOpened]: Connection;
  [ClientEventKind.ChannelError]: Error;
  [ClientEventKind.ChannelClosed]: undefined;
  [ClientEventKind.ChannelOpened]: ConfirmChannel;
};

export type TPublishOptions = Omit<
  Options.Publish,
  'contentEncoding' | 'contentType'
>;

export type TRPCOptions = Omit<TPublishOptions, 'expiration'> & {
  timeoutInMs?: number;
};

export type TClientOptions = {
  rpcResultsQueueName?: TQueueName;
};

export class Client extends TypedEventEmitter<TClientEventMap> {
  #connection: Promise<Connection> | null = null;
  #channel: Promise<ConfirmChannel> | null = null;
  #rpcMap = new Map<TCorrelationId, (result: any) => void>();
  #rpcConsumer: Consumer;

  public readonly url: string;

  public constructor(
    /**
     * cf: https://www.rabbitmq.com/uri-spec.html
     */
    url: string,
    public readonly options?: TClientOptions,
  ) {
    super();

    const providedUrl = new URL(url);

    if (!providedUrl.searchParams.has('heartbeat')) {
      providedUrl.searchParams.set('heartbeat', '15');
    }

    this.url = providedUrl.toString();

    this.#rpcConsumer = new Consumer(
      this,
      options?.rpcResultsQueueName ||
        `rpc_results.${crypto.randomBytes(4).toString('base64')}`,
      async ({ payload, message }) =>
        this.#rpcMap.get(message.properties.correlationId)?.(payload),
      { exclusive: true },
    );
  }

  public async getConnection(): Promise<Connection> {
    if (!this.#connection) {
      const connection = (await (this.#connection = connect(this.url)))
        // The "close" event is always fired after the "error" one
        .on('error', (error: Error) =>
          this.emit(ClientEventKind.ConnectionError, error),
        )
        .on('close', () => {
          this.#connection = null;

          this.emit(ClientEventKind.ConnectionClosed);
        });

      this.emit(ClientEventKind.ConnectionOpened, connection);
    }

    return this.#connection;
  }

  public async getChannel(): Promise<ConfirmChannel> {
    if (!this.#channel) {
      const channel = (
        await (this.#channel = new Promise(async (resolve, reject) => {
          try {
            const connection = await this.getConnection();
            const channel = await connection.createConfirmChannel();

            resolve(channel);
          } catch (error) {
            reject(error);
          }
        }))
      )
        // The "close" event is always fired after the "error" one
        .on('error', (error: Error) =>
          this.emit(ClientEventKind.ChannelError, error),
        )
        .on('close', () => {
          this.#channel = null;

          this.emit(ClientEventKind.ChannelClosed);
        });

      this.emit(ClientEventKind.ChannelOpened, channel);
    }

    return this.#channel;
  }

  public async close(): Promise<void> {
    await (await this.#connection)?.close();
  }

  public async publish<TPayload extends TMessagePayload = any>(
    exchange: string = '',
    routingKey: string = '',
    payload: TPayload,
    options?: TPublishOptions,
  ): Promise<void> {
    const channel = await this.getChannel();

    await new Promise((resolve, reject) =>
      channel.publish(
        exchange,
        routingKey,
        Buffer.from(JSON.stringify(payload)),
        { ...options, contentType: 'application/json' },
        (error) => (error ? reject(error) : resolve()),
      ),
    );
  }

  public async consume<
    TPayload extends TMessagePayload = any,
    TReply extends TMessagePayload = any
  >(
    queueName: TQueueName,
    callback: TConsumerCallback<TPayload, TReply>,
    options?: TConsumerOptions,
  ): Promise<Consumer<TPayload, TReply>> {
    const consumer = new Consumer(this, queueName, callback, options);
    await consumer.start();

    return consumer;
  }

  public async consumeAndWait<
    TEvent extends TEventName<TConsumerEventMap>,
    TPayload extends TMessagePayload = any,
    TReply extends TMessagePayload = any
  >(
    queueName: TQueueName,
    callback: TConsumerCallback<TPayload, TReply>,
    options: TConsumerOptions,
    eventName: TEvent,
    timeoutInMs: number,
  ) {
    const consumer = await this.consume(queueName, callback, options);

    return consumer.wait(eventName, timeoutInMs);
  }

  public async rpc<
    TRequest extends TMessagePayload = any,
    TResponse extends TMessagePayload = any
  >(
    exchange: string = '',
    routingKey: string = '',
    request: TRequest,
    { timeoutInMs = 60000, ...options }: TRPCOptions = {},
  ): Promise<TResponse> {
    if (!this.#rpcConsumer.isConsuming()) {
      await this.assertQueue(this.#rpcConsumer.queueName, { exclusive: true });
      await this.#rpcConsumer.start();
    }

    const correlationId = crypto.randomBytes(4).toString('base64');

    return new Promise(async (resolve, reject) => {
      // Handle the timeout
      const timeoutId = setTimeout(() => {
        this.#rpcMap.delete(correlationId);

        reject(
          new Error(
            `The RPC "${correlationId}" has reached the timeout of ${timeoutInMs}ms`,
          ),
        );
      }, timeoutInMs);

      // Handle the result
      this.#rpcMap.set(correlationId, (result: TResponse) => {
        clearTimeout(timeoutId);
        this.#rpcMap.delete(correlationId);

        resolve(result);
      });

      try {
        await this.publish(exchange, routingKey, request, {
          ...options,
          replyTo: this.#rpcConsumer.queueName,
          correlationId,
          expiration: String(timeoutInMs),
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * TODO: Dynamically create these methods
   */
  protected async seamlessCallOnChannel<
    TMethodName extends Extract<
      | 'assertExchange'
      | 'checkExchange'
      | 'bindExchange'
      | 'unbindExchange'
      | 'deleteExchange'
      | 'assertQueue'
      | 'checkQueue'
      | 'bindQueue'
      | 'unbindQueue'
      | 'purgeQueue'
      | 'deleteQueue',
      keyof ConfirmChannel
    >
  >(
    methodName: TMethodName,
    ...args: Parameters<ConfirmChannel[TMethodName]>
  ): Promise<
    ReturnType<ConfirmChannel[TMethodName]> extends Promise<infer R> ? R : never
  > {
    const channel = await this.getChannel();

    return (channel as any)[methodName](...args);
  }

  public async checkExchange(
    ...args: Parameters<ConfirmChannel['checkExchange']>
  ) {
    return this.seamlessCallOnChannel('checkExchange', ...args);
  }

  public async bindExchange(
    ...args: Parameters<ConfirmChannel['bindExchange']>
  ) {
    return this.seamlessCallOnChannel('bindExchange', ...args);
  }

  public async unbindExchange(
    ...args: Parameters<ConfirmChannel['unbindExchange']>
  ) {
    return this.seamlessCallOnChannel('unbindExchange', ...args);
  }

  public async deleteExchange(
    ...args: Parameters<ConfirmChannel['deleteExchange']>
  ) {
    return this.seamlessCallOnChannel('deleteExchange', ...args);
  }

  public async assertQueue(...args: Parameters<ConfirmChannel['assertQueue']>) {
    return this.seamlessCallOnChannel('assertQueue', ...args);
  }

  public async checkQueue(...args: Parameters<ConfirmChannel['checkQueue']>) {
    return this.seamlessCallOnChannel('checkQueue', ...args);
  }

  public async bindQueue(...args: Parameters<ConfirmChannel['bindQueue']>) {
    return this.seamlessCallOnChannel('bindQueue', ...args);
  }

  public async unbindQueue(...args: Parameters<ConfirmChannel['unbindQueue']>) {
    return this.seamlessCallOnChannel('unbindQueue', ...args);
  }

  public async purgeQueue(...args: Parameters<ConfirmChannel['purgeQueue']>) {
    return this.seamlessCallOnChannel('purgeQueue', ...args);
  }

  public async deleteQueue(...args: Parameters<ConfirmChannel['deleteQueue']>) {
    return this.seamlessCallOnChannel('deleteQueue', ...args);
  }
}
