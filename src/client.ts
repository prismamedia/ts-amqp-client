import { ConfirmChannel, connect, Connection, Options } from 'amqplib';
import crypto from 'crypto';
import { errorMonitor } from 'events';
import { clearTimeout, setTimeout } from 'timers';
import { URL } from 'url';
import {
  Consumer,
  ConsumerEventKind,
  TConsumerCallback,
  TConsumerEventMap,
  TConsumerOptions,
} from './consumer';
import {
  TEventListenerOptions,
  TEventName,
  TypedEventEmitter,
} from './typed-event-emitter';

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

  /**
   * Add some event listeners
   */
  on?: TEventListenerOptions<TClientEventMap>;
};

export class Client extends TypedEventEmitter<TClientEventMap> {
  #connection: Promise<Connection> | null = null;
  #channel: Promise<ConfirmChannel> | null = null;
  #rpcMap = new Map<
    TCorrelationId,
    { resolve: (result: any) => void; reject: (error: Error) => void }
  >();
  #rpcConsumer: Consumer;

  public readonly url: string;

  public constructor(
    /**
     * cf: https://www.rabbitmq.com/uri-spec.html
     */
    url: string,
    public readonly options?: TClientOptions,
  ) {
    super(options?.on);

    const providedUrl = new URL(url);

    if (!providedUrl.searchParams.has('heartbeat')) {
      providedUrl.searchParams.set('heartbeat', '15');
    }

    this.url = providedUrl.toString();

    this.#rpcConsumer = new Consumer(
      this,
      options?.rpcResultsQueueName ||
        `rpc_results.${crypto.randomBytes(4).toString('hex')}`,
      async ({ payload, message }) =>
        this.#rpcMap.get(message.properties.correlationId)?.resolve(payload),
      {
        on: {
          [errorMonitor]: (error) =>
            this.#rpcMap.forEach(({ reject }) => reject(error)),
          [ConsumerEventKind.Stopped]: () =>
            this.#rpcMap.forEach(({ reject }) =>
              reject(new Error(`The RPC consumer stopped`)),
            ),
        },
      },
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

  public async publish<TPayload = any>(
    exchange: string = '',
    routingKey: string = '',
    payload: TPayload,
    options?: TPublishOptions,
  ): Promise<void> {
    // Forbid usage of undefined and null values as they are not "safe" to be stringified and parsed
    if (payload == null) {
      throw new TypeError('The payload has to be a non-null value.');
    }

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

  public async consume<TPayload = any, TReply = any>(
    queueName: TQueueName,
    callback: TConsumerCallback<TPayload, TReply>,
    options?: TConsumerOptions<TPayload, TReply>,
  ): Promise<Consumer<TPayload, TReply>> {
    const consumer = new Consumer(this, queueName, callback, options);
    await consumer.start();

    return consumer;
  }

  public async consumeAndWait<TPayload = any, TReply = any>(
    queueName: TQueueName,
    callback: TConsumerCallback<TPayload, TReply>,
    options: TConsumerOptions<TPayload, TReply>,
    ...eventNames: TEventName<TConsumerEventMap<TPayload, TReply>>[]
  ) {
    const consumer = await this.consume(queueName, callback, options);

    return consumer.wait(...eventNames);
  }

  public async rpc<TRequest = any, TResponse = any>(
    exchange: string = '',
    routingKey: string = '',
    request: TRequest,
    { timeoutInMs = 60000, ...options }: TRPCOptions = {},
  ): Promise<TResponse> {
    if (!this.#rpcConsumer.isConsuming()) {
      await this.assertQueue(this.#rpcConsumer.queueName, { exclusive: true });
      await this.#rpcConsumer.start();
    }

    const correlationId = crypto.randomBytes(4).toString('hex');

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
      this.#rpcMap.set(correlationId, {
        resolve: (result: TResponse) => {
          clearTimeout(timeoutId);
          this.#rpcMap.delete(correlationId);

          resolve(result);
        },
        reject,
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

  public async assertExchange(
    ...args: Parameters<ConfirmChannel['assertExchange']>
  ) {
    return this.seamlessCallOnChannel('assertExchange', ...args);
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

  public async forceExchange(
    ...args: Parameters<ConfirmChannel['assertExchange']>
  ) {
    try {
      return await this.assertExchange(...args);
    } catch (error) {
      // https://www.rabbitmq.com/amqp-0-9-1-reference.html#constants
      if (typeof error === 'object' && [405, 406].includes(error.code)) {
        await this.deleteExchange(args[0]);

        return await this.assertExchange(...args);
      }

      throw error;
    }
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

  public async forceQueue(...args: Parameters<ConfirmChannel['assertQueue']>) {
    try {
      return await this.assertQueue(...args);
    } catch (error) {
      // https://www.rabbitmq.com/amqp-0-9-1-reference.html#constants
      if (typeof error === 'object' && [405, 406].includes(error.code)) {
        await this.deleteQueue(args[0]);

        return await this.assertQueue(...args);
      }

      throw error;
    }
  }
}
