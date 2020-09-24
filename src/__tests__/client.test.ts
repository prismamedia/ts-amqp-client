import { URL } from 'url';
import { Client } from '../client';
import { Consumer, ConsumerEventKind, ConsumerStatus } from '../consumer';

type TConsumerRequest = {
  request?: 'waitFor100ms' | 'requeue' | 'reject';
};

type TConsumerResponse = {
  response: string;
};

async function waitFor(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Client', () => {
  let client: Client;

  beforeEach(() => {
    client = new Client(process.env.AMQP_DSN!).on('error', console.error);
  });

  afterEach(async () => client.close());

  it('adds some options to the provided URL', () => {
    expect(new URL(client.url).search).toEqual('?heartbeat=15');
  });

  it('forceExchange works', async () => {
    const exchangeName = 'test_force-exchange_works';

    try {
      await expect(
        client.assertExchange(exchangeName, 'fanout'),
      ).resolves.toMatchObject({
        exchange: exchangeName,
      });

      await expect(
        client.assertExchange(exchangeName, 'topic'),
      ).rejects.toThrowError();

      await expect(
        client.forceExchange(exchangeName, 'topic'),
      ).resolves.toMatchObject({
        exchange: exchangeName,
      });
    } finally {
      await expect(client.deleteExchange(exchangeName)).resolves.toEqual({});
    }
  });

  it('forceQueue works', async () => {
    const queueName = 'test_force-queue_works';

    try {
      await expect(client.assertQueue(queueName)).resolves.toMatchObject({
        queue: queueName,
      });

      await expect(
        client.assertQueue(queueName, { exclusive: true }),
      ).rejects.toThrowError();

      await expect(
        client.forceQueue(queueName, { exclusive: true }),
      ).resolves.toMatchObject({
        queue: queueName,
      });
    } finally {
      await expect(client.deleteQueue(queueName)).resolves.toEqual({
        messageCount: 0,
      });
    }
  });

  it('publishing works', async () => {
    const queueName = 'test_publishing_works';
    const queueOptions = { durable: false };

    await expect(
      client.checkQueue(queueName),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `"Operation failed: QueueDeclare; 404 (NOT-FOUND) with message \\"NOT_FOUND - no queue 'test_publishing_works' in vhost '/'\\""`,
    );

    await client.assertQueue(queueName, queueOptions);

    try {
      await Promise.all(
        [...new Array(10)].map(() => client.publish('', queueName, {})),
      );

      await expect(client.checkQueue(queueName)).resolves.toMatchObject({
        messageCount: 10,
      });
    } finally {
      await client.deleteQueue(queueName);
    }
  });

  it('consuming works', async () => {
    const queueName = 'test_consuming_works';
    const queueOptions = { durable: false };

    await expect(
      client.checkQueue(queueName),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `"Operation failed: QueueDeclare; 404 (NOT-FOUND) with message \\"NOT_FOUND - no queue 'test_consuming_works' in vhost '/'\\""`,
    );

    await client.assertQueue(queueName, queueOptions);

    try {
      let messageProcessedCount = 0;

      const consumer = await client.consume<
        TConsumerRequest,
        TConsumerResponse
      >(
        queueName,
        async ({ payload, requeueOnError }) => {
          messageProcessedCount++;

          switch (payload.request) {
            case 'waitFor100ms':
              await waitFor(100);
              break;

            case 'reject':
              throw new Error(`An error`);

            case 'requeue':
              requeueOnError?.();
              throw new Error(`A "requeueOnError" reason`);
          }

          return {
            response: 'OK',
          };
        },
        { prefetch: 1, idleInMs: 250 },
      );

      await Promise.all([
        Promise.all(
          [...new Array(5)].map(() =>
            client.publish<TConsumerRequest>('', queueName, {
              request: 'waitFor100ms',
            }),
          ),
        ),
        Promise.all(
          [...new Array(5)].map(() =>
            client.publish<TConsumerRequest>('', queueName, {
              request: 'requeue',
            }),
          ),
        ),
        consumer.wait(ConsumerEventKind.Stopped),
      ]);

      expect(messageProcessedCount).toBe(15);

      await Promise.all(
        [...new Array(5)].map(() =>
          client.publish<TConsumerRequest>('', queueName, {}),
        ),
      );

      await expect(client.checkQueue(queueName)).resolves.toMatchObject({
        messageCount: 5,
      });

      await consumer.startAndWait(ConsumerEventKind.Stopped);

      expect(messageProcessedCount).toBe(20);
    } finally {
      await client.deleteQueue(queueName);
    }
  });

  it('stops on message callback error works', async () => {
    const queueName = 'test_stop_on_message_callback_error_works';
    const queueOptions = { durable: false };

    await expect(
      client.checkQueue(queueName),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `"Operation failed: QueueDeclare; 404 (NOT-FOUND) with message \\"NOT_FOUND - no queue 'test_stop_on_message_callback_error_works' in vhost '/'\\""`,
    );

    await client.assertQueue(queueName, queueOptions);

    try {
      const consumer = await client.consume(
        queueName,
        () => {
          throw new Error('An error');
        },
        { stopOnMessageCallbackError: true },
      );

      await Promise.all([
        [...new Array(5)].map(() =>
          client.publish<TConsumerRequest>('', queueName, {}),
        ),
        consumer.wait(ConsumerEventKind.Stopped),
      ]);

      // The first message throws an Error, so the consumer is stopped and 4 messages remains
      await expect(client.checkQueue(queueName)).resolves.toMatchObject({
        messageCount: 4,
      });
    } finally {
      await client.deleteQueue(queueName);
    }
  });

  it('rpc works', async () => {
    const queueName = 'test_rpc_works';
    const queueOptions = {};

    await expect(
      client.checkQueue(queueName),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `"Operation failed: QueueDeclare; 404 (NOT-FOUND) with message \\"NOT_FOUND - no queue 'test_rpc_works' in vhost '/'\\""`,
    );

    await client.assertQueue(queueName, queueOptions);

    let consumer: Consumer | undefined;

    try {
      consumer = await client.consume<TConsumerRequest, TConsumerResponse>(
        queueName,
        async ({ payload, requeueOnError }) => {
          switch (payload.request) {
            case 'waitFor100ms':
              await waitFor(100);
              break;

            case 'reject':
              throw new Error(`An error`);

            case 'requeue':
              requeueOnError?.();
              throw new Error(`A "requeueOnError" reason`);
          }

          return {
            response: 'OK',
          };
        },
      );

      await expect(
        client.rpc<TConsumerRequest, TConsumerResponse>('', queueName, {}),
      ).resolves.toEqual({
        response: 'OK',
      });

      await expect(
        client.rpc<TConsumerRequest, TConsumerResponse>(
          '',
          queueName,
          { request: 'waitFor100ms' },
          { timeoutInMs: 250 },
        ),
      ).resolves.toEqual({
        response: 'OK',
      });

      // Will throw an error, as the consumer will eventually reject the message, the client will never receive anything
      await expect(
        client.rpc<TConsumerRequest, TConsumerResponse>(
          '',
          queueName,
          { request: 'requeue' },
          { timeoutInMs: 50 },
        ),
      ).rejects.toThrow(/The RPC "[^"]+" has reached the timeout of 50ms/);

      // Will throw an error, as the consumer will reject the message, the client will never receive anything
      await expect(
        client.rpc<TConsumerRequest, TConsumerResponse>(
          '',
          queueName,
          { request: 'reject' },
          { timeoutInMs: 50 },
        ),
      ).rejects.toThrow(/The RPC "[^"]+" has reached the timeout of 50ms/);

      expect(consumer.getStatus()).toEqual(ConsumerStatus.Consuming);
    } finally {
      await client.deleteQueue(queueName);
      await waitFor(5);

      expect(consumer?.getStatus()).toEqual(ConsumerStatus.Idle);
    }
  });
});
