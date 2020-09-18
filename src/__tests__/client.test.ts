import { URL } from 'url';
import { Client } from '../client';
import { Consumer, ConsumerStatus } from '../consumer';

type TConsumerRequest = {
  request?: 'waitFor100ms' | 'throwAnError' | 'reject';
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
        async ({ payload, reject }) => {
          messageProcessedCount++;

          switch (payload.request) {
            case 'waitFor100ms':
              await waitFor(100);
              break;

            case 'throwAnError':
              throw new Error(`An error`);

            case 'reject':
              return reject(new Error(`A reject reason`));
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
              request: 'throwAnError',
            }),
          ),
        ),
        consumer.wait(),
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

      await consumer.startAndWait();

      expect(messageProcessedCount).toBe(20);
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
        async ({ payload, reject }) => {
          switch (payload.request) {
            case 'waitFor100ms':
              await waitFor(100);
              break;

            case 'throwAnError':
              throw new Error(`An error`);

            case 'reject':
              return reject(new Error(`A reject reason`));
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

      // Will throw an error, as the consumer will throw an error, the client will never receive anything
      await expect(
        client.rpc<TConsumerRequest, TConsumerResponse>(
          '',
          queueName,
          { request: 'throwAnError' },
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
