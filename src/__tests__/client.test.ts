import { Options } from 'amqplib';
import { URL } from 'url';
import { Client } from '../client';
import { ConsumerEventKind, ConsumerStatus } from '../consumer';

type TConsumerRequest = {
  request?: 'waitFor100ms' | 'throw';
};

type TConsumerResponse = {
  response: string;
};

type TRPCConsumerRequest = {
  request?: 'waitFor100ms' | 'throw';
};

type TRPCConsumerResponse = {
  request: TRPCConsumerRequest;
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

    await expect(client.deleteExchange(exchangeName)).resolves.toEqual({});
  });

  it('forceQueue works', async () => {
    const queueName = 'test_force-queue_works';

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

    await expect(client.deleteQueue(queueName)).resolves.toEqual({
      messageCount: 0,
    });
  });

  it('publishing works', async () => {
    const queueName = 'test_publishing_works';
    const queueOptions: Options.AssertQueue = { exclusive: true };

    await client.forceQueue(queueName, queueOptions);
    await client.purgeQueue(queueName);

    await Promise.all(
      [...new Array(10)].map(() =>
        expect(client.publish('', queueName, {})).resolves.toBeUndefined(),
      ),
    );

    await expect(client.checkQueue(queueName)).resolves.toMatchObject({
      messageCount: 10,
    });

    await expect(client.deleteQueue(queueName)).resolves.toMatchObject({
      messageCount: 10,
    });
  });

  it('consuming works', async () => {
    const queueName = 'test_consuming_works';
    const queueOptions: Options.AssertQueue = { exclusive: true };

    await client.forceQueue(queueName, queueOptions);
    await client.purgeQueue(queueName);

    try {
      let messageProcessedCount = 0;

      const consumer = await client.consume<
        TConsumerRequest,
        TConsumerResponse
      >(
        queueName,
        async ({ payload }) => {
          messageProcessedCount++;

          switch (payload.request) {
            case 'waitFor100ms':
              await waitFor(100);
              break;

            case 'throw':
              throw new Error(`An error`);
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
              request: 'throw',
            }),
          ),
        ),
        consumer.wait(ConsumerEventKind.Stopped),
      ]);

      expect(messageProcessedCount).toBe(10);

      await Promise.all(
        [...new Array(5)].map(() =>
          client.publish<TConsumerRequest>('', queueName, {}),
        ),
      );

      await expect(client.checkQueue(queueName)).resolves.toMatchObject({
        messageCount: 5,
      });

      await consumer.startAndWait(ConsumerEventKind.Stopped);

      expect(messageProcessedCount).toBe(15);
    } finally {
      await client.deleteQueue(queueName);
    }
  });

  it('stops on error works', async () => {
    const queueName = 'test_stop_on_error_works';
    const queueOptions: Options.AssertQueue = { exclusive: true };

    await client.forceQueue(queueName, queueOptions);
    await client.purgeQueue(queueName);

    try {
      const consumer = await client.consume(
        queueName,
        () => {
          throw new Error('An error');
        },
        { stopOnError: true },
      );

      await Promise.all([
        [...new Array(5)].map(() => client.publish('', queueName, {})),
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

  it('stops on signals works', async () => {
    const queueName = 'test_consuming_works';
    const queueOptions: Options.AssertQueue = { exclusive: true };

    await client.forceQueue(queueName, queueOptions);
    await client.purgeQueue(queueName);

    try {
      const consumer = await client.consume<any, void>(
        queueName,
        async () => {},
        { idleInMs: 60000, stopOnSignals: true },
      );

      async function waitThenSendSignal(): Promise<void> {
        await waitFor(100);

        process.emit('SIGINT', 'SIGINT');
      }

      await expect(
        Promise.all([
          consumer.wait(ConsumerEventKind.Stopped),
          waitThenSendSignal(),
        ]),
      ).resolves.toBeTruthy();
    } finally {
      await client.deleteQueue(queueName);
    }
  });

  it('rpc works', async () => {
    const queueName = 'test_rpc_works';
    const queueOptions: Options.AssertQueue = { exclusive: true };

    await client.forceQueue(queueName, queueOptions);
    await client.purgeQueue(queueName);

    const consumer = await client.consume<
      TRPCConsumerRequest,
      TRPCConsumerResponse
    >(queueName, async ({ payload }) => {
      switch (payload.request) {
        case 'waitFor100ms':
          await waitFor(100);
          break;

        case 'throw':
          throw new Error(`An error`);
      }

      return {
        request: payload,
        response: 'OK',
      };
    });

    await expect(
      client.rpc<TRPCConsumerRequest, TRPCConsumerResponse>('', queueName, {}),
    ).resolves.toEqual({
      request: {},
      response: 'OK',
    });

    await expect(
      client.rpc<TRPCConsumerRequest, TRPCConsumerResponse>(
        '',
        queueName,
        { request: 'waitFor100ms' },
        { timeoutInMs: 250 },
      ),
    ).resolves.toEqual({
      request: { request: 'waitFor100ms' },
      response: 'OK',
    });

    // Will throw an error, as the consumer will eventually reject the message, the client will never receive anything
    await expect(
      client.rpc<TRPCConsumerRequest, TRPCConsumerResponse>(
        '',
        queueName,
        { request: 'throw' },
        { timeoutInMs: 50 },
      ),
    ).rejects.toThrow(/The RPC "[^"]+" has reached the timeout of 50ms/);

    expect(consumer.getStatus()).toEqual(ConsumerStatus.Consuming);

    await Promise.all([
      client.deleteQueue(queueName),
      consumer.wait(ConsumerEventKind.Stopped),
    ]);

    expect(consumer.getStatus()).toEqual(ConsumerStatus.Idle);
  });
});
