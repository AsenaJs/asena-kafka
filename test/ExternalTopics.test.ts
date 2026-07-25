import { afterEach, describe, expect, it } from 'bun:test';
import { UlakError, UlakErrorCode } from '@asenajs/asena/messaging';
import type { MessageContext } from '@asenajs/asena/microservice';
import { KafkaMicroserviceTransport, TopicNaming } from '../lib/microservice';
import { BROKERS, ensureTestTopic, rawKafka, readTopic, sleep, sweepNamespace, uniquePrefix, waitFor } from './util';

const FAST = {
  sessionTimeout: 3000,
  heartbeatInterval: 800,
  healthCheckIntervalMs: 1000,
  drainTimeout: 500,
};

/** Raw kafkajs produce - the "Quarkus side": plain JSON value, verbatim headers, no Asena envelope. */
async function produceRaw(topic: string, value: string, headers?: Record<string, string>, key?: string): Promise<void> {
  const producer = rawKafka('asena-test-ext-producer').producer();

  await producer.connect();

  try {
    // A just-created topic's metadata may not have reached this client yet -
    // retry the transient UNKNOWN_TOPIC_OR_PARTITION window
    for (let i = 0; ; i++) {
      try {
        await producer.send({ topic, messages: [{ value, headers, ...(key !== undefined && { key }) }] });

        return;
      } catch (error) {
        if (i >= 10) throw error;

        await sleep(300);
      }
    }
  } finally {
    await producer.disconnect();
  }
}

describe('External topics (foreign-system interop)', () => {
  let transports: KafkaMicroserviceTransport[] = [];
  let prefixes: string[] = [];

  const track = (transport: KafkaMicroserviceTransport): KafkaMicroserviceTransport => {
    transports.push(transport);

    return transport;
  };

  const namespace = (scope: string): string => {
    const prefix = uniquePrefix(scope);

    prefixes.push(prefix);

    return prefix;
  };

  afterEach(async () => {
    for (const transport of transports) {
      await transport.destroy({ drainTimeout: 500 }).catch(() => {});
    }

    transports = [];

    for (const prefix of prefixes) {
      await sweepNamespace(prefix);
    }

    prefixes = [];
  }, 60000);

  it('should deliver a raw foreign record with topic-as-pattern, raw headers and position-based messageId', async () => {
    const topicPrefix = namespace('exta');
    const ordersTopic = `${namespace('extb')}.orders`;

    await ensureTestTopic(ordersTopic);

    const received: { data: any; context: MessageContext }[] = [];

    const transport = track(
      new KafkaMicroserviceTransport(
        { brokers: BROKERS },
        { serviceName: 'asena-consumer', topicPrefix, ...FAST, external: { topics: [ordersTopic] } },
      ),
    );

    transport.registerEventHandler(ordersTopic, async (data, context) => {
      received.push({ data, context });
    });

    await transport.init();
    await transport.listen();

    await produceRaw(ordersTopic, JSON.stringify({ orderId: 'o-1', amount: 42 }), {
      traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
      'ce-type': 'order-created',
    });

    await waitFor(() => received.length >= 1, 20000, 'external record delivered');

    const { data, context } = received[0];

    expect(data).toEqual({ orderId: 'o-1', amount: 42 });
    expect(context.pattern).toBe(ordersTopic);
    // ALL raw record headers reach the handler (traceparent continuity, ce-*)
    expect(context.headers['traceparent']).toBe('00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01');
    expect(context.headers['ce-type']).toBe('order-created');
    // No envelope -> stable position-based identity (single partition, first record)
    expect(context.messageId).toBe(`${ordersTopic}:0:0`);
    expect(context.attempt).toBe(1);
  }, 60000);

  it('should never let a foreign "p" header steer dispatch, while honoring a "mid" header as messageId', async () => {
    const topicPrefix = namespace('extc');
    const topic = `${namespace('extd')}.events`;

    await ensureTestTopic(topic);

    const received: MessageContext[] = [];
    let foreignPatternHits = 0;

    const transport = track(
      new KafkaMicroserviceTransport(
        { brokers: BROKERS },
        { serviceName: 'asena-consumer', topicPrefix, ...FAST, external: { topics: [topic] } },
      ),
    );

    transport.registerEventHandler(topic, async (_data, context) => {
      received.push(context);
    });
    transport.registerEventHandler('something.else', async () => {
      foreignPatternHits++;
    });

    await transport.init();
    await transport.listen();

    await produceRaw(topic, '"payload"', { p: 'something.else', mid: 'fixed-id-1' });

    await waitFor(() => received.length >= 1, 20000, 'record delivered under topic name');

    // Deterministic dispatch: pattern is ALWAYS the topic, even when the
    // foreign record happens to carry a p header
    expect(received[0].pattern).toBe(topic);
    expect(received[0].messageId).toBe('fixed-id-1');

    await sleep(800);
    expect(foreignPatternHits).toBe(0);
  }, 60000);

  it('should retry a failing external handler and move the record to the DLQ with foreign headers preserved', async () => {
    const topicPrefix = namespace('exte');
    const topic = `${namespace('extf')}.poison`;

    await ensureTestTopic(topic);

    const attempts: number[] = [];

    const transport = track(
      new KafkaMicroserviceTransport(
        { brokers: BROKERS },
        {
          serviceName: 'asena-consumer',
          topicPrefix,
          maxRetries: 2,
          retryBackoffMs: 200,
          ...FAST,
          external: { topics: [topic] },
        },
      ),
    );

    transport.registerEventHandler(topic, async (_data, context) => {
      attempts.push(context.attempt);
      throw new Error('poison');
    });

    await transport.init();
    await transport.listen();

    await produceRaw(topic, JSON.stringify({ boom: true }), { traceparent: 'tp-1' });

    await waitFor(() => attempts.length >= 2, 20000, 'two failing attempts');

    const dlqRecords = await readTopic(new TopicNaming(topicPrefix).dlqTopic, 1, 20000);

    expect(attempts).toEqual([1, 2]);

    const dlq = dlqRecords[0];

    expect(dlq.headers['origin_stream']).toBe(topic);
    expect(dlq.headers['origin_group']).toBe('asena-consumer');
    expect(Number(dlq.headers['delivery_count'])).toBeGreaterThan(2);
    // Foreign value and headers survive the DLQ move untouched
    expect(JSON.parse(dlq.value ?? 'null')).toEqual({ boom: true });
    expect(dlq.headers['traceparent']).toBe('tp-1');
  }, 60000);

  it('should emit raw to an external topic: plain JSON, verbatim headers only, keyHeader as record key', async () => {
    const topicPrefix = namespace('extg');
    const outTopic = `${namespace('exth')}.commands`;

    await ensureTestTopic(outTopic);

    const transport = track(
      new KafkaMicroserviceTransport(
        { brokers: BROKERS },
        {
          serviceName: 'asena-producer',
          topicPrefix,
          ...FAST,
          external: { topics: [{ name: outTopic, keyHeader: 'x-tenant' }] },
        },
      ),
    );

    await transport.init();
    await transport.listen();

    await transport.emit(outTopic, { hello: 'quarkus' }, { headers: { 'x-tenant': 't-42', traceparent: 'tp-out' } });

    const records = await readTopic(outTopic, 1, 20000);
    const record = records[0];

    expect(JSON.parse(record.value ?? 'null')).toEqual({ hello: 'quarkus' });
    // EXACTLY the user headers - no Asena envelope (p/mid/h/ts), keyHeader not removed
    expect(record.headers).toEqual({ 'x-tenant': 't-42', traceparent: 'tp-out' });
    expect(record.key).toBe('t-42');
  }, 60000);

  it('should reject send() to an external pattern immediately, without minting a request topic', async () => {
    const topicPrefix = namespace('exti');
    const topic = `${namespace('extj')}.orders`;

    const transport = track(
      new KafkaMicroserviceTransport(
        { brokers: BROKERS },
        { serviceName: 'asena-caller', topicPrefix, ...FAST, external: { topics: [topic] } },
      ),
    );

    await transport.init();

    let error: UlakError | undefined;

    try {
      await transport.send(topic, {});
    } catch (caught) {
      error = caught as UlakError;
    }

    expect(error).toBeInstanceOf(UlakError);
    expect(error!.code).toBe(UlakErrorCode.SEND_FAILED);
    expect(error!.message).toContain('event-only');

    const admin = rawKafka('asena-test-listtopics').admin();

    await admin.connect();

    const allTopics = await admin.listTopics();

    await admin.disconnect();

    expect(allTopics).not.toContain(`${topicPrefix}.req.${topic}`);
  }, 60000);

  it('should fail listen() loudly for a missing subscribed external topic, but boot when the missing topic is outbound-only', async () => {
    const missingSubscribed = `${namespace('extk')}.ghost`;
    const missingOutbound = `${namespace('extl')}.ghost`;

    const subscriber = track(
      new KafkaMicroserviceTransport(
        { brokers: BROKERS },
        {
          serviceName: 'asena-consumer',
          topicPrefix: namespace('extm'),
          ...FAST,
          external: { topics: [missingSubscribed] },
        },
      ),
    );

    subscriber.registerEventHandler(missingSubscribed, async () => {});

    await subscriber.init();

    let error: Error | undefined;

    try {
      await subscriber.listen();
    } catch (caught) {
      error = caught as Error;
    }

    expect(error).toBeDefined();
    expect(error!.message).toContain('not available');
    expect(error!.message).toContain('foreign-owned');

    // Outbound-only: absence must NOT block boot - emit errors surface per call
    const producerOnly = track(
      new KafkaMicroserviceTransport(
        { brokers: BROKERS },
        {
          serviceName: 'asena-producer',
          topicPrefix: namespace('extn'),
          ...FAST,
          external: { topics: [missingOutbound] },
        },
      ),
    );

    producerOnly.registerEventHandler('regular.event', async () => {});

    await producerOnly.init();
    await producerOnly.listen();

    expect(producerOnly.isConnected).toBe(true);
  }, 90000);

  it('should hint at a prefix-shadowed handler when an external topic is outbound-only', async () => {
    // Since Asena 0.8 the @MessageController prefix is joined onto @EventPattern
    // too, so a handler meant for the foreign topic 'orders' silently registers
    // as 'billing.orders' and the topic is never consumed. listen() must say so.
    const foreign = `${namespace('extshadow')}.orders`;

    await ensureTestTopic(foreign);

    const transport = track(
      new KafkaMicroserviceTransport(
        { brokers: BROKERS },
        {
          serviceName: 'billing-service',
          topicPrefix: namespace('extshadow2'),
          ...FAST,
          external: { topics: [foreign] },
        },
      ),
    );

    // What a prefixed @MessageController('billing') would produce
    transport.registerEventHandler(`billing.${foreign}`, async () => {});

    const lines: string[] = [];
    const original = console.log;

    console.log = (...args: unknown[]) => {
      lines.push(args.join(' '));
    };

    try {
      await transport.init();
      await transport.listen();
    } finally {
      console.log = original;
    }

    const hint = lines.find((line) => line.includes(foreign) && line.includes('outbound-only'));

    expect(hint).toBeDefined();
    expect(hint).toContain(`billing.${foreign}`);
    expect(hint).toContain('prefix: false');
  }, 90000);

  it('should honor fromBeginning: default skips pre-boot records, true delivers them', async () => {
    const historyTopic = `${namespace('exto')}.history`;

    await ensureTestTopic(historyTopic);
    await produceRaw(historyTopic, '"before"');

    // default (false = latest): the pre-boot record stays invisible
    const latestReader = track(
      new KafkaMicroserviceTransport(
        { brokers: BROKERS },
        { serviceName: 'latest-reader', topicPrefix: namespace('extp'), ...FAST, external: { topics: [historyTopic] } },
      ),
    );

    const latestSeen: unknown[] = [];

    latestReader.registerEventHandler(historyTopic, async (data) => {
      latestSeen.push(data);
    });

    await latestReader.init();
    await latestReader.listen();

    await sleep(1500);
    expect(latestSeen).toEqual([]);

    await produceRaw(historyTopic, '"after"');
    await waitFor(() => latestSeen.length >= 1, 20000, 'post-boot record delivered');
    expect(latestSeen).toEqual(['after']);

    // fromBeginning: true - a fresh group reads the whole retained history
    const beginningReader = track(
      new KafkaMicroserviceTransport(
        { brokers: BROKERS },
        {
          serviceName: 'beginning-reader',
          topicPrefix: namespace('extq'),
          ...FAST,
          external: { topics: [historyTopic], fromBeginning: true },
        },
      ),
    );

    const beginningSeen: unknown[] = [];

    beginningReader.registerEventHandler(historyTopic, async (data) => {
      beginningSeen.push(data);
    });

    await beginningReader.init();
    await beginningReader.listen();

    await waitFor(() => beginningSeen.length >= 2, 20000, 'pre-boot record delivered');
    expect(beginningSeen).toEqual(['before', 'after']);
  }, 120000);

  it('should subscribe an external topic pulled in by a wildcard handler', async () => {
    const upstreamBase = namespace('extr');
    const upstreamTopic = `${upstreamBase}.up.order-created`;

    await ensureTestTopic(upstreamTopic);

    const received: MessageContext[] = [];

    const transport = track(
      new KafkaMicroserviceTransport(
        { brokers: BROKERS },
        { serviceName: 'asena-bridge', topicPrefix: namespace('exts'), ...FAST, external: { topics: [upstreamTopic] } },
      ),
    );

    // The wildcard handler is what pulls the external topic into the
    // subscription (collect() as subscription criterion)
    transport.registerEventHandler(`${upstreamBase}.up.*`, async (_data, context) => {
      received.push(context);
    });

    await transport.init();
    await transport.listen();

    await produceRaw(upstreamTopic, JSON.stringify({ orderId: 'o-2' }));

    await waitFor(() => received.length >= 1, 20000, 'wildcard-subscribed external record delivered');

    expect(received[0].pattern).toBe(upstreamTopic);
  }, 60000);

  it('should validate external names, duplicates, namespace collisions and message-pattern collisions', () => {
    const topicPrefix = uniquePrefix('extv');
    const options = (topics: any[]) => ({ serviceName: 'validator', topicPrefix, external: { topics } });

    expect(() => new KafkaMicroserviceTransport({ brokers: BROKERS }, options(['bad topic!']))).toThrow(
      /Kafka-topic-legal/,
    );

    expect(() => new KafkaMicroserviceTransport({ brokers: BROKERS }, options(['dup.a', { name: 'dup.a' }]))).toThrow(
      /Duplicate external topic/,
    );

    expect(() => new KafkaMicroserviceTransport({ brokers: BROKERS }, options([`${topicPrefix}.evt`]))).toThrow(
      /collides with the transport's own namespace/,
    );

    expect(() => new KafkaMicroserviceTransport({ brokers: BROKERS }, options([`${topicPrefix}.req.x`]))).toThrow(
      /collides with the transport's own namespace/,
    );

    const transport = new KafkaMicroserviceTransport({ brokers: BROKERS }, options(['legal.name']));

    expect(() => transport.registerMessageHandler('legal.name', async () => null)).toThrow(/event-only/);
  });
});
