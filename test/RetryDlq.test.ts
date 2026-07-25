import { afterEach, describe, expect, it } from 'bun:test';
import { Kafka, logLevel } from 'kafkajs';
import { KafkaMicroserviceTransport, TopicNaming } from '../lib/microservice';
import { buildRequestHeaders } from '../lib/microservice/envelope';
import { BROKERS, readTopic, sweepNamespace, uniquePrefix, waitFor } from './util';

const FAST = {
  sessionTimeout: 3000,
  heartbeatInterval: 800,
  healthCheckIntervalMs: 1000,
  drainTimeout: 500,
};

describe('Retry, DLQ and stale-request drop', () => {
  let transport: KafkaMicroserviceTransport | undefined;
  let topicPrefix: string;

  afterEach(async () => {
    await transport?.destroy({ drainTimeout: 500 }).catch(() => {});
    transport = undefined;
    await sweepNamespace(topicPrefix);
  }, 30000);

  it('should redeliver a failing event with incrementing attempts, then move it to the DLQ with provenance', async () => {
    topicPrefix = uniquePrefix('dlq');

    const attempts: number[] = [];
    const messageIds = new Set<string>();

    transport = new KafkaMicroserviceTransport(
      { brokers: BROKERS },
      { serviceName: 'notification-service', topicPrefix, maxRetries: 2, retryBackoffMs: 200, ...FAST },
    );

    transport.registerEventHandler('booking.poison', async (_data, context) => {
      attempts.push(context.attempt);
      messageIds.add(context.messageId);
      throw new Error('poison');
    });

    await transport.init();
    await transport.listen();

    await transport.emit('booking.poison', { boom: true }, { headers: { 'x-origin': 'test' } });

    // maxRetries=2: attempts 1 and 2 execute, delivery 3 goes to the DLQ
    await waitFor(() => attempts.length >= 2, 20000, 'two failing attempts');

    const dlqRecords = await readTopic(new TopicNaming(topicPrefix).dlqTopic, 1, 20000);

    expect(attempts).toEqual([1, 2]);
    // One messageId per emit - identical across every redelivery
    expect(messageIds.size).toBe(1);

    const dlq = dlqRecords[0];

    expect(dlq.headers['p']).toBe('booking.poison');
    expect(dlq.headers['origin_group']).toBe('notification-service');
    expect(dlq.headers['origin_stream']).toBe(new TopicNaming(topicPrefix).eventTopic);
    expect(Number(dlq.headers['delivery_count'])).toBeGreaterThan(2);
    expect(dlq.headers['mid']).toBe([...messageIds][0]);
    // Original envelope survives the DLQ move
    expect(JSON.parse(dlq.value ?? 'null')).toEqual({ boom: true });
    expect(JSON.parse(dlq.headers['h'])).toEqual({ 'x-origin': 'test' });
  }, 60000);

  it('should drop a stale request (caller already timed out) without executing it', async () => {
    topicPrefix = uniquePrefix('stale');

    let invocations = 0;

    transport = new KafkaMicroserviceTransport(
      { brokers: BROKERS },
      { serviceName: 'booking-service', topicPrefix, ...FAST },
    );

    transport.registerMessageHandler('booking.noop', async () => {
      invocations++;

      return null;
    });

    await transport.init();
    await transport.listen();

    const naming = new TopicNaming(topicPrefix);

    // A request whose producer timestamp is far past its caller timeout -
    // the backlog a restarting service must NOT execute
    const kafka = new Kafka({ clientId: 'stale-producer', brokers: BROKERS, logLevel: logLevel.NOTHING });
    const producer = kafka.producer();

    await producer.connect();

    const staleHeaders = buildRequestHeaders(
      'booking.noop',
      'stale-mid',
      undefined,
      'stale-corr',
      naming.replyTopic,
      1000,
    );

    staleHeaders['ts'] = String(Date.now() - 5000);

    // A just-created topic's metadata may not have propagated to this raw
    // client yet - retry the transient UNKNOWN_TOPIC_OR_PARTITION window
    for (let i = 0; ; i++) {
      try {
        await producer.send({
          topic: naming.requestTopic('booking.noop'),
          messages: [{ value: 'null', headers: staleHeaders }],
        });
        break;
      } catch (error) {
        if (i >= 10) throw error;

        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    }

    await producer.disconnect();

    // The offset must advance past the stale record (commit without dispatch)
    const admin = kafka.admin();

    await admin.connect();

    try {
      await waitFor(
        async () => {
          try {
            const fetched = await admin.fetchOffsets({
              groupId: naming.groupId('booking-service'),
              topics: [naming.requestTopic('booking.noop')],
            });

            return fetched.some((topic) => topic.partitions.some((partition) => partition.offset === '1'));
          } catch {
            // Metadata still propagating - keep polling
            return false;
          }
        },
        20000,
        'stale request committed without execution',
      );
    } finally {
      await admin.disconnect();
    }

    expect(invocations).toBe(0);

    // A fresh request on the same pattern still executes normally
    const reply = await transport.send('booking.noop', {}, { timeout: 15000 });

    expect(reply).toBeNull();
    expect(invocations).toBe(1);
  }, 60000);
});
