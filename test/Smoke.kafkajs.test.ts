import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import type { Admin, Consumer, EachMessagePayload, Producer } from 'kafkajs';
import { Kafka, logLevel } from 'kafkajs';

/**
 * P0 smoke gate: proves every kafkajs primitive the transport design relies on
 * works under Bun against the asena-kafka broker. If any of these fail, the
 * transport design must be revisited BEFORE any transport code is written.
 *
 * Load-bearing primitives:
 *  - offset commit WITH metadata + admin.fetchOffsets metadata round-trip
 *    (the attempt-tracking protocol persists delivery counts in commit metadata)
 *  - seek/pause/resume redelivery (the event retry path)
 *  - GROUP_JOIN event (the marker-load hook after rebalance)
 *  - header byte fidelity (envelope lives in record headers)
 *
 * Requires: docker container `asena-kafka` on localhost:9092 (see README).
 */

process.env['KAFKAJS_NO_PARTITIONER_WARNING'] = '1';

const BROKERS = [Bun.env['KAFKA_BROKERS'] ?? 'localhost:9092'];
const suffix = crypto.randomUUID().slice(0, 8);
const TOPIC = `asena.smoke.${suffix}.main`;
const GROUP = `asena.smoke.${suffix}.group`;

const kafka = new Kafka({ clientId: `asena-smoke-${suffix}`, brokers: BROKERS, logLevel: logLevel.NOTHING });

let admin: Admin;
let producer: Producer;
const consumers: Consumer[] = [];

async function makeConsumer(groupId: string): Promise<Consumer> {
  const consumer = kafka.consumer({ groupId, sessionTimeout: 6000, heartbeatInterval: 1500 });

  await consumer.connect();
  consumers.push(consumer);

  return consumer;
}

async function waitFor(cond: () => boolean, timeoutMs: number, label: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`waitFor timed out: ${label}`);

    await new Promise((r) => setTimeout(r, 50));
  }
}

describe('kafkajs-on-Bun smoke (P0 gate)', () => {
  beforeAll(async () => {
    admin = kafka.admin();

    // The broker may still be warming up right after `docker run` - retry connect.
    const deadline = Date.now() + 30000;

    for (;;) {
      try {
        await admin.connect();
        await admin.listTopics();
        break;
      } catch (error) {
        if (Date.now() > deadline) throw error;

        await new Promise((r) => setTimeout(r, 1000));
      }
    }

    producer = kafka.producer();
    await producer.connect();
  }, 40000);

  afterAll(async () => {
    for (const consumer of consumers) {
      try {
        await consumer.disconnect();
      } catch {
        /* already gone */
      }
    }

    try {
      await admin.deleteGroups([GROUP]);
    } catch {
      /* group may be gone or never created */
    }

    await admin.deleteTopics({ topics: [TOPIC] }).catch(() => {});
    await producer.disconnect();
    await admin.disconnect();
  }, 20000);

  it('creates topics explicitly with a fixed partition count', async () => {
    const created = await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: TOPIC, numPartitions: 2, replicationFactor: 1 }],
    });

    expect(created).toBe(true);

    const metadata = await admin.fetchTopicMetadata({ topics: [TOPIC] });

    expect(metadata.topics[0].partitions.length).toBe(2);
  }, 15000);

  it('produces and consumes with byte-identical headers, exposes GROUP_JOIN', async () => {
    const headers = { p: 'booking.created', mid: 'msg-1', h: JSON.stringify({ 'x-tenant': 'acme' }), ts: '123' };

    await producer.send({
      topic: TOPIC,
      messages: [{ partition: 0, value: JSON.stringify({ ok: true }), headers }],
    });

    const consumer = await makeConsumer(GROUP);
    let joinPayload: any = null;

    consumer.on(consumer.events.GROUP_JOIN, (event) => {
      joinPayload = event.payload;
    });

    await consumer.subscribe({ topics: [TOPIC], fromBeginning: true });

    const received: EachMessagePayload[] = [];

    await consumer.run({
      autoCommit: false,
      eachMessage: async (payload) => {
        received.push(payload);
      },
    });

    await waitFor(() => received.length >= 1, 15000, 'first delivery');

    const message = received[0].message;

    expect(String(message.headers?.['p'])).toBe('booking.created');
    expect(String(message.headers?.['mid'])).toBe('msg-1');
    expect(String(message.headers?.['h'])).toBe(JSON.stringify({ 'x-tenant': 'acme' }));
    expect(JSON.parse(String(message.value))).toEqual({ ok: true });

    expect(joinPayload).not.toBeNull();
    expect(joinPayload.groupId).toBe(GROUP);
    expect(typeof joinPayload.memberId).toBe('string');
  }, 30000);

  it('round-trips offset-commit METADATA through admin.fetchOffsets (attempt-marker primitive)', async () => {
    const consumer = consumers[0];
    const marker = JSON.stringify({ a: 2 });

    // Commit offset 0 (= "next fetch starts at 0", nothing consumed yet) carrying marker metadata.
    await consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '0', metadata: marker }]);

    const fetched = await admin.fetchOffsets({ groupId: GROUP, topics: [TOPIC] });
    const partition0 = fetched.find((t) => t.topic === TOPIC)?.partitions.find((p) => p.partition === 0);

    expect(partition0?.offset).toBe('0');
    expect(partition0?.metadata).toBe(marker);

    // Advancing the commit with empty metadata clears the marker.
    await consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1', metadata: null }]);

    const cleared = await admin.fetchOffsets({ groupId: GROUP, topics: [TOPIC] });
    const clearedP0 = cleared.find((t) => t.topic === TOPIC)?.partitions.find((p) => p.partition === 0);

    expect(clearedP0?.offset).toBe('1');
    expect(clearedP0?.metadata ?? null).not.toBe(marker);
  }, 15000);

  it('redelivers the same offset after pause + seek + resume (event retry primitive)', async () => {
    const consumer = consumers[0];

    const redelivery = await producer.send({
      topic: TOPIC,
      messages: [{ partition: 1, value: 'retry-me', headers: { mid: 'retry-1' } }],
    });

    const targetOffset = redelivery[0].baseOffset ?? '0';
    const deliveries: string[] = [];
    let paused = false;

    // The running eachMessage from the previous test still feeds `received`;
    // we need our own tracking - restart the consumer with fresh handler state.
    await consumer.stop();

    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
        if (partition !== 1 || String(message.headers?.['mid']) !== 'retry-1') return;

        deliveries.push(message.offset);

        if (!paused) {
          paused = true;
          consumer.pause([{ topic, partitions: [partition] }]);
          consumer.seek({ topic, partition, offset: message.offset });
          setTimeout(() => consumer.resume([{ topic, partitions: [partition] }]), 300);
        }
      },
    });

    consumer.seek({ topic: TOPIC, partition: 1, offset: targetOffset });

    await waitFor(() => deliveries.length >= 2, 20000, 'redelivery after pause/seek/resume');

    expect(deliveries[0]).toBe(deliveries[1]);
  }, 30000);

  it('deletes groups and topics (namespace sweep primitives)', async () => {
    const sweepTopic = `asena.smoke.${suffix}.sweep`;
    const sweepGroup = `asena.smoke.${suffix}.sweepgroup`;

    await admin.createTopics({ waitForLeaders: true, topics: [{ topic: sweepTopic, numPartitions: 1 }] });

    // A group only exists once it has committed or joined; commit via a short-lived consumer.
    const consumer = await makeConsumer(sweepGroup);

    await consumer.subscribe({ topics: [sweepTopic], fromBeginning: true });
    await consumer.run({ autoCommit: false, eachMessage: async () => {} });

    // Group must be EMPTY before deleteGroups - disconnect first.
    await consumer.disconnect();
    consumers.splice(consumers.indexOf(consumer), 1);

    await admin.deleteGroups([sweepGroup]);
    await admin.deleteTopics({ topics: [sweepTopic] });

    const topics = await admin.listTopics();

    expect(topics).not.toContain(sweepTopic);
  }, 30000);
});
