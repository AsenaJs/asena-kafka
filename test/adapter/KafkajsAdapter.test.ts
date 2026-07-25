import { afterAll, describe, expect, it } from 'bun:test';
import { KafkajsAdapter } from '../../lib/adapter';
import { BROKERS, ensureTestTopic, readTopic, sweepNamespace, uniquePrefix } from '../util';

const prefix = uniquePrefix('adapter');

describe('KafkajsAdapter', () => {
  const adapter = new KafkajsAdapter({ brokers: BROKERS, clientId: 'adapter-test' });

  afterAll(async () => {
    await adapter.disconnect();
    await sweepNamespace(prefix);
  }, 30000);

  it('should start disconnected and reject send before connect', async () => {
    expect(adapter.isConnected).toBe(false);
    await expect(adapter.send({ topic: `${prefix}.nope`, messages: [{ value: 'x' }] })).rejects.toThrow(
      'not connected',
    );
  });

  it('should connect idempotently', async () => {
    await adapter.connect();
    await adapter.connect();

    expect(adapter.isConnected).toBe(true);
  }, 30000);

  it('should produce via the default producer and read back', async () => {
    const topic = `${prefix}.roundtrip`;

    await ensureTestTopic(topic);

    const metadata = await adapter.send({ topic, messages: [{ value: 'ping', headers: { h1: 'v1' } }] });

    expect(metadata[0].errorCode).toBe(0);

    const records = await readTopic(topic, 1);

    expect(records[0].value).toBe('ping');
    expect(records[0].headers['h1']).toBe('v1');
  }, 30000);

  it('should hand out working factories', async () => {
    const producer = adapter.producer();

    await producer.connect();
    await producer.disconnect();

    const consumer = adapter.consumer({ groupId: `${prefix}.consumer` });

    await consumer.connect();
    expect(typeof consumer.events.GROUP_JOIN).toBe('string');
    await consumer.disconnect();

    const admin = adapter.admin();

    await admin.connect();

    const topics = await admin.listTopics();

    expect(Array.isArray(topics)).toBe(true);
    await admin.disconnect();
  }, 30000);
});
