import { Kafka, logLevel } from 'kafkajs';

process.env['KAFKAJS_NO_PARTITIONER_WARNING'] = '1';

export const BROKERS = [Bun.env['KAFKA_BROKERS'] ?? 'localhost:9092'];

export const quietLogger = { info: () => {}, warn: () => {}, error: () => {}, profile: () => {} } as any;

export function uniquePrefix(scope: string): string {
  return `asena.test.${scope}.${crypto.randomUUID().slice(0, 8)}`;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export async function waitFor(cond: () => boolean | Promise<boolean>, timeoutMs = 10_000, label = ''): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error(`waitFor timed out${label ? `: ${label}` : ''}`);

    await sleep(50);
  }
}

export function rawKafka(clientId: string): Kafka {
  return new Kafka({ clientId, brokers: BROKERS, logLevel: logLevel.NOTHING });
}

/** Deletes every topic and consumer group under the given prefix. */
export async function sweepNamespace(prefix: string): Promise<void> {
  const admin = rawKafka('asena-test-sweep').admin();

  await admin.connect();

  try {
    const topics = (await admin.listTopics()).filter((topic) => topic.startsWith(prefix));

    if (topics.length) {
      await admin.deleteTopics({ topics }).catch(() => {});
    }

    const { groups } = await admin.listGroups();
    const mine = groups.map((group) => group.groupId).filter((groupId) => groupId.startsWith(prefix));

    if (mine.length) {
      await admin.deleteGroups(mine).catch(() => {});
    }
  } finally {
    await admin.disconnect();
  }
}

export interface ReadRecord {
  value: string | null;
  headers: Record<string, string>;
  key: string | null;
}

/**
 * Creates a topic and waits until its partition leaders serve. Retried:
 * kafkajs admin metadata refreshes THROW while any unrelated topic on the
 * cluster is mid-deletion (e.g. a prior test's sweep).
 */
export async function ensureTestTopic(topic: string, numPartitions = 1): Promise<void> {
  const admin = rawKafka('asena-test-ensure').admin();

  await admin.connect();

  try {
    const deadline = Date.now() + 10_000;

    for (;;) {
      try {
        await admin.createTopics({ waitForLeaders: true, topics: [{ topic, numPartitions }] });
        break;
      } catch (error) {
        if (Date.now() > deadline) throw error;

        await sleep(100);
      }
    }
  } finally {
    await admin.disconnect();
  }

  await waitTopicServed(topic);
}

/** Waits until the topic's partitions have elected leaders - touching a topic earlier makes kafkajs spray internal unhandled rejections. */
export async function waitTopicServed(topic: string, timeoutMs = 10_000): Promise<void> {
  const admin = rawKafka('asena-test-warm').admin();

  await admin.connect();

  try {
    const deadline = Date.now() + timeoutMs;

    for (;;) {
      const metadata: any = await admin.fetchTopicMetadata({ topics: [topic] }).catch(() => null);
      const served = metadata?.topics?.[0]?.partitions?.every((partition: any) => partition.leader >= 0);

      if (served) return;

      if (Date.now() > deadline) throw new Error(`topic ${topic} has no elected leaders after ${timeoutMs}ms`);

      await sleep(100);
    }
  } finally {
    await admin.disconnect();
  }
}

/** Reads a topic from the beginning with an ephemeral group until `count` records or timeout. */
export async function readTopic(topic: string, count: number, timeoutMs = 15_000): Promise<ReadRecord[]> {
  await waitTopicServed(topic);

  const kafka = rawKafka('asena-test-read');
  const groupId = `${topic}.reader.${crypto.randomUUID().slice(0, 8)}`;
  const consumer = kafka.consumer({ groupId, sessionTimeout: 6000, heartbeatInterval: 1500 });
  const records: ReadRecord[] = [];

  await consumer.connect();

  try {
    await consumer.subscribe({ topics: [topic], fromBeginning: true });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const headers: Record<string, string> = {};

        for (const [key, headerValue] of Object.entries(message.headers ?? {})) {
          if (headerValue !== undefined) headers[key] = String(headerValue);
        }

        records.push({
          value: message.value ? message.value.toString() : null,
          headers,
          key: message.key ? message.key.toString() : null,
        });
      },
    });

    await waitFor(() => records.length >= count, timeoutMs, `${count} record(s) on ${topic}`);
  } finally {
    await consumer.disconnect().catch(() => {});

    const admin = kafka.admin();

    await admin.connect();
    await admin.deleteGroups([groupId]).catch(() => {});
    await admin.disconnect();
  }

  return records;
}
