import { describe, expect, it, setSystemTime } from 'bun:test';
import { KafkaMicroserviceTransport } from '../lib/microservice';
import type {
  KafkaAdminLike,
  KafkaClientAdapter,
  KafkaConsumerConfig,
  KafkaConsumerLike,
  KafkaOffsetCommit,
  KafkaProducerLike,
} from '../lib/adapter';

/**
 * Offline consumer-recovery tests. The transport recreates a consumer that
 * kafkajs gave up on (CRASH with restart:false, e.g. after a broker outage
 * outlasted the client's retries). Both consumers this transport owns must
 * recover: the REPLY consumer is the one every caller depends on - an
 * instance whose reply consumer stays dead answers no send() ever again.
 *
 * Recovering the consumer is not enough on its own, so the same fakes cover
 * the two properties that make a recovered reply consumer actually useful:
 * it must resume from a COMMITTED offset (a rejoin that re-resolves 'latest'
 * skips a reply appended while it was away - permanently), and isConnected
 * must not claim health while it is not fetching.
 */

const PREFIX = 'asena.test.recovery';
const REPLY_TOPIC = `${PREFIX}.reply`;
const REPLY_PARTITIONS = [0, 1, 2, 3];

const EVENTS = {
  GROUP_JOIN: 'consumer.group_join',
  CRASH: 'consumer.crash',
  FETCH: 'consumer.fetch',
  DISCONNECT: 'consumer.disconnect',
  STOP: 'consumer.stop',
} as const;

class FakeConsumer implements KafkaConsumerLike {
  public readonly events = EVENTS;

  public disconnected = false;

  /** Every offset this consumer committed, in order. */
  public readonly commits: KafkaOffsetCommit[] = [];

  private listeners = new Map<string, ((event: any) => void)[]>();

  public constructor(
    public readonly groupId: string,
    private readonly assignment: Record<string, number[]>,
    private readonly committed: Map<number, string>,
  ) {}

  public async connect(): Promise<void> {}

  public async disconnect(): Promise<void> {
    this.disconnected = true;
    this.emit(EVENTS.DISCONNECT, {});
  }

  public async stop(): Promise<void> {}

  public async subscribe(): Promise<void> {}

  public async run(): Promise<void> {
    // A real consumer joins and completes its first fetch right after run();
    // the transport blocks on both before reporting ready.
    this.join();
  }

  /** Emits one join/fetch cycle, exactly as a kafkajs rejoin does. */
  public join(): void {
    this.emit(EVENTS.GROUP_JOIN, { payload: { memberAssignment: this.assignment } });
    this.emit(EVENTS.FETCH, {});
  }

  public async commitOffsets(offsets: KafkaOffsetCommit[]): Promise<void> {
    for (const offset of offsets) {
      this.commits.push(offset);

      if (offset.topic === REPLY_TOPIC) this.committed.set(offset.partition, offset.offset);
    }
  }

  public seek(): void {}

  public pause(): void {}

  public resume(): void {}

  public on(eventName: string, listener: (event: any) => void): unknown {
    this.listeners.set(eventName, [...(this.listeners.get(eventName) ?? []), listener]);

    return () => {};
  }

  public emit(eventName: string, event: any): void {
    for (const listener of this.listeners.get(eventName) ?? []) listener(event);
  }
}

class FakeAdapter implements KafkaClientAdapter {
  public readonly isConnected = true;

  public readonly consumers: FakeConsumer[] = [];

  /** Reply-topic high watermarks the next fetchTopicOffsets reports. */
  public high = new Map<number, string>(REPLY_PARTITIONS.map((partition) => [partition, String(10 * partition + 10)]));

  /** Broker-side committed offsets of the ephemeral reply group. */
  public readonly replyCommitted = new Map<number, string>();

  public async connect(): Promise<void> {}

  public async disconnect(): Promise<void> {}

  public producer(): KafkaProducerLike {
    return {
      connect: async () => {},
      disconnect: async () => {},
      send: async () => [],
    };
  }

  public consumer(config: KafkaConsumerConfig): KafkaConsumerLike {
    const reply = config.groupId.includes('.reply.');
    const consumer = new FakeConsumer(
      config.groupId,
      reply ? { [REPLY_TOPIC]: [...REPLY_PARTITIONS] } : {},
      this.replyCommitted,
    );

    this.consumers.push(consumer);

    return consumer;
  }

  public admin(): KafkaAdminLike {
    return {
      connect: async () => {},
      disconnect: async () => {},
      createTopics: async () => true,
      deleteTopics: async () => {},
      listTopics: async () => [],
      deleteGroups: async () => undefined,
      // A group with nothing committed answers -1 per partition, which is
      // what makes kafkajs re-resolve 'latest' on every join
      fetchOffsets: async ({ topics }: { groupId: string; topics?: string[] }) =>
        (topics ?? []).map((topic) => ({
          topic,
          partitions: REPLY_PARTITIONS.map((partition) => ({
            partition,
            offset: this.replyCommitted.get(partition) ?? '-1',
            metadata: null,
          })),
        })),
      fetchTopicMetadata: async (options?: { topics?: string[] }) => ({
        topics: (options?.topics ?? []).map((name) => ({ name, partitions: [{ leader: 0 }] })),
      }),
      fetchTopicOffsets: async () =>
        REPLY_PARTITIONS.map((partition) => ({
          partition,
          offset: this.high.get(partition)!,
          high: this.high.get(partition)!,
          low: '0',
        })),
    };
  }

  public async send(): Promise<[]> {
    return [];
  }

  public replyConsumers(): FakeConsumer[] {
    return this.consumers.filter((consumer) => consumer.groupId.includes('.reply.'));
  }
}

function build(adapter: FakeAdapter, healthCheckIntervalMs = 60_000): KafkaMicroserviceTransport {
  // A source with createConsumer() is treated as an AsenaKafkaService, so the
  // transport uses `client` as its adapter - the seam this test rides on.
  const source = { createConsumer: () => {}, client: adapter } as any;

  return new KafkaMicroserviceTransport(source, {
    serviceName: 'caller-service',
    topicPrefix: PREFIX,
    // Default is long enough that the periodic probe never fires during a test
    healthCheckIntervalMs,
  });
}

/** Client-only, like an HTTP gateway: no handlers, so the reply consumer is the only one. */
async function bootClientOnly(
  adapter: FakeAdapter,
  healthCheckIntervalMs?: number,
): Promise<KafkaMicroserviceTransport> {
  const transport = build(adapter, healthCheckIntervalMs);

  await transport.init();
  await transport.listen();

  return transport;
}

async function waitFor(condition: () => boolean, timeoutMs = 5_000, label = 'condition'): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`waitFor timed out after ${timeoutMs}ms: ${label}`);

    await new Promise((resolve) => {
      setTimeout(resolve, 10);
    });
  }
}

const replyPins = (consumer: FakeConsumer): string[] =>
  consumer.commits
    .filter((commit) => commit.topic === REPLY_TOPIC)
    .sort((a, b) => a.partition - b.partition)
    .map((commit) => `${commit.partition}:${commit.offset}`);

describe('reply consumer crash recovery', () => {
  it('recreates the reply consumer after a non-restarting crash', async () => {
    const adapter = new FakeAdapter();

    // No handlers registered: a client-only instance (an HTTP gateway that
    // only send()s). listen() starts no main consumer, so the reply consumer
    // is the ONLY thing keeping send() alive.
    const transport = await bootClientOnly(adapter);

    expect(adapter.replyConsumers()).toHaveLength(1);

    const first = adapter.replyConsumers()[0]!;

    // kafkajs gave up (retries exhausted during a broker outage) and stopped
    // this consumer for good - nothing restarts it from the inside.
    first.emit(EVENTS.CRASH, { payload: { error: new Error('Connection error'), restart: false } });

    await waitFor(() => adapter.replyConsumers().length >= 2, 5_000, 'reply consumer recreated after crash');

    await transport.destroy({ drainTimeout: 0 });
  });

  it('does not recreate the reply consumer after destroy', async () => {
    const adapter = new FakeAdapter();
    const transport = await bootClientOnly(adapter);

    const first = adapter.replyConsumers()[0]!;

    await transport.destroy({ drainTimeout: 0 });

    first.emit(EVENTS.CRASH, { payload: { error: new Error('Connection error'), restart: false } });

    await new Promise((resolve) => {
      setTimeout(resolve, 200);
    });

    expect(adapter.replyConsumers()).toHaveLength(1);
  });
});

describe('reply consumer start-position pinning', () => {
  it('commits a start offset for every assigned partition that has none', async () => {
    // Without a committed offset kafkajs re-resolves 'latest' on EVERY join,
    // so a reply appended while the consumer was rejoining is skipped for
    // good: the responder produced it, the topic holds it, and the caller
    // times out anyway. The pin is what turns that loss into a late delivery.
    const adapter = new FakeAdapter();
    const transport = await bootClientOnly(adapter);
    const reply = adapter.replyConsumers()[0]!;

    await waitFor(() => replyPins(reply).length === 4, 5_000, 'reply start pins committed');

    expect(replyPins(reply)).toEqual(['0:10', '1:20', '2:30', '3:40']);

    await transport.destroy({ drainTimeout: 0 });
  });

  it('leaves a partition that already has a committed offset alone', async () => {
    const adapter = new FakeAdapter();

    // Partition 2 already carries a position - overwriting it with the
    // pre-run watermark would rewind (or skip) a live consumer
    adapter.replyCommitted.set(2, '7');

    const transport = await bootClientOnly(adapter);
    const reply = adapter.replyConsumers()[0]!;

    await waitFor(() => replyPins(reply).length === 3, 5_000, 'reply start pins committed');

    expect(replyPins(reply)).toEqual(['0:10', '1:20', '3:40']);

    await transport.destroy({ drainTimeout: 0 });
  });

  it('pins the pre-outage watermark on recreate, never the current one', async () => {
    const adapter = new FakeAdapter();
    const transport = await bootClientOnly(adapter);
    const first = adapter.replyConsumers()[0]!;

    await waitFor(() => replyPins(first).length === 4, 5_000, 'initial reply start pins');

    // The outage: the pin commits never reached the broker, and meanwhile the
    // reply topic advanced - including the reply this caller is waiting for.
    // Re-capturing the watermark here would pin PAST that reply, which is the
    // very loss the pin exists to prevent.
    adapter.replyCommitted.clear();
    adapter.high = new Map(REPLY_PARTITIONS.map((partition) => [partition, String(1_000 + partition)]));

    first.emit(EVENTS.CRASH, { payload: { error: new Error('Connection error'), restart: false } });

    await waitFor(() => adapter.replyConsumers().length >= 2, 5_000, 'reply consumer recreated');

    const second = adapter.replyConsumers()[1]!;

    await waitFor(() => replyPins(second).length === 4, 5_000, 'reply start pins after recreate');

    expect(replyPins(second)).toEqual(['0:10', '1:20', '2:30', '3:40']);

    await transport.destroy({ drainTimeout: 0 });
  });
});

describe('readiness reflects the reply consumer', () => {
  it('reports unhealthy until a rejoined reply consumer is fetching again', async () => {
    const adapter = new FakeAdapter();
    const transport = await bootClientOnly(adapter);
    const reply = adapter.replyConsumers()[0]!;

    expect(transport.isConnected).toBe(true);

    // Connection lost. The admin metadata probe is untouched by this - it is
    // exactly what used to keep /healthz green while every send() timed out.
    reply.emit(EVENTS.DISCONNECT, {});
    expect(transport.isConnected).toBe(false);

    // Joined but not yet fetching: 'latest' is resolved between these two
    // points, so the group being Stable proves nothing on its own
    reply.emit(EVENTS.GROUP_JOIN, { payload: { memberAssignment: { [REPLY_TOPIC]: [...REPLY_PARTITIONS] } } });
    expect(transport.isConnected).toBe(false);

    reply.emit(EVENTS.FETCH, {});
    expect(transport.isConnected).toBe(true);

    await transport.destroy({ drainTimeout: 0 });
  });

  it('reports unhealthy when the fetch loop stalls without any event', async () => {
    // kafkajs only raises CRASH/DISCONNECT once its own retries are spent
    // (~7.5s measured against a restarting broker); until then a dead fetch
    // loop emits nothing at all, so a stalled loop must degrade readiness on
    // its own.
    const adapter = new FakeAdapter();
    const transport = await bootClientOnly(adapter);

    expect(transport.isConnected).toBe(true);

    try {
      setSystemTime(new Date(Date.now() + 6_000));
      expect(transport.isConnected).toBe(false);
    } finally {
      setSystemTime();
    }

    expect(transport.isConnected).toBe(true);

    await transport.destroy({ drainTimeout: 0 });
  });

  it('recovers readiness once the replacement consumer is fetching', async () => {
    // A short probe interval so the admin half of isConnected (which a CRASH
    // also knocks down) comes back on its own - the point under test is that
    // the reply half is not stuck false after a recreate.
    const adapter = new FakeAdapter();
    const transport = await bootClientOnly(adapter, 100);
    const first = adapter.replyConsumers()[0]!;

    first.emit(EVENTS.CRASH, { payload: { error: new Error('Connection error'), restart: false } });
    expect(transport.isConnected).toBe(false);

    await waitFor(() => adapter.replyConsumers().length >= 2, 5_000, 'reply consumer recreated');
    await waitFor(() => transport.isConnected, 5_000, 'readiness restored by the replacement');

    await transport.destroy({ drainTimeout: 0 });
  });
});
