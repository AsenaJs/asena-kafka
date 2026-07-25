import { afterEach, describe, expect, it } from 'bun:test';
import { KafkaMicroserviceTransport } from '../lib/microservice';
import { BROKERS, sweepNamespace, uniquePrefix, waitFor } from './util';

const FAST = {
  sessionTimeout: 3000,
  heartbeatInterval: 800,
  healthCheckIntervalMs: 1000,
  drainTimeout: 500,
};

/**
 * The load-bearing scenario of the attempt-marker protocol: instance A fails
 * an event and shuts down WITHOUT advancing the offset; instance B of the
 * same group must derive attempt=2 from the offset-commit metadata (there is
 * no shared process state - the count travels through the broker).
 */
describe('Broker-persisted attempt tracking across instances', () => {
  let a: KafkaMicroserviceTransport | undefined;
  let b: KafkaMicroserviceTransport | undefined;
  const topicPrefix = uniquePrefix('attempt');

  afterEach(async () => {
    await a?.destroy({ drainTimeout: 500 }).catch(() => {});
    await b?.destroy({ drainTimeout: 500 }).catch(() => {});
    a = undefined;
    b = undefined;
    await sweepNamespace(topicPrefix);
  }, 30000);

  it('should hand a failed event to a successor instance with attempt=2', async () => {
    const seenByA: { attempt: number; messageId: string }[] = [];
    const seenByB: { attempt: number; messageId: string }[] = [];

    // Long retry backoff: A must NOT retry locally before it is destroyed -
    // the redelivery has to happen on B, driven purely by broker state
    a = new KafkaMicroserviceTransport(
      { brokers: BROKERS },
      { serviceName: 'attempt-svc', topicPrefix, maxRetries: 5, retryBackoffMs: 60000, ...FAST },
    );

    a.registerEventHandler('booking.created', async (_data, context) => {
      seenByA.push({ attempt: context.attempt, messageId: context.messageId });
      throw new Error('transient failure');
    });

    await a.init();
    await a.listen();

    await a.emit('booking.created', { ref: 'r1' });

    await waitFor(() => seenByA.length >= 1, 20000, 'first (failing) attempt on A');

    expect(seenByA[0].attempt).toBe(1);

    // Graceful shutdown: the failed offset stays uncommitted (marker intact)
    await a.destroy({ drainTimeout: 500 });
    a = undefined;

    b = new KafkaMicroserviceTransport(
      { brokers: BROKERS },
      { serviceName: 'attempt-svc', topicPrefix, maxRetries: 5, retryBackoffMs: 300, ...FAST },
    );

    b.registerEventHandler('booking.created', async (_data, context) => {
      seenByB.push({ attempt: context.attempt, messageId: context.messageId });
    });

    await b.init();
    await b.listen();

    await waitFor(() => seenByB.length >= 1, 30000, 'redelivery on B');

    // The broker told B this is delivery #2 - no process shared any state
    expect(seenByB[0].attempt).toBe(2);
    // Same emit, same identity - across instances and redeliveries
    expect(seenByB[0].messageId).toBe(seenByA[0].messageId);
  }, 90000);
});
