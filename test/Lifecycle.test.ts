import { afterEach, describe, expect, it } from 'bun:test';
import { AsenaServerFactory } from '@asenajs/asena';
import { Container } from '@asenajs/asena/container';
import { AsenaKafkaService } from '../lib/AsenaKafkaService';
import { Kafka } from '../lib/decorators';
import type {
  KafkaAdminLike,
  KafkaClientAdapter,
  KafkaConsumerLike,
  KafkaProducerLike,
  KafkaRecordMetadata,
} from '../lib/adapter';
import { quietLogger } from './util';

const CONFIG = { brokers: ['localhost:9092'] };

/**
 * Offline: the stop hook is a wiring question, not a Kafka one. A stub client records who
 * closed what, which is exactly what has to be proven - the service releases the client it
 * runs on, and leaves the factory objects to the caller that asked for them.
 */

class StubProducer implements KafkaProducerLike {
  public disconnects = 0;

  async connect(): Promise<void> {}

  async disconnect(): Promise<void> {
    this.disconnects++;
  }

  async send(): Promise<KafkaRecordMetadata[]> {
    return [];
  }
}

class StubClient implements KafkaClientAdapter {
  public isConnected = false;

  public disconnects = 0;

  public readonly handedOut: StubProducer[] = [];

  async connect(): Promise<void> {
    this.isConnected = true;
  }

  async disconnect(): Promise<void> {
    this.disconnects++;
    this.isConnected = false;
  }

  producer(): KafkaProducerLike {
    const producer = new StubProducer();

    this.handedOut.push(producer);

    return producer;
  }

  consumer(): KafkaConsumerLike {
    throw new Error('consumer() is not exercised here');
  }

  admin(): KafkaAdminLike {
    throw new Error('admin() is not exercised here');
  }

  async send(): Promise<KafkaRecordMetadata[]> {
    return [];
  }
}

describe('AsenaKafkaService lifecycle hooks', () => {
  let server: any;

  afterEach(async () => {
    await server?.stop();
    server = undefined;
  });

  it('should declare both hooks where the lifecycle looks for them', () => {
    @Kafka({ config: CONFIG, name: 'HookProbeKafka' })
    class HookProbeKafka extends AsenaKafkaService {}

    // Same reads LifecycleService does, and through the wrapper class @Kafka returns rather
    // than the base - the hooks only fire if they survive that replacement.
    const container = new Container();

    expect(container.getStartHooks(HookProbeKafka as any)).toContain('onStart');
    expect(container.getStopHooks(HookProbeKafka as any)).toContain('onStop');
  });

  it('should release the client on server.stop() and leave caller-owned objects alone', async () => {
    const client = new StubClient();

    @Kafka({ config: CONFIG, client, logger: quietLogger, name: 'StoppableKafka' })
    class StoppableKafka extends AsenaKafkaService {}

    server = await AsenaServerFactory.create({
      headless: true,
      logger: quietLogger,
      components: [StoppableKafka],
    });

    await server.start();

    const service = (await server.coreContainer.container.resolve('StoppableKafka')) as AsenaKafkaService;

    expect(client.isConnected).toBe(true);

    // A producer the application owns: handed out before shutdown, never registered anywhere
    const owned = service.createProducer() as StubProducer;

    await owned.connect();

    await server.stop();
    server = undefined;

    expect(client.disconnects).toBe(1);
    expect(client.isConnected).toBe(false);
    expect(() => service.client).toThrow('not initialized');

    // The contract the factories promise: the framework does not close these
    expect(owned.disconnects).toBe(0);
  }, 30000);
});
