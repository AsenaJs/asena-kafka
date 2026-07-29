import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { AsenaKafkaService } from '../lib/AsenaKafkaService';
import type { KafkaOptions } from '../lib/types';
import { BROKERS, ensureTestTopic, quietLogger, readTopic, sweepNamespace, uniquePrefix } from './util';

const prefix = uniquePrefix('service');

class TestKafkaService extends AsenaKafkaService {
  public initWithOptions(options: KafkaOptions) {
    this.setKafkaOptions(options);
  }
}

describe('AsenaKafkaService', () => {
  let service: TestKafkaService;

  beforeAll(async () => {
    service = new TestKafkaService();
    service.initWithOptions({ config: { brokers: BROKERS, name: 'test' }, logger: quietLogger });
    await service.onStart();
  }, 30000);

  afterAll(async () => {
    await service.disconnect();
    await sweepNamespace(prefix);
  }, 30000);

  describe('Lifecycle', () => {
    it('should throw if options not set', async () => {
      const bare = new TestKafkaService();

      await expect(bare.onStart()).rejects.toThrow('Kafka options not initialized');
    });

    it('should connect and report a healthy connection', async () => {
      expect(service.client.isConnected).toBe(true);
      expect(await service.testConnection()).toBe(true);
    });

    it('should expose the config', () => {
      expect(service.config.brokers).toEqual(BROKERS);
      expect(service.config.name).toBe('test');
    });

    it('should throw on client access before start', () => {
      const bare = new TestKafkaService();

      expect(() => bare.client).toThrow('not initialized');
    });

    it('should release the client from the stop hook', async () => {
      const scoped = new TestKafkaService();

      scoped.initWithOptions({ config: { brokers: BROKERS }, logger: quietLogger });
      await scoped.onStart();

      expect(scoped.client.isConnected).toBe(true);

      await scoped.onStop();

      expect(() => scoped.client).toThrow('not initialized');

      // Shutdown containment: a second pass over a component that already let go must not throw
      await scoped.onStop();
    }, 30000);
  });

  describe('Produce & factories', () => {
    it('should produce messages via sendMessage', async () => {
      const topic = `${prefix}.produce`;

      await ensureTestTopic(topic);

      const metadata = await service.sendMessage(topic, [{ value: 'hello', headers: { k: 'v' } }]);

      expect(metadata[0].errorCode).toBe(0);

      const records = await readTopic(topic, 1);

      expect(records[0].value).toBe('hello');
      expect(records[0].headers['k']).toBe('v');
    }, 30000);

    it('should hand out caller-owned consumers and producers', async () => {
      const producer = service.createProducer();

      await producer.connect();
      await producer.disconnect();

      const consumer = service.createConsumer({ groupId: `${prefix}.factory-group` });

      await consumer.connect();
      await consumer.disconnect();

      // The service's own connection is untouched by factory lifecycles
      expect(service.client.isConnected).toBe(true);
    }, 30000);
  });

  describe('Custom client', () => {
    it('should use a provided custom client without reconnecting it', async () => {
      const custom = new TestKafkaService();

      custom.initWithOptions({ config: { brokers: BROKERS }, client: service.client, logger: quietLogger });
      await custom.onStart();

      expect(custom.client).toBe(service.client);
    });
  });

  describe('testConnection', () => {
    it('should return false when never connected', async () => {
      const bare = new TestKafkaService();

      expect(await bare.testConnection()).toBe(false);
    });
  });
});
