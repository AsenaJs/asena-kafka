import { describe, expect, it } from 'bun:test';
import { Kafka } from '../lib/decorators';
import { AsenaKafkaService } from '../lib/AsenaKafkaService';

const CONFIG = { brokers: ['localhost:9092'] };

describe('@Kafka decorator', () => {
  it('should return a class extending AsenaKafkaService', () => {
    @Kafka({ config: CONFIG })
    class MyKafka extends AsenaKafkaService {}

    const instance = new MyKafka();

    expect(instance).toBeInstanceOf(AsenaKafkaService);
  });

  it('should preserve original class name', () => {
    @Kafka({ config: CONFIG })
    class EventBusService extends AsenaKafkaService {}

    expect(EventBusService.name).toBe('EventBusService');
  });

  it('should copy prototype methods from target', () => {
    @Kafka({ config: CONFIG })
    class CustomKafka extends AsenaKafkaService {
      public customMethod(): string {
        return 'custom';
      }
    }

    const instance = new CustomKafka();

    expect(instance.customMethod()).toBe('custom');
  });

  it('should copy static properties from target', () => {
    @Kafka({ config: CONFIG })
    class StaticKafka extends AsenaKafkaService {
      public static VERSION = '1.0.0';
    }

    expect(StaticKafka.VERSION).toBe('1.0.0');
  });

  it('should set default logger to console when not provided', () => {
    const options: any = { config: CONFIG };

    @Kafka(options)
    class LogKafka extends AsenaKafkaService {}

    const _ = new LogKafka();

    expect(options.logger).toBe(console);
  });

  it('should not override provided logger', () => {
    const customLogger = { info: () => {}, error: () => {}, warn: () => {}, debug: () => {} };
    const options: any = { config: CONFIG, logger: customLogger };

    @Kafka(options)
    class CustomLogKafka extends AsenaKafkaService {}

    const _ = new CustomLogKafka();

    expect(options.logger).toBe(customLogger);
  });
});
