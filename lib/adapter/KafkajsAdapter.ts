import { Kafka, logLevel as KafkaJSLogLevel } from 'kafkajs';
import type { Producer } from 'kafkajs';
import type {
  KafkaAdminLike,
  KafkaClientAdapter,
  KafkaConsumerConfig,
  KafkaConsumerLike,
  KafkaProducerLike,
  KafkaProducerRecord,
  KafkaRecordMetadata,
} from './KafkaClientAdapter';
import type { KafkaConfig } from '../types';

/**
 * KafkaClientAdapter implementation backed by kafkajs.
 * kafkajs is the required peer dependency, so it is imported statically;
 * future alternative adapters should lazy-load their client instead.
 */
export class KafkajsAdapter implements KafkaClientAdapter {
  private kafka: Kafka;

  private defaultProducer: Producer | null = null;

  private connected = false;

  public constructor(config: KafkaConfig) {
    const { name: _name, brokers, clientId, logLevel, ...rest } = config;

    this.kafka = new Kafka({
      clientId: clientId ?? 'asena-kafka',
      brokers,
      logLevel: logLevel ?? KafkaJSLogLevel.NOTHING,
      ...rest,
    });
  }

  public get isConnected(): boolean {
    return this.connected;
  }

  public async connect(): Promise<void> {
    if (this.connected) return;

    this.defaultProducer = this.kafka.producer({ allowAutoTopicCreation: false });

    const remove = this.defaultProducer.on(this.defaultProducer.events.DISCONNECT, () => {
      this.connected = false;
      remove();
    });

    await this.defaultProducer.connect();
    this.connected = true;
  }

  public async disconnect(): Promise<void> {
    if (this.defaultProducer) {
      await this.defaultProducer.disconnect();
      this.defaultProducer = null;
    }

    this.connected = false;
  }

  public producer(): KafkaProducerLike {
    return this.kafka.producer({ allowAutoTopicCreation: false });
  }

  public consumer(config: KafkaConsumerConfig): KafkaConsumerLike {
    return this.kafka.consumer({ allowAutoTopicCreation: false, ...config }) as unknown as KafkaConsumerLike;
  }

  public admin(): KafkaAdminLike {
    return this.kafka.admin() as unknown as KafkaAdminLike;
  }

  public async send(record: KafkaProducerRecord): Promise<KafkaRecordMetadata[]> {
    if (!this.defaultProducer) {
      throw new Error('Kafka producer not connected. Call connect() first.');
    }

    return this.defaultProducer.send(record);
  }
}
