import { PostConstruct } from '@asenajs/asena/decorators/ioc';
import type {
  KafkaAdminLike,
  KafkaClientAdapter,
  KafkaConsumerConfig,
  KafkaConsumerLike,
  KafkaMessageLike,
  KafkaProducerLike,
  KafkaRecordMetadata,
} from './adapter';
import { KafkajsAdapter } from './adapter';
import type { KafkaConfig, KafkaOptions } from './types';

export abstract class AsenaKafkaService {
  protected _client: KafkaClientAdapter | null = null;

  protected options: KafkaOptions | null = null;

  @PostConstruct()
  public async onStart() {
    if (!this.options) {
      throw new Error('Kafka options not initialized. Make sure to use @Kafka decorator properly.');
    }

    // If a custom client was provided, use it directly
    if (this.options.client) {
      this._client = this.options.client;

      if (!this._client.isConnected) {
        await this._client.connect();
      }

      this.options.logger?.info(
        `Kafka Connected (custom client)${this.options.config.name ? ` - ${this.options.config.name}` : ''}`,
      );

      return;
    }

    try {
      this._client = new KafkajsAdapter(this.options.config);

      await this._client.connect();

      this.options.logger?.info(`Kafka Connected${this.options.config.name ? ` - ${this.options.config.name}` : ''}`);
    } catch (error) {
      this.options.logger?.error('Kafka connection failed:', error);
      throw new Error(`Kafka connection failed: ${error}`);
    }
  }

  // Produce

  public async sendMessage(topic: string, messages: KafkaMessageLike[]): Promise<KafkaRecordMetadata[]> {
    return this.getClient().send({ topic, messages });
  }

  // Factories - returned objects are owned (connected/disconnected) by the caller

  public createProducer(): KafkaProducerLike {
    return this.getClient().producer();
  }

  public createConsumer(config: KafkaConsumerConfig): KafkaConsumerLike {
    return this.getClient().consumer(config);
  }

  public createAdmin(): KafkaAdminLike {
    return this.getClient().admin();
  }

  // Client access

  public get client(): KafkaClientAdapter {
    return this.getClient();
  }

  public get config(): KafkaConfig {
    if (!this.options) {
      throw new Error('Kafka options not initialized.');
    }

    return this.options.config;
  }

  // Lifecycle

  public async disconnect(): Promise<void> {
    if (this._client) {
      await this._client.disconnect();
      this._client = null;
    }
  }

  public async testConnection(): Promise<boolean> {
    if (!this._client || !this._client.isConnected) {
      return false;
    }

    const admin = this._client.admin();

    try {
      await admin.connect();

      const probe = admin.listTopics();
      const bound = new Promise<never>((_, reject) => setTimeout(() => reject(new Error('probe timeout')), 3000));

      await Promise.race([probe, bound]);

      return true;
    } catch {
      return false;
    } finally {
      await admin.disconnect().catch(() => {});
    }
  }

  protected setKafkaOptions(options: KafkaOptions): void {
    this.options = options;
  }

  protected setKafkaClient(client: KafkaClientAdapter): void {
    this._client = client;
  }

  private getClient(): KafkaClientAdapter {
    if (!this._client) {
      throw new Error('Kafka client not initialized. Service may not have started properly.');
    }

    return this._client;
  }
}
