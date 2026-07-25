// Core Service
export { AsenaKafkaService } from './lib/AsenaKafkaService';

// Microservice transport
export { KafkaMicroserviceTransport } from './lib/microservice';

// Adapter
export type {
  KafkaClientAdapter,
  KafkaProducerLike,
  KafkaConsumerLike,
  KafkaAdminLike,
  KafkaConsumerConfig,
  KafkaProducerRecord,
  KafkaRecordMetadata,
  KafkaMessageLike,
  KafkaEachMessagePayload,
  KafkaHeaderValue,
} from './lib/adapter';
export { KafkajsAdapter } from './lib/adapter';

// Decorators
export { Kafka } from './lib/decorators';
export type { KafkaDecoratorOptions } from './lib/decorators';

// Types
export type {
  KafkaConfig,
  KafkaOptions,
  KafkaMicroserviceOptions,
  KafkaExternalTopic,
  KafkaExternalOptions,
} from './lib/types';
