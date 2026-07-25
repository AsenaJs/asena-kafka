/**
 * Structural types covering exactly the kafkajs surface this package uses.
 * kafkajs's own Producer/Consumer/Admin satisfy them structurally, and so does
 * any kafkajs-compatible client (e.g. @confluentinc/kafka-javascript), which
 * is what makes the adapter seam a realistic exit ramp.
 */

export type KafkaHeaderValue = Buffer | string | (Buffer | string)[] | undefined;

export interface KafkaMessageLike {
  key?: Buffer | string | null;
  value: Buffer | string | null;
  headers?: Record<string, KafkaHeaderValue>;
  partition?: number;
}

export interface KafkaProducerRecord {
  topic: string;
  messages: KafkaMessageLike[];
}

export interface KafkaRecordMetadata {
  topicName: string;
  partition: number;
  baseOffset?: string;
  errorCode: number;
}

export interface KafkaProducerLike {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(record: KafkaProducerRecord): Promise<KafkaRecordMetadata[]>;
}

export interface KafkaConsumedMessage {
  key: Buffer | null;
  value: Buffer | null;
  headers?: Record<string, KafkaHeaderValue>;
  offset: string;
  timestamp: string;
}

export interface KafkaEachMessagePayload {
  topic: string;
  partition: number;
  message: KafkaConsumedMessage;
  heartbeat(): Promise<void>;
}

export interface KafkaTopicPartitions {
  topic: string;
  partitions?: number[];
}

export interface KafkaOffsetCommit {
  topic: string;
  partition: number;
  offset: string;
  metadata?: string | null;
}

export interface KafkaConsumerEvents {
  readonly GROUP_JOIN: string;
  readonly CRASH: string;
  readonly FETCH: string;
  readonly DISCONNECT: string;
  readonly STOP: string;
}

export interface KafkaConsumerLike {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  stop(): Promise<void>;
  subscribe(subscription: { topics: string[]; fromBeginning?: boolean }): Promise<void>;
  run(config: {
    autoCommit?: boolean;
    partitionsConsumedConcurrently?: number;
    eachMessage: (payload: KafkaEachMessagePayload) => Promise<void>;
  }): Promise<void>;
  commitOffsets(offsets: KafkaOffsetCommit[]): Promise<void>;
  seek(entry: { topic: string; partition: number; offset: string }): void;
  pause(topics: KafkaTopicPartitions[]): void;
  resume(topics: KafkaTopicPartitions[]): void;
  on(eventName: string, listener: (event: any) => void): unknown;
  readonly events: KafkaConsumerEvents;
}

export interface KafkaConsumerConfig {
  groupId: string;
  sessionTimeout?: number;
  heartbeatInterval?: number;
  rebalanceTimeout?: number;
  maxWaitTimeInMs?: number;
  allowAutoTopicCreation?: boolean;
}

export interface KafkaTopicConfig {
  topic: string;
  numPartitions?: number;
  replicationFactor?: number;
}

export interface KafkaFetchedOffsets {
  topic: string;
  partitions: { partition: number; offset: string; metadata: string | null }[];
}

export interface KafkaAdminLike {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  createTopics(options: { waitForLeaders?: boolean; topics: KafkaTopicConfig[] }): Promise<boolean>;
  deleteTopics(options: { topics: string[] }): Promise<void>;
  listTopics(): Promise<string[]>;
  deleteGroups(groupIds: string[]): Promise<unknown>;
  fetchOffsets(options: { groupId: string; topics?: string[] }): Promise<KafkaFetchedOffsets[]>;
  fetchTopicMetadata(options?: { topics?: string[] }): Promise<unknown>;
  fetchTopicOffsets(topic: string): Promise<{ partition: number; offset: string; high: string; low: string }[]>;
}

/**
 * Adapter interface for Kafka clients.
 * A factory-style seam: the adapter owns one default producer (for the service
 * layer's convenience API) and hands out fresh producer/consumer/admin objects
 * to callers that need their own lifecycle - the microservice transport always
 * creates its OWN objects and never shares the default producer.
 */
export interface KafkaClientAdapter {
  // Lifecycle (default producer)
  readonly isConnected: boolean;
  connect(): Promise<void>;
  disconnect(): Promise<void>;

  // Factories - returned objects are owned (connected/disconnected) by the caller
  producer(): KafkaProducerLike;
  consumer(config: KafkaConsumerConfig): KafkaConsumerLike;
  admin(): KafkaAdminLike;

  // Convenience produce via the default producer
  send(record: KafkaProducerRecord): Promise<KafkaRecordMetadata[]>;
}
