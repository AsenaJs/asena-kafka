import type { KafkaConfig as KafkaJSClientConfig } from 'kafkajs';
import type { ServerLogger } from '@asenajs/asena/logger';
import type { KafkaClientAdapter } from '../adapter';

/**
 * Connection configuration, passed through to the kafkajs `Kafka` constructor.
 * `name` is Asena's display name for the connection and is not forwarded.
 */
export interface KafkaConfig extends Omit<KafkaJSClientConfig, 'brokers' | 'clientId'> {
  name?: string;
  clientId?: string;
  brokers: string[];
}

export interface KafkaOptions {
  config: KafkaConfig;
  client?: KafkaClientAdapter;
  logger?: ServerLogger;
}

export interface KafkaDecoratorOptions extends KafkaOptions {
  name?: string;
}

/**
 * One foreign-owned topic the transport interoperates with. The plain string
 * form is shorthand for `{ name }`.
 */
export type KafkaExternalTopic =
  | string
  | {
      name: string;

      /**
       * Outbound only: the value of this user header becomes the record KEY
       * (partition affinity on the foreign topic). The header itself is still
       * sent verbatim alongside the rest.
       */
      keyHeader?: string;
    };

/**
 * Interop with topics produced/consumed by non-Asena systems (Quarkus,
 * SmallRye, plain Kafka clients). External topics carry NO Asena envelope:
 * inbound records dispatch under the TOPIC NAME as pattern with the raw
 * record headers exposed, outbound emits publish plain JSON with the user
 * headers verbatim. External topics are event-only (no request/reply) and are
 * never created by the transport - they are foreign property.
 */
export interface KafkaExternalOptions {
  topics: KafkaExternalTopic[];

  /**
   * Start position for the FIRST subscribe (no committed group offset yet):
   * `true` reads each external topic from the earliest retained record,
   * `false` from latest.
   * @default false
   */
  fromBeginning?: boolean;
}

/**
 * Options for KafkaMicroserviceTransport.
 */
export interface KafkaMicroserviceOptions {
  /**
   * REQUIRED - consumer group identity. All replicas of the same service must
   * share this name; different services must use different names (each service
   * group receives its own copy of every event). The actual Kafka group id is
   * prefix-scoped: `{topicPrefix}.{serviceName}`.
   */
  serviceName: string;

  /**
   * Topic/group name prefix. Must contain only Kafka-legal characters
   * (`[a-zA-Z0-9._-]`).
   * @default 'asena.ms'
   */
  topicPrefix?: string;

  /**
   * Default reply timeout for send() in milliseconds
   * @default 30000
   */
  requestTimeout?: number;

  /**
   * Max delivery attempts for EVENT handlers before the record moves to the DLQ
   * topic. RPC handlers are never retried (errors are final).
   * @default 3
   */
  maxRetries?: number;

  /**
   * Delay before a failed event's partition is resumed for the retry fetch
   * (pause -> seek -> resume). Keep well below sessionTimeout.
   * @default 5000
   */
  retryBackoffMs?: number;

  /**
   * Per-handler execution timeout in milliseconds. Must not exceed
   * sessionTimeout - kafkajs cannot heartbeat while eachMessage runs, so a
   * handler outliving sessionTimeout gets the member evicted and the message
   * concurrently redelivered elsewhere; explicit values above sessionTimeout
   * throw at construction. Note: on timeout the dispatch is rejected but the
   * handler itself keeps running (no cancellation).
   * @default min(30000, sessionTimeout)
   */
  handlerTimeout?: number;

  /**
   * Max partitions processed concurrently (kafkajs
   * partitionsConsumedConcurrently). Within a partition processing is strictly
   * sequential - that ordering is what makes broker-persisted attempt markers
   * sound.
   * @default 16
   */
  maxInFlight?: number;

  /**
   * Default graceful drain timeout for destroy() in milliseconds
   * @default 10000
   */
  drainTimeout?: number;

  /**
   * Consumer group session timeout (ms), passed to kafkajs. Lower values give
   * faster crash detection/rebalance at the cost of more heartbeat traffic.
   * The broker's `group.min.session.timeout.ms` bounds how low this can go.
   * @default 30000
   */
  sessionTimeout?: number;

  /**
   * Consumer heartbeat interval (ms), passed to kafkajs. Must be well below
   * sessionTimeout (rule of thumb: at most 1/3).
   * @default 3000
   */
  heartbeatInterval?: number;

  /**
   * Max time (ms) a rebalance may take before a member is dropped, passed to
   * kafkajs.
   * @default 60000
   */
  rebalanceTimeout?: number;

  /**
   * Broker long-poll duration (ms) for an idle fetch (kafkajs
   * maxWaitTimeInMs). Data arriving mid-poll returns immediately, so this
   * only bounds idle-cycle latency: boot readiness (listen() waits for the
   * first completed fetch) and shutdown responsiveness. ~1/maxWait fetch
   * requests per second per idle consumer.
   * @default 1000
   */
  maxWaitTimeInMs?: number;

  /**
   * Partition count for the shared event topic (`{prefix}.evt`). More
   * partitions = more parallelism, but per-consumer publish order is only
   * preserved with a single partition.
   * @default 4
   */
  eventPartitions?: number;

  /**
   * Partition count for request topics (`{prefix}.req.{pattern}`).
   * @default 4
   */
  requestPartitions?: number;

  /**
   * Partition count for the shared reply topic (`{prefix}.reply`).
   * @default 4
   */
  replyPartitions?: number;

  /**
   * Replication factor for transport-created topics. -1 uses the broker
   * default.
   * @default -1
   */
  replicationFactor?: number;

  /**
   * Interval (ms) of the active broker health probe (bounded metadata fetch).
   * A failed probe flips isConnected to false (health endpoint reports 503)
   * until a probe succeeds again.
   * @default 5000
   */
  healthCheckIntervalMs?: number;

  /**
   * Foreign-topic interop (see KafkaExternalOptions). Controllers stay
   * transport-agnostic: an `@EventPattern` matching an external topic NAME
   * receives its records, `emit(topicName)` publishes to it raw.
   */
  external?: KafkaExternalOptions;
}
