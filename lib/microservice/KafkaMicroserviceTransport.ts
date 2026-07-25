import { UlakError, UlakErrorCode } from '@asenajs/asena/messaging';
import { PatternHandlerIndex } from '@asenajs/asena/event';
import type {
  DestroyOptions,
  EmitOptions,
  EventPatternHandler,
  MessageHandler,
  MicroserviceTransport,
  SendOptions,
} from '@asenajs/asena/microservice';
import type {
  KafkaAdminLike,
  KafkaClientAdapter,
  KafkaConsumerLike,
  KafkaEachMessagePayload,
  KafkaProducerLike,
  KafkaTopicConfig,
} from '../adapter';
import { KafkajsAdapter } from '../adapter';
import type { AsenaKafkaService } from '../AsenaKafkaService';
import type { KafkaConfig, KafkaMicroserviceOptions } from '../types';
import {
  buildContext,
  buildEventHeaders,
  buildExternalContext,
  buildRequestHeaders,
  decodeHeaders,
  decodeMarker,
  encodeMarker,
  parsePayload,
} from './envelope';
import type { TransportReply } from './envelope';
import { TopicNaming, assertTopicLegal } from './topics';

const DEFAULT_TOPIC_PREFIX = 'asena.ms';
const DEFAULT_REQUEST_TIMEOUT = 30_000;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_RETRY_BACKOFF = 5_000;
const DEFAULT_HANDLER_TIMEOUT = 30_000;
const DEFAULT_MAX_IN_FLIGHT = 16;
const DEFAULT_DRAIN_TIMEOUT = 10_000;
const DEFAULT_SESSION_TIMEOUT = 30_000;
const DEFAULT_HEARTBEAT_INTERVAL = 3_000;
const DEFAULT_REBALANCE_TIMEOUT = 60_000;
const DEFAULT_PARTITIONS = 4;
const DEFAULT_HEALTH_CHECK_INTERVAL = 5_000;
const RECONNECT_BACKOFF_START = 1_000;
const RECONNECT_BACKOFF_CAP = 30_000;
const SHUTDOWN_STEP_TIMEOUT = 1_000;
const CONSUMER_DISCONNECT_TIMEOUT = 5_000;
const MARKER_LOAD_TIMEOUT = 2_000;
const REPLY_READY_TIMEOUT = 15_000;
const LISTEN_READY_TIMEOUT = 30_000;
const DEFAULT_MAX_WAIT_TIME = 1_000;
const REPLY_MAX_WAIT_TIME = 500;
const TOPIC_VISIBILITY_TIMEOUT = 10_000;
const START_PIN_TIMEOUT = 5_000;
const STARTUP_RETRY_TIMEOUT = 20_000;

interface PendingRequest {
  resolve: (value: any) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

interface AttemptMarker {
  offset: number;
  attempts: number;
}

/**
 * @description Kafka microservice transport (kafkajs-based).
 *
 * Delivery model:
 * - Events: single shared topic `{prefix}.evt` (keyless, round-robin across
 *   partitions), one consumer group per service (`{prefix}.{serviceName}`).
 *   Every service group receives a copy; inside a group exactly one replica
 *   processes each record. Wildcard patterns are matched locally; records
 *   matching no local handler are committed immediately. At-least-once:
 *   handler errors leave the offset uncommitted and the partition is
 *   paused/seeked back for a broker re-fetch, up to maxRetries, then the
 *   record moves to the `{prefix}.dlq` topic with provenance headers.
 * - Requests: one topic per exact pattern `{prefix}.req.{pattern}`, consumer
 *   group per responding service. RPC errors are FINAL: the caller gets an
 *   `ok:false` reply and the offset is committed (no broker retry). Requests
 *   older than the caller's own timeout (envelope header `to`) are committed
 *   without execution - the caller has already given up.
 * - Replies: one shared topic `{prefix}.reply`; every caller instance runs an
 *   ephemeral consumer group (`{prefix}.{service}.reply.{instance8}`) and
 *   filters by correlationId. The group is deleted on destroy; abandoned
 *   groups are reaped by the broker's offsets.retention.
 *
 * Attempt tracking (broker-persisted): before dispatching the record at
 * offset X the transport commits offset X (= "next fetch starts at X") with
 * metadata `{"a":attempt}`; on success it commits X+1 with empty metadata.
 * A crash therefore redelivers X, and the successor derives attempt = a+1
 * from the committed metadata (loaded on every GROUP_JOIN). Retry and crash
 * recovery share this one mechanism. The cost is two synchronous commits per
 * record per partition - the price of broker-tracked `context.attempt`;
 * throughput scales by adding partitions.
 *
 * Operational notes:
 * - Handlers should be idempotent: duplicate delivery is possible
 *   (at-least-once). Use context.messageId (`mid` header) for deduplication.
 * - Handler duration must stay below sessionTimeout: kafkajs cannot heartbeat
 *   while eachMessage runs, so a longer handler gets the member evicted and
 *   the record concurrently redelivered elsewhere.
 * - Per-partition processing is strictly sequential; parallelism is across
 *   partitions (maxInFlight = partitionsConsumedConcurrently).
 */
export class KafkaMicroserviceTransport implements MicroserviceTransport {
  public readonly name = 'kafka';

  private readonly instanceId = crypto.randomUUID();

  private readonly source: AsenaKafkaService | KafkaConfig;

  private readonly serviceName: string;

  private readonly naming: TopicNaming;

  private readonly requestTimeout: number;

  private readonly maxRetries: number;

  private readonly retryBackoffMs: number;

  private readonly handlerTimeout: number;

  private readonly maxInFlight: number;

  private readonly defaultDrainTimeout: number;

  private readonly sessionTimeout: number;

  private readonly heartbeatInterval: number;

  private readonly rebalanceTimeout: number;

  private readonly maxWaitTimeInMs: number;

  private readonly eventPartitions: number;

  private readonly requestPartitions: number;

  private readonly replyPartitions: number;

  private readonly replicationFactor: number;

  private readonly healthCheckIntervalMs: number;

  private adapter!: KafkaClientAdapter;

  private producer?: KafkaProducerLike;

  private admin?: KafkaAdminLike;

  private consumer?: KafkaConsumerLike;

  private replyConsumer?: KafkaConsumerLike;

  private messageHandlers = new Map<string, MessageHandler>();

  private eventHandlers = new PatternHandlerIndex<EventPatternHandler>();

  /** Foreign-owned interop topics (name → per-topic outbound options). */
  private readonly externalTopics = new Map<string, { keyHeader?: string }>();

  private readonly externalFromBeginning: boolean;

  private pendingRequests = new Map<string, PendingRequest>();

  private pendingAttempts = new Map<string, AttemptMarker>();

  private markerLoad?: Promise<void>;

  private assignment = new Map<string, number[]>();

  private startPins = new Map<string, string>();

  private ensuredTopics = new Set<string>();

  private retryTimers = new Set<ReturnType<typeof setTimeout>>();

  private inFlight = new Set<Promise<void>>();

  private running = false;

  private connected = false;

  private destroyed = false;

  private destroyPromise?: Promise<void>;

  private readonly stopPromise: Promise<void>;

  private stopResolve!: () => void;

  private healthTimer?: ReturnType<typeof setInterval>;

  private lastProbeOk = true;

  private probing = false;

  public constructor(source: AsenaKafkaService | KafkaConfig, options: KafkaMicroserviceOptions) {
    if (!options?.serviceName) {
      throw new Error(
        'KafkaMicroserviceTransport requires a serviceName - it is the consumer group identity shared by all replicas of this service',
      );
    }

    assertTopicLegal(options.serviceName, 'serviceName');

    this.source = source;
    this.serviceName = options.serviceName;
    this.naming = new TopicNaming(options.topicPrefix ?? DEFAULT_TOPIC_PREFIX);
    this.requestTimeout = options.requestTimeout ?? DEFAULT_REQUEST_TIMEOUT;
    this.maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;
    this.retryBackoffMs = options.retryBackoffMs ?? DEFAULT_RETRY_BACKOFF;
    this.maxInFlight = options.maxInFlight ?? DEFAULT_MAX_IN_FLIGHT;
    this.defaultDrainTimeout = options.drainTimeout ?? DEFAULT_DRAIN_TIMEOUT;
    this.sessionTimeout = options.sessionTimeout ?? DEFAULT_SESSION_TIMEOUT;
    this.heartbeatInterval = options.heartbeatInterval ?? DEFAULT_HEARTBEAT_INTERVAL;
    this.rebalanceTimeout = options.rebalanceTimeout ?? DEFAULT_REBALANCE_TIMEOUT;
    this.maxWaitTimeInMs = options.maxWaitTimeInMs ?? DEFAULT_MAX_WAIT_TIME;
    this.eventPartitions = options.eventPartitions ?? DEFAULT_PARTITIONS;
    this.requestPartitions = options.requestPartitions ?? DEFAULT_PARTITIONS;
    this.replyPartitions = options.replyPartitions ?? DEFAULT_PARTITIONS;
    this.replicationFactor = options.replicationFactor ?? -1;
    this.healthCheckIntervalMs = options.healthCheckIntervalMs ?? DEFAULT_HEALTH_CHECK_INTERVAL;
    this.externalFromBeginning = options.external?.fromBeginning ?? false;

    for (const entry of options.external?.topics ?? []) {
      const name = typeof entry === 'string' ? entry : entry.name;
      const keyHeader = typeof entry === 'string' ? undefined : entry.keyHeader;

      assertTopicLegal(name, 'External topic');

      if (this.externalTopics.has(name)) {
        throw new Error(`Duplicate external topic "${name}"`);
      }

      // A name inside the transport's own namespace would make the dispatch
      // branching (evt/req/external) ambiguous - and onMessage's guarantee
      // that external records always take the event path relies on this
      if (
        name === this.naming.eventTopic ||
        name === this.naming.dlqTopic ||
        name === this.naming.replyTopic ||
        this.naming.isRequestTopic(name)
      ) {
        throw new Error(
          `External topic "${name}" collides with the transport's own namespace ` +
            `("${this.naming.eventTopic}", "${this.naming.dlqTopic}", "${this.naming.replyTopic}" or request topics) - pick a name outside it`,
        );
      }

      this.externalTopics.set(name, { keyHeader });
    }

    this.stopPromise = new Promise<void>((resolve) => {
      this.stopResolve = resolve;
    });

    // kafkajs cannot heartbeat while eachMessage runs: a handler outliving
    // sessionTimeout gets this member evicted and the record concurrently
    // redelivered on another replica - systematic duplicate processing. The
    // derived default keeps the invariant when only sessionTimeout is
    // lowered; an explicit conflicting value is a config error. Note:
    // handlerTimeout rejects the dispatch but does NOT cancel the handler.
    if (options.handlerTimeout !== undefined && options.handlerTimeout > this.sessionTimeout) {
      throw new Error(
        `handlerTimeout (${options.handlerTimeout}ms) must not exceed sessionTimeout (${this.sessionTimeout}ms) - ` +
          'a handler outliving sessionTimeout is redelivered to another replica while it is still running',
      );
    }

    this.handlerTimeout = options.handlerTimeout ?? Math.min(DEFAULT_HANDLER_TIMEOUT, this.sessionTimeout);
  }

  public get isConnected(): boolean {
    return this.connected && this.lastProbeOk;
  }

  public async init(): Promise<void> {
    // A borrowed AsenaKafkaService contributes only its adapter (a factory):
    // the transport always creates its OWN producer/consumers/admin, so its
    // lifecycle never touches objects the user owns.
    this.adapter = this.isKafkaService(this.source) ? this.source.client : new KafkajsAdapter(this.source);

    this.producer = this.adapter.producer();
    await this.producer.connect();

    this.admin = this.adapter.admin();
    await this.admin.connect();

    // Shared topics exist from init() so client-only send()/emit() work
    // before (or without) listen(). Explicit creation, never broker
    // auto-create: partition counts must be deterministic.
    await this.ensureTopics([
      { topic: this.naming.eventTopic, numPartitions: this.eventPartitions },
      { topic: this.naming.dlqTopic, numPartitions: 1 },
      { topic: this.naming.replyTopic, numPartitions: this.replyPartitions },
    ]);

    await this.withStartupRetry('reply consumer', () => this.startReplyConsumer());

    this.healthTimer = setInterval(() => void this.probeHealth(), this.healthCheckIntervalMs);
    this.lastProbeOk = true;
    this.connected = true;
  }

  public registerMessageHandler(pattern: string, handler: MessageHandler): void {
    // Validate the FINAL pattern: the decorator only sees the raw method
    // pattern, so a wildcard or emptiness introduced by the @MessageController
    // prefix would otherwise slip through
    if (!pattern) {
      throw new Error('Message pattern cannot be empty - check @MessagePattern and the @MessageController prefix');
    }

    if (pattern.includes('*')) {
      throw new Error(
        `Message pattern "${pattern}" cannot contain wildcards - request/response requires exact routing ` +
          '(a wildcard likely leaked in via the @MessageController prefix - remove it, or set ' +
            'prefix: false on the @MessagePattern)',
      );
    }

    // Message patterns become request-topic name segments - event patterns
    // are matched locally and stay exempt
    assertTopicLegal(pattern, 'Message pattern');

    // send() to an external pattern is rejected (external topics are
    // event-only), so a message handler under that name could never be
    // reached through this transport
    if (this.externalTopics.has(pattern)) {
      throw new Error(
        `@MessagePattern('${pattern}') collides with the external topic "${pattern}" - ` +
          'external topics are event-only, this handler would be unreachable',
      );
    }

    if (this.messageHandlers.has(pattern)) {
      throw new Error(`Duplicate @MessagePattern('${pattern}') - a message pattern can only have one handler`);
    }

    this.messageHandlers.set(pattern, handler);
  }

  public registerEventHandler(pattern: string, handler: EventPatternHandler): void {
    if (!pattern) {
      throw new Error('Event pattern cannot be empty - check @EventPattern and the @MessageController prefix');
    }

    this.eventHandlers.add(pattern, handler);
  }

  public async listen(): Promise<void> {
    const topics = this.consumedTopics();
    const outboundOnly = [...this.externalTopics.keys()].filter((name) => !topics.includes(name));

    if (outboundOnly.length) {
      const registered = this.eventHandlers.patterns();

      for (const name of outboundOnly) {
        // The usual cause since Asena 0.8: the @MessageController prefix is
        // joined onto @EventPattern too, so 'orders' became 'billing.orders'
        // and stopped matching the foreign topic name
        const shadowed = registered.filter((pattern) => pattern.endsWith(`.${name}`));

        console.log(
          `KafkaMicroserviceTransport(${this.serviceName}): external topic "${name}" has no matching event ` +
            'handler - outbound-only (emit works, nothing is consumed)' +
            (shadowed.length
              ? ` - but ${shadowed.map((pattern) => `"${pattern}"`).join(', ')} looks like the same handler with ` +
                'the @MessageController prefix joined on; add prefix: false to that @EventPattern to register ' +
                `"${name}" verbatim`
              : ''),
        );
      }
    }

    // Contract: zero handlers → no consumer, no groups (client-only mode)
    if (!topics.length) {
      return;
    }

    await this.ensureTopics(
      [...this.messageHandlers.keys()].map((pattern) => ({
        topic: this.naming.requestTopic(pattern),
        numPartitions: this.requestPartitions,
      })),
    );

    // Subscribed-to external topics must EXIST before the group subscribes -
    // but they are foreign property, so unlike ensureTopics this only waits
    // (bounded) and then fails loudly. Outbound-only topics are not checked:
    // emit errors surface per call, like every other emit.
    await this.awaitExternalTopics(topics.filter((topic) => this.externalTopics.has(topic)));

    // Pre-run 'latest' snapshot: after the group joins, these offsets are
    // committed for partitions that have no committed offset yet (start-
    // position pinning in loadMarkers). Without a committed offset, a
    // consumer crash-restart re-resolves its start position to latest-at-
    // restart and silently skips everything produced since - pinning turns
    // that permanent loss into a late delivery at worst.
    await this.captureStartPins(topics);

    this.running = true;
    await this.withStartupRetry('consumer', () => this.startMainConsumer(topics));
  }

  private async captureStartPins(topics: string[]): Promise<void> {
    const deadline = Date.now() + START_PIN_TIMEOUT;

    // With external fromBeginning:true the pins MUST be skipped: pinning
    // would commit the pre-run 'latest' snapshot and silently defeat
    // fromBeginning. Those topics need no pin anyway - a crash-restart
    // without a committed offset re-resolves to earliest, which cannot skip
    // records.
    if (this.externalFromBeginning) {
      topics = topics.filter((topic) => !this.externalTopics.has(topic));
    }

    for (const topic of topics) {
      for (;;) {
        try {
          const offsets = await this.admin!.fetchTopicOffsets(topic);

          for (const partition of offsets) {
            this.startPins.set(`${topic}:${partition.partition}`, partition.high);
          }

          break;
        } catch (error) {
          if (Date.now() > deadline) {
            // Best effort - without pins the transport falls back to resolve-at-join
            console.error(`KafkaMicroserviceTransport(${this.serviceName}): start-pin capture failed:`, error);

            return;
          }

          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }
    }
  }

  public async send<T = unknown>(pattern: string, data?: unknown, options?: SendOptions): Promise<T> {
    // Reject before any side effect: without this guard the send would mint a
    // junk `{prefix}.req.{topic}` topic and stall until timeout
    if (this.externalTopics.has(pattern)) {
      throw new UlakError(
        `Cannot send() to external topic "${pattern}" - external topics are event-only (no request/reply envelope), use emit()`,
        UlakErrorCode.SEND_FAILED,
      );
    }

    const correlationId = crypto.randomUUID();
    const timeout = options?.timeout ?? this.requestTimeout;
    const headers = buildRequestHeaders(
      pattern,
      crypto.randomUUID(),
      options?.headers,
      correlationId,
      this.naming.replyTopic,
      timeout,
    );

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingRequests.delete(correlationId);
        reject(new UlakError(`Request "${pattern}" timed out after ${timeout}ms`, UlakErrorCode.TIMEOUT));
      }, timeout);

      this.pendingRequests.set(correlationId, { resolve, reject, timer });

      this.ensureRequestTopic(pattern)
        .then(() =>
          this.producer!.send({
            topic: this.naming.requestTopic(pattern),
            messages: [{ value: JSON.stringify(data ?? null), headers }],
          }),
        )
        .catch((error) => {
          this.lastProbeOk = false;

          const pending = this.pendingRequests.get(correlationId);

          if (pending) {
            this.pendingRequests.delete(correlationId);
            clearTimeout(pending.timer);
            pending.reject(
              new UlakError(
                `Failed to publish request "${pattern}": ${(error as Error).message}`,
                UlakErrorCode.SEND_FAILED,
                undefined,
                error as Error,
              ),
            );
          }
        });
    });
  }

  public async emit(pattern: string, data?: unknown, options?: EmitOptions): Promise<void> {
    // External patterns publish RAW to the foreign topic: plain JSON value,
    // user headers verbatim, NO Asena envelope (p/mid/h/ts) - the consumer is
    // a foreign system expecting its own conventions. OTel's onSend hook
    // injects traceparent into options.headers, so trace continuity crosses
    // over as a plain header.
    const external = this.externalTopics.get(pattern);

    if (external) {
      const headers = options?.headers ?? {};
      const key = external.keyHeader !== undefined ? headers[external.keyHeader] : undefined;

      try {
        await this.producer!.send({
          topic: pattern,
          messages: [{ value: JSON.stringify(data ?? null), headers, ...(key !== undefined && { key }) }],
        });
      } catch (error) {
        this.lastProbeOk = false;
        throw error;
      }

      return;
    }

    const headers = buildEventHeaders(pattern, crypto.randomUUID(), options?.headers);

    try {
      await this.producer!.send({
        topic: this.naming.eventTopic,
        messages: [{ value: JSON.stringify(data ?? null), headers }],
      });
    } catch (error) {
      this.lastProbeOk = false;
      throw error;
    }
  }

  public destroy(options?: DestroyOptions): Promise<void> {
    // Idempotent: concurrent/repeated calls join the first teardown
    this.destroyPromise ??= this.doDestroy(options);

    return this.destroyPromise;
  }

  private async doDestroy(options?: DestroyOptions): Promise<void> {
    const drainTimeout = options?.drainTimeout ?? this.defaultDrainTimeout;

    // 1. Stop dispatching new records. The stop signal wakes the consumer
    //    recreation loop out of a reconnect backoff sleep.
    this.running = false;
    this.destroyed = true;
    this.stopResolve();

    if (this.healthTimer) {
      clearInterval(this.healthTimer);
      this.healthTimer = undefined;
    }

    for (const timer of this.retryTimers) {
      clearTimeout(timer);
    }

    this.retryTimers.clear();

    // 2. Drain in-flight handlers. Finished ones commit their offsets;
    //    unfinished ones stay uncommitted for another replica -
    //    at-least-once tolerates this (and R2 depends on it: a failed
    //    event's marker survives shutdown untouched).
    if (this.inFlight.size) {
      await Promise.race([
        Promise.allSettled([...this.inFlight]),
        new Promise((resolve) => setTimeout(resolve, drainTimeout)),
      ]);
    }

    // 3. Reject pending sends
    for (const [correlationId, pending] of this.pendingRequests) {
      clearTimeout(pending.timer);
      pending.reject(new UlakError('Transport destroyed', UlakErrorCode.SEND_FAILED));
      this.pendingRequests.delete(correlationId);
    }

    // 4. Graceful LeaveGroup matters: it triggers an immediate rebalance so
    //    surviving replicas take over without waiting out sessionTimeout
    //    (rolling deploys). Bounded higher than the other steps because a
    //    clean group exit is worth a few seconds.
    if (this.consumer) {
      await this.bounded(this.consumer.disconnect(), CONSUMER_DISCONNECT_TIMEOUT);
      this.consumer = undefined;
    }

    // 5. Ephemeral reply-group hygiene: disconnect (empties the group), then
    //    best-effort delete. Abandoned groups (SIGKILL) are reaped by the
    //    broker's offsets.retention instead.
    this.connected = false;

    if (this.replyConsumer) {
      await this.bounded(this.replyConsumer.disconnect());
      this.replyConsumer = undefined;
    }

    if (this.admin) {
      await this.bounded(this.admin.deleteGroups([this.naming.replyGroupId(this.serviceName, this.instanceId)]));
    }

    if (this.producer) {
      await this.bounded(this.producer.disconnect());
      this.producer = undefined;
    }

    if (this.admin) {
      await this.bounded(this.admin.disconnect());
      this.admin = undefined;
    }
  }

  /**
   * Best-effort await with an upper bound - teardown steps must never hang
   * shutdown behind a dead broker connection.
   */
  private async bounded(work: Promise<unknown>, timeoutMs: number = SHUTDOWN_STEP_TIMEOUT): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;

    await Promise.race([
      work.catch(() => {}),
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, timeoutMs);
      }),
    ]).finally(() => clearTimeout(timer));
  }

  // --- Consumers -----------------------------------------------------------

  /**
   * Consumer startup retry: kafkajs's consumer.run() awaits the initial
   * join, and a join inside the fresh-topic metadata window rejects with a
   * retriable UNKNOWN_TOPIC_OR_PARTITION / NOT_LEADER_FOR_PARTITION instead
   * of retrying. Each attempt builds a completely fresh consumer.
   */
  private async withStartupRetry(label: string, start: () => Promise<void>): Promise<void> {
    const deadline = Date.now() + STARTUP_RETRY_TIMEOUT;

    for (;;) {
      try {
        await start();

        return;
      } catch (error) {
        if (this.destroyed || Date.now() > deadline) throw error;

        console.error(
          `KafkaMicroserviceTransport(${this.serviceName}): ${label} startup failed, retrying:`,
          (error as Error).message,
        );

        await new Promise((resolve) => setTimeout(resolve, 250));
      }
    }
  }

  private async startReplyConsumer(): Promise<void> {
    const replyConsumer = this.adapter.consumer({
      groupId: this.naming.replyGroupId(this.serviceName, this.instanceId),
      sessionTimeout: this.sessionTimeout,
      heartbeatInterval: this.heartbeatInterval,
      rebalanceTimeout: this.rebalanceTimeout,
      // Short long-poll: init() waits for the first COMPLETED fetch (see
      // below), and a longer idle poll would stretch boot by that much
      maxWaitTimeInMs: REPLY_MAX_WAIT_TIME,
    });

    this.replyConsumer = replyConsumer;

    const joined = new Promise<void>((resolve) => replyConsumer.on(replyConsumer.events.GROUP_JOIN, () => resolve()));
    const fetched = new Promise<void>((resolve) => replyConsumer.on(replyConsumer.events.FETCH, () => resolve()));

    try {
      await replyConsumer.connect();
      await replyConsumer.subscribe({ topics: [this.naming.replyTopic], fromBeginning: false });

      await replyConsumer.run({
        autoCommit: true,
        eachMessage: async ({ message }) => this.handleReply(message.value),
      });
    } catch (error) {
      // Leave no half-started consumer behind for the retry attempt
      await this.bounded(replyConsumer.disconnect());
      this.replyConsumer = undefined;
      throw error;
    }

    // Wait until the group has joined AND completed one fetch: only then is
    // the "latest" position resolved, so a reply produced from now on cannot
    // land before the consumer's start offset. Bounded - a slow broker
    // degrades to possibly-missed early replies (caller times out and
    // retries) instead of a hung boot.
    let timer: ReturnType<typeof setTimeout> | undefined;

    const ready = await Promise.race([
      Promise.all([joined, fetched]).then(() => true),
      new Promise<false>((resolve) => {
        timer = setTimeout(() => resolve(false), REPLY_READY_TIMEOUT);
      }),
    ]).finally(() => clearTimeout(timer));

    if (!ready) {
      console.error(
        `KafkaMicroserviceTransport(${this.serviceName}): reply consumer not ready after ${REPLY_READY_TIMEOUT}ms - early sends may time out`,
      );
    }
  }

  private async startMainConsumer(topics: string[]): Promise<void> {
    const consumer = this.adapter.consumer({
      groupId: this.naming.groupId(this.serviceName),
      sessionTimeout: this.sessionTimeout,
      heartbeatInterval: this.heartbeatInterval,
      rebalanceTimeout: this.rebalanceTimeout,
      maxWaitTimeInMs: this.maxWaitTimeInMs,
    });

    this.consumer = consumer;

    // kafkajs emits GROUP_JOIN synchronously inside the join flow, before the
    // fetch loop starts - markerLoad is therefore always set before the first
    // eachMessage of the new generation awaits it.
    consumer.on(consumer.events.GROUP_JOIN, (event) => {
      this.assignment = new Map(Object.entries((event?.payload?.memberAssignment ?? {}) as Record<string, number[]>));
      this.markerLoad = this.loadMarkers();
    });

    consumer.on(consumer.events.CRASH, (event) => {
      this.lastProbeOk = false;

      // kafkajs restarts itself on retriable errors; only a non-restarting
      // crash needs a full recreate
      if (this.running && event?.payload?.restart === false) {
        void this.recreateConsumer(topics);
      }
    });

    const joined = new Promise<void>((resolve) => consumer.on(consumer.events.GROUP_JOIN, () => resolve()));
    const fetched = new Promise<void>((resolve) => consumer.on(consumer.events.FETCH, () => resolve()));

    const own = topics.filter((topic) => !this.externalTopics.has(topic));
    const external = topics.filter((topic) => this.externalTopics.has(topic));

    try {
      await consumer.connect();

      // fromBeginning:false = auto.offset.reset "latest": requests/events from
      // before a service's FIRST boot are invisible (R7a semantics). External
      // topics carry their own configured start position - kafkajs accepts
      // multiple subscribe calls before run()
      if (own.length) {
        await consumer.subscribe({ topics: own, fromBeginning: false });
      }

      if (external.length) {
        await consumer.subscribe({ topics: external, fromBeginning: this.externalFromBeginning });
      }

      await consumer.run({
        autoCommit: false,
        partitionsConsumedConcurrently: this.maxInFlight,
        eachMessage: (payload) => this.onMessage(payload),
      });
    } catch (error) {
      // Leave no half-started consumer behind for the retry attempt
      await this.bounded(consumer.disconnect());
      this.consumer = undefined;
      throw error;
    }

    // listen() must not return before the group has joined AND completed one
    // fetch: start positions ('latest') are resolved between those two
    // points, and only records produced AFTER resolution are visible. Without
    // this wait, a message sent right after listen() could silently miss the
    // consumer (the R7a first-boot semantic applied to our own boot). Bounded:
    // a struggling broker degrades to possibly-missed early messages instead
    // of a hung boot.
    let timer: ReturnType<typeof setTimeout> | undefined;

    const ready = await Promise.race([
      Promise.all([joined, fetched]).then(() => true),
      new Promise<false>((resolve) => {
        timer = setTimeout(() => resolve(false), LISTEN_READY_TIMEOUT);
      }),
    ]).finally(() => clearTimeout(timer));

    if (!ready) {
      console.error(
        `KafkaMicroserviceTransport(${this.serviceName}): consumer not ready after ${LISTEN_READY_TIMEOUT}ms - early messages may be missed`,
      );
    }
  }

  private async recreateConsumer(topics: string[]): Promise<void> {
    let backoff = RECONNECT_BACKOFF_START;

    while (this.running) {
      try {
        if (this.consumer) {
          await this.bounded(this.consumer.disconnect());
        }

        await this.startMainConsumer(topics);

        return;
      } catch (error) {
        console.error(`KafkaMicroserviceTransport(${this.serviceName}): consumer recreate failed:`, error);

        // Raced against the stop signal so destroy() never waits out a
        // capped (up to 30s) backoff sleep
        let timer: ReturnType<typeof setTimeout> | undefined;

        await Promise.race([
          new Promise<void>((resolve) => {
            timer = setTimeout(resolve, backoff);
          }),
          this.stopPromise,
        ]).finally(() => clearTimeout(timer));

        backoff = Math.min(backoff * 2, RECONNECT_BACKOFF_CAP);
      }
    }
  }

  // --- Dispatch ------------------------------------------------------------

  private async onMessage(payload: KafkaEachMessagePayload): Promise<void> {
    // Shutting down: leave the offset uncommitted, another replica redelivers
    if (!this.running) return;

    if (this.markerLoad) {
      await this.markerLoad;
    }

    const offset = Number(payload.message.offset);
    const marker = this.pendingAttempts.get(this.partitionKey(payload));
    const attempt = marker && marker.offset === offset ? marker.attempts + 1 : 1;

    // eachMessage is awaited by kafkajs per partition - processing stays
    // strictly sequential within a partition (what makes the scalar attempt
    // marker sound); the tracked promise also feeds the destroy() drain.
    const work = this.naming.isRequestTopic(payload.topic)
      ? this.dispatchRequest(payload, attempt)
      : this.dispatchEvent(payload, attempt);

    this.track(work);
    await work;
  }

  private track(work: Promise<void>): void {
    const tracked: Promise<void> = work.finally(() => {
      this.inFlight.delete(tracked);
    });

    this.inFlight.add(tracked);
  }

  private partitionKey(payload: KafkaEachMessagePayload): string {
    return `${payload.topic}:${payload.partition}`;
  }

  // --- Event path (at-least-once, seek-based retry + DLQ) ------------------

  private async dispatchEvent(payload: KafkaEachMessagePayload, attempt: number): Promise<void> {
    // External records dispatch under the TOPIC NAME with raw headers; the
    // whole retry/DLQ/no-handler machinery below is envelope-agnostic and
    // applies unchanged
    const context = this.externalTopics.has(payload.topic)
      ? buildExternalContext(payload, attempt)
      : buildContext(payload, attempt);
    const handlers = this.eventHandlers.collect(context.pattern);

    // No local handler for this pattern - commit immediately, nothing to do
    if (!handlers.length) {
      await this.commitNext(payload);

      return;
    }

    if (attempt > this.maxRetries) {
      try {
        await this.moveToDlq(payload, attempt);
        await this.commitNext(payload);
      } catch (error) {
        // DLQ produce failed (broker trouble) - keep the offset uncommitted
        // and retry the DLQ move on the next redelivery
        console.error(`KafkaMicroserviceTransport(${this.serviceName}): DLQ move failed:`, error);
        this.scheduleRetry(payload);
      }

      return;
    }

    await this.commitMarker(payload, attempt);

    const data = parsePayload(payload.message.value);

    try {
      await this.withHandlerTimeout(
        Promise.all(handlers.map((handler) => Promise.resolve(handler(data, context)))),
        context.pattern,
      );

      await this.commitNext(payload);
    } catch (error) {
      console.error(
        `KafkaMicroserviceTransport(${this.serviceName}): event handler failed for "${context.pattern}" (attempt ${attempt}):`,
        error,
      );

      // NO commit: pause the partition, seek back to the failed offset and
      // resume after the backoff - the broker genuinely re-fetches the
      // record, sharing one redelivery mechanism with crash recovery
      this.scheduleRetry(payload);
    }
  }

  private scheduleRetry(payload: KafkaEachMessagePayload): void {
    if (!this.running || !this.consumer) return;

    const { topic, partition } = payload;

    try {
      this.consumer.pause([{ topic, partitions: [partition] }]);
      this.consumer.seek({ topic, partition, offset: payload.message.offset });
    } catch {
      // Assignment revoked mid-rebalance - the new owner redelivers instead
      return;
    }

    const timer = setTimeout(() => {
      this.retryTimers.delete(timer);

      if (!this.running || !this.consumer) return;

      try {
        this.consumer.resume([{ topic, partitions: [partition] }]);
      } catch {
        // Assignment revoked while paused - nothing to resume
      }
    }, this.retryBackoffMs);

    this.retryTimers.add(timer);
  }

  private async moveToDlq(payload: KafkaEachMessagePayload, deliveryCount: number): Promise<void> {
    const raw = decodeHeaders(payload.message.headers);

    await this.producer!.send({
      topic: this.naming.dlqTopic,
      messages: [
        {
          value: payload.message.value,
          headers: {
            ...raw,
            origin_stream: payload.topic,
            // Bare serviceName (not the prefix-scoped group id): provenance
            // should read as the service, independent of namespace prefix
            origin_group: this.serviceName,
            origin_offset: `${payload.topic}:${payload.partition}:${payload.message.offset}`,
            delivery_count: String(deliveryCount),
            dlq_ts: String(Date.now()),
          },
        },
      ],
    });

    console.error(
      `KafkaMicroserviceTransport(${this.serviceName}): record ${payload.topic}:${payload.partition}:${payload.message.offset} ` +
        `(pattern "${raw['p'] ?? payload.topic}") moved to DLQ after ${deliveryCount} deliveries`,
    );
  }

  // --- Request path (RPC errors are final, no broker retry) ----------------

  private async dispatchRequest(payload: KafkaEachMessagePayload, attempt: number): Promise<void> {
    const context = buildContext(payload, attempt);
    const raw = decodeHeaders(payload.message.headers);
    const replyTopic = raw['r'];

    // The caller has already timed out - executing is wasted work (and after
    // a restart, a whole backlog of dead requests would otherwise run here)
    const callerTimeout = Number(raw['to']) || this.requestTimeout;
    const age = Date.now() - context.timestamp;

    if (age > callerTimeout) {
      await this.commitNext(payload);

      return;
    }

    await this.commitMarker(payload, attempt);

    const handler = this.messageHandlers.get(context.pattern);

    let reply: TransportReply;

    if (!handler) {
      // Topic exists but no local handler (should not happen - topics are derived from handlers)
      reply = {
        c: context.correlationId ?? '',
        ok: false,
        e: { name: 'UlakError', message: `No handler for pattern "${context.pattern}"` },
      };
    } else {
      try {
        const result = await this.withHandlerTimeout(
          Promise.resolve(handler(parsePayload(payload.message.value), context)),
          context.pattern,
        );

        reply = { c: context.correlationId ?? '', ok: true, d: result ?? null };
      } catch (error) {
        reply = {
          c: context.correlationId ?? '',
          ok: false,
          e: { name: (error as Error).name || 'Error', message: (error as Error).message },
        };
      }
    }

    if (replyTopic) {
      await this.producer!.send({ topic: replyTopic, messages: [{ value: JSON.stringify(reply) }] }).catch((error) => {
        console.error(`KafkaMicroserviceTransport(${this.serviceName}): failed to publish reply:`, error);
      });
    }

    // RPC is final either way - commit success AND error (no broker retry)
    await this.commitNext(payload);
  }

  private handleReply(value: Buffer | null): void {
    if (value === null) return;

    try {
      const reply: TransportReply = JSON.parse(value.toString());
      const pending = this.pendingRequests.get(reply.c);

      // Replies for other instances on the shared topic, and late/duplicate
      // replies, land here - ignore them
      if (!pending) return;

      this.pendingRequests.delete(reply.c);
      clearTimeout(pending.timer);

      if (reply.ok) {
        pending.resolve(reply.d);
      } else {
        pending.reject(
          new UlakError(`Remote handler failed: ${reply.e?.message ?? 'unknown error'}`, UlakErrorCode.REMOTE_ERROR),
        );
      }
    } catch (error) {
      console.error(`KafkaMicroserviceTransport(${this.serviceName}): failed to handle reply:`, error);
    }
  }

  // --- Attempt markers (offset-commit metadata protocol) -------------------

  /**
   * Pre-dispatch: commit the CURRENT offset (no advance) with the attempt
   * count as metadata. A crash after this point redelivers exactly this
   * record, and the successor reads the count from the committed metadata.
   */
  private async commitMarker(payload: KafkaEachMessagePayload, attempt: number): Promise<void> {
    this.pendingAttempts.set(this.partitionKey(payload), { offset: Number(payload.message.offset), attempts: attempt });

    try {
      await this.consumer!.commitOffsets([
        {
          topic: payload.topic,
          partition: payload.partition,
          offset: payload.message.offset,
          metadata: encodeMarker(attempt),
        },
      ]);
    } catch (error) {
      // REBALANCE_IN_PROGRESS and friends: at-least-once redelivery plus the
      // locally kept marker absorb a lost marker commit - never fatal
      console.error(`KafkaMicroserviceTransport(${this.serviceName}): marker commit failed:`, error);
    }
  }

  /** Post-dispatch: advance to offset+1 with empty metadata, clearing the marker. */
  private async commitNext(payload: KafkaEachMessagePayload): Promise<void> {
    this.pendingAttempts.delete(this.partitionKey(payload));

    try {
      await this.consumer!.commitOffsets([
        {
          topic: payload.topic,
          partition: payload.partition,
          offset: String(Number(payload.message.offset) + 1),
          metadata: null,
        },
      ]);
    } catch (error) {
      // Lost advance = the record is redelivered after rebalance and runs
      // again (at-least-once); handler idempotency via messageId absorbs it
      console.error(`KafkaMicroserviceTransport(${this.serviceName}): offset commit failed:`, error);
    }
  }

  /**
   * Loads attempt markers from committed offset metadata - runs on every
   * GROUP_JOIN so a rebalance survivor derives attempt = marker + 1 for the
   * offset it takes over. Merges with locally kept markers: a marker whose
   * broker commit was lost to a rebalance still counts.
   */
  private async loadMarkers(): Promise<void> {
    try {
      let timer: ReturnType<typeof setTimeout> | undefined;

      const fetched = await Promise.race([
        this.admin!.fetchOffsets({ groupId: this.naming.groupId(this.serviceName), topics: this.consumedTopics() }),
        new Promise<null>((resolve) => {
          timer = setTimeout(() => resolve(null), MARKER_LOAD_TIMEOUT);
        }),
      ]).finally(() => clearTimeout(timer));

      if (!fetched) {
        console.error(
          `KafkaMicroserviceTransport(${this.serviceName}): marker load timed out - attempts degrade to local knowledge`,
        );

        return;
      }

      const pins: { topic: string; partition: number; offset: string; metadata: null }[] = [];

      for (const topicOffsets of fetched) {
        const assigned = new Set(this.assignment.get(topicOffsets.topic) ?? []);

        for (const partition of topicOffsets.partitions) {
          const key = `${topicOffsets.topic}:${partition.partition}`;
          const offset = Number(partition.offset);
          const attempts = decodeMarker(partition.metadata);
          const local = this.pendingAttempts.get(key);

          if (attempts !== null) {
            const merged = local && local.offset === offset ? Math.max(local.attempts, attempts) : attempts;

            this.pendingAttempts.set(key, { offset, attempts: merged });
          } else if (local && offset > local.offset) {
            // The group advanced past our stale local marker
            this.pendingAttempts.delete(key);
          }

          // Start-position pinning: an assigned partition with no committed
          // offset gets the pre-run 'latest' snapshot committed, so a later
          // crash-restart resumes there instead of skipping to latest-at-restart
          if (offset < 0 && assigned.has(partition.partition)) {
            const pin = this.startPins.get(key);

            if (pin !== undefined) {
              pins.push({ topic: topicOffsets.topic, partition: partition.partition, offset: pin, metadata: null });
            }
          }
        }
      }

      if (pins.length && this.consumer) {
        await this.consumer.commitOffsets(pins).catch((error) => {
          console.error(`KafkaMicroserviceTransport(${this.serviceName}): start-pin commit failed:`, error);
        });
      }
    } catch (error) {
      // Degraded (attempt may read 1 after a crash) but never fatal
      console.error(`KafkaMicroserviceTransport(${this.serviceName}): marker load failed:`, error);
    }
  }

  // --- Health --------------------------------------------------------------

  /**
   * Active broker probe: kafkajs does not reliably surface socket-level
   * failures as events, so isConnected is driven by a bounded metadata fetch.
   * kafkajs's internal retries would delay a plain await far beyond the probe
   * interval - the race makes degradation deterministic.
   */
  private async probeHealth(): Promise<void> {
    if (this.probing || this.destroyed) return;

    this.probing = true;

    try {
      const probe = this.admin!.fetchTopicMetadata({ topics: [this.naming.eventTopic] });

      probe.catch(() => {});

      let timer: ReturnType<typeof setTimeout> | undefined;

      const ok = await Promise.race([
        probe.then(() => true),
        new Promise<false>((resolve) => {
          timer = setTimeout(() => resolve(false), Math.max(1_000, this.healthCheckIntervalMs));
        }),
      ]).finally(() => clearTimeout(timer));

      this.lastProbeOk = ok;
    } catch {
      this.lastProbeOk = false;
    } finally {
      this.probing = false;
    }
  }

  // --- Helpers -------------------------------------------------------------

  private async ensureTopics(topics: KafkaTopicConfig[]): Promise<void> {
    const missing = topics.filter((topic) => !this.ensuredTopics.has(topic.topic));

    if (!missing.length) return;

    const createDeadline = Date.now() + TOPIC_VISIBILITY_TIMEOUT;

    // kafkajs swallows TOPIC_ALREADY_EXISTS (returns false) - idempotent.
    // Retried because the admin's full-metadata refresh inside createTopics
    // THROWS when any unrelated topic on the cluster is mid-deletion
    // (per-topic UNKNOWN_TOPIC_OR_PARTITION fails the whole metadata parse).
    for (;;) {
      try {
        await this.admin!.createTopics({
          waitForLeaders: true,
          topics: missing.map((topic) => ({ ...topic, replicationFactor: this.replicationFactor })),
        });
        break;
      } catch (error) {
        if (this.destroyed || Date.now() > createDeadline) throw error;

        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    // createTopics returning (even with waitForLeaders) does not mean the
    // partition leaders actually serve clients yet - touching the topic
    // inside that window gets UNKNOWN_TOPIC_OR_PARTITION or
    // NOT_LEADER_FOR_PARTITION, and a consumer crash-restart in that state
    // re-resolves its start position PAST records produced meanwhile. Wait
    // until metadata shows an elected leader for every partition (an
    // error-free check: hammering calls that THROW inside the window makes
    // kafkajs spray internal unhandled rejections), then confirm with one
    // ListOffsets - the exact call the fetch path depends on.
    const deadline = Date.now() + TOPIC_VISIBILITY_TIMEOUT;
    const names = missing.map((topic) => topic.topic);

    for (;;) {
      const metadata = (await this.admin!.fetchTopicMetadata({ topics: names }).catch(() => null)) as {
        topics: { name: string; partitions: { leader: number }[] }[];
      } | null;

      const allLed = metadata?.topics.every(
        (topic) => topic.partitions.length > 0 && topic.partitions.every((partition) => partition.leader >= 0),
      );

      if (allLed) break;

      if (Date.now() > deadline) {
        throw new Error(`Topics [${names.join(', ')}] have no elected leaders after ${TOPIC_VISIBILITY_TIMEOUT}ms`);
      }

      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    for (const topic of missing) {
      for (;;) {
        try {
          await this.admin!.fetchTopicOffsets(topic.topic);
          break;
        } catch (error) {
          if (Date.now() > deadline) throw error;

          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }
    }

    for (const topic of missing) {
      this.ensuredTopics.add(topic.topic);
    }
  }

  /**
   * Bounded wait for foreign-owned topics to be visible and served (leader
   * poll + a ListOffsets confirm, the same idiom ensureTopics uses after
   * creating) - the transport NEVER creates external topics.
   */
  private async awaitExternalTopics(topics: string[]): Promise<void> {
    if (!topics.length) return;

    const deadline = Date.now() + TOPIC_VISIBILITY_TIMEOUT;
    const unavailable = (cause: unknown) =>
      new Error(
        `External topic(s) [${topics.join(', ')}] are not available after ${TOPIC_VISIBILITY_TIMEOUT}ms - ` +
          'external topics are foreign-owned and the transport never creates them; ' +
          `make sure the owning system created them${cause ? ` (last error: ${(cause as Error).message})` : ''}`,
      );

    for (;;) {
      const metadata = (await this.admin!.fetchTopicMetadata({ topics }).catch(() => null)) as {
        topics: { name: string; partitions: { leader: number }[] }[];
      } | null;

      const allLed =
        metadata !== null &&
        metadata.topics.length === topics.length &&
        metadata.topics.every(
          (topic) => topic.partitions.length > 0 && topic.partitions.every((partition) => partition.leader >= 0),
        );

      if (allLed) break;

      if (Date.now() > deadline) throw unavailable(null);

      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    for (const topic of topics) {
      for (;;) {
        try {
          await this.admin!.fetchTopicOffsets(topic);
          break;
        } catch (error) {
          if (Date.now() > deadline) throw unavailable(error);

          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }
    }
  }

  /** send() to a pattern nobody has sent to yet creates its topic lazily - once per process. */
  private async ensureRequestTopic(pattern: string): Promise<void> {
    await this.ensureTopics([{ topic: this.naming.requestTopic(pattern), numPartitions: this.requestPartitions }]);
  }

  /**
   * Topics this instance consumes: the shared event topic (only when event
   * handlers exist), one request topic per registered message pattern, plus
   * every external topic some event handler matches. collect() is the
   * subscription criterion on purpose: a wildcard handler like `upstream.*`
   * pulls the external topic `upstream.order-created` in too. External topics
   * in config with no matching handler are outbound-only - never subscribed,
   * never an error (the config is bidirectional).
   */
  private consumedTopics(): string[] {
    const topics: string[] = [];

    if (!this.eventHandlers.isEmpty) {
      topics.push(this.naming.eventTopic);
    }

    for (const pattern of this.messageHandlers.keys()) {
      topics.push(this.naming.requestTopic(pattern));
    }

    for (const name of this.externalTopics.keys()) {
      if (this.eventHandlers.collect(name).length > 0) {
        topics.push(name);
      }
    }

    return topics;
  }

  private withHandlerTimeout<T>(promise: Promise<T>, pattern: string): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(
          new UlakError(
            `Handler for "${pattern}" exceeded handlerTimeout (${this.handlerTimeout}ms)`,
            UlakErrorCode.TIMEOUT,
          ),
        );
      }, this.handlerTimeout);

      promise
        .then((value) => {
          clearTimeout(timer);
          resolve(value);
        })
        .catch((error) => {
          clearTimeout(timer);
          reject(error);
        });
    });
  }

  private isKafkaService(source: AsenaKafkaService | KafkaConfig): source is AsenaKafkaService {
    return typeof (source as AsenaKafkaService).createConsumer === 'function';
  }
}
