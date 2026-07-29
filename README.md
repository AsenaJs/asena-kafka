<p width="%100" align="center">
  <img src="https://avatars.githubusercontent.com/u/179836938?s=200&v=4" width="150" align="center"/>
</p>

# @asenajs/asena-kafka

[![Version](https://img.shields.io/badge/version-3.0.0-blue.svg)](https://github.com/AsenaJs/asena-kafka)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Bun Version](https://img.shields.io/badge/Bun-1.3.12%2B-blueviolet)](https://bun.sh)

Kafka integration for AsenaJS — service client and microservice transport.

`KafkaMicroserviceTransport` plugs Kafka into Asena's microservice layer (`@MessageController`, `@MessagePattern`, `@EventPattern`, RPC over `ulak`), and the `@Kafka` decorated service gives you raw produce/consume access with automatic IoC registration.

## Features

- **Microservice Transport** - Full `MicroserviceTransport` implementation: RPC (request/reply), event fan-out with wildcards, retries, DLQ
- **Broker-Tracked Delivery Attempts** - `context.attempt` survives crashes and rebalances (persisted in offset-commit metadata, no side store)
- **Decorator-Based Setup** - `@Kafka` decorator handles IoC registration and connection lifecycle
- **Adapter Seam** - kafkajs today behind a `KafkaClientAdapter` interface (see [Client Roadmap](#client-roadmap))
- **Deterministic Topic Management** - The transport creates its topics explicitly and waits for partition leaders; broker auto-create is never relied on
- **External-Topic Interop** - Consume from and emit to envelope-less foreign topics (Quarkus/SmallRye, CDC, plain Kafka clients) with raw headers and traceparent continuity
- **Zero Runtime Dependencies** - Only peer deps (asena, kafkajs, reflect-metadata)

## Requirements

- [Bun](https://bun.sh) v1.3.12 or higher
- [@asenajs/asena](https://github.com/AsenaJs/Asena) v0.10.0 or higher
- [kafkajs](https://kafka.js.org) v2.2.4 (peer dependency)
- Apache Kafka **2.8 – 3.9**. Kafka 4.0 removed old protocol API versions (KIP-896) and kafkajs 2.2.4 has reported incompatibilities — pin your broker to 3.9.x. See [Client Roadmap](#client-roadmap).

## Installation

```bash
bun add @asenajs/asena-kafka kafkajs
```

### Local development broker

A single-node KRaft container is all you need:

```bash
docker run -d --name asena-kafka --restart always -p 9092:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS=1000 \
  apache/kafka:3.9.1
```

`KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS=1000` lets tests use low `sessionTimeout` values for fast crash detection; production brokers can keep the default.

## Quick Start

### Microservice Transport

```typescript
import { Config, MessageController } from '@asenajs/asena/decorators';
import { MessagePattern, EventPattern } from '@asenajs/asena/microservice';
import { KafkaMicroserviceTransport } from '@asenajs/asena-kafka';

@Config()
export class ServerConfig {
  transport() {
    return {
      microservice: new KafkaMicroserviceTransport(
        { brokers: ['localhost:9092'] },
        { serviceName: 'order-service' },
      ),
    };
  }
}

@MessageController('order')
export class OrderController {
  @MessagePattern('create') // answers RPC 'order.create'
  async create(data: CreateOrderDto, context: MessageContext) {
    return { id: 42, ...data };
  }

  @EventPattern('*') // wildcard event subscription - handles 'order.*'
  async onOrderEvent(data: any, context: MessageContext) {
    // context.messageId is the dedup key, context.attempt > 1 marks redelivery
  }

  // Another service's vocabulary - opt out of the 'order' prefix
  @EventPattern({ pattern: 'payment.*', prefix: false })
  async onPayment(data: any, context: MessageContext) {}
}
```

Any service can then call `ulak.messages('order').send('create', dto)` or `emit(...)` — see the [Microservices concepts doc](https://asena.sh/docs/concepts/microservices) for the full picture (headless mode, named transports, interceptors, OTel tracing).

### Service Client

```typescript
import { Kafka, AsenaKafkaService } from '@asenajs/asena-kafka';

@Kafka({
  config: { brokers: ['localhost:9092'], clientId: 'my-app' },
  name: 'AppKafka',
})
export class AppKafka extends AsenaKafkaService {

  async publishAudit(entry: AuditEntry) {
    await this.sendMessage('audit.log', [{ value: JSON.stringify(entry) }]);
  }
}
```

`AsenaKafkaService` exposes `sendMessage(topic, messages)`, `createProducer()`, `createConsumer(config)`, `createAdmin()`, `client`, `disconnect()` and `testConnection()`. The transport can borrow a decorated service too: `new KafkaMicroserviceTransport(appKafka, { serviceName })` — it still creates its own producer/consumers, your client is never touched.

The service connects on `server.start()` and disconnects itself on `server.stop()`. Objects from `createProducer()` / `createConsumer(config)` / `createAdmin()` stay yours: close them from an `@OnStop()` on the component that created them, which runs while the service is still up.

## Delivery Model

| Concern | Kafka shape |
|---|---|
| Events | Single shared topic `{prefix}.evt` (keyless round-robin over `eventPartitions`), one consumer group per service. Wildcards are matched locally; non-matching records are committed immediately. |
| Requests (RPC) | One topic per exact pattern `{prefix}.req.{pattern}`, consumer group per responding service. RPC errors are FINAL — the caller gets the error, the offset is committed, no broker retry. |
| Replies | One shared topic `{prefix}.reply`; every caller instance runs an ephemeral consumer group and filters by correlationId. |
| DLQ | `{prefix}.dlq` with provenance headers (`origin_group`, `origin_stream`, `origin_offset`, `delivery_count`, `dlq_ts`). |

### Attempt tracking (broker-persisted)

Kafka has no per-message delivery counter, so the transport persists one in **offset-commit metadata**: before dispatching the record at offset X it commits offset X with `{"a":attempt}`; on success it commits X+1 clean. A crash therefore redelivers exactly that record, and the successor (loaded on every group join) derives `attempt = a + 1` — `context.attempt` is trustworthy across processes with no extra infrastructure.

The cost is two synchronous commits per record per partition. Processing inside a partition is strictly sequential (that is what makes the marker sound); throughput scales by adding partitions (`maxInFlight` maps to kafkajs `partitionsConsumedConcurrently`).

### Delivery guarantees

- Events are **at-least-once**: a failed handler leaves the offset uncommitted; the partition is paused, seeked back and re-fetched after `retryBackoffMs`, up to `maxRetries`, then the record moves to the DLQ. Make handlers idempotent — dedup on `context.messageId` (one id per emit, identical for every group and every redelivery).
- Requests older than the caller's own timeout (envelope header `to`) are **dropped without execution** — a restarting service never burns through a backlog of dead RPCs.
- On its very first boot a consumer group starts at **latest**: messages sent before a service ever existed are invisible (deploy the consumer before the producer). The start position is pinned to a committed offset immediately, so later crashes can never skip records.

## External Topics (interop)

Everything above rides Asena's own envelope on Asena-owned topics. The `external` option is the escape hatch for **foreign systems** — a Quarkus/SmallRye service, a CDC pipeline, any plain Kafka producer/consumer — whose topics carry no envelope at all:

```typescript
const transport = new KafkaMicroserviceTransport(
  { brokers: ['localhost:9092'] },
  {
    serviceName: 'billing-service',
    external: {
      topics: [
        'orders',                                    // string shorthand
        { name: 'invoices', keyHeader: 'x-tenant' }, // outbound partition affinity
      ],
      fromBeginning: false, // start position on FIRST subscribe (default: latest)
    },
  },
);
```

Controllers stay completely transport-agnostic — external topics surface through the same decorators:

```typescript
@MessageController()
export class OrdersListener {
  // Pattern = the external TOPIC NAME. Segment wildcards match it like any
  // event: an 'upstream.*' handler pulls an 'upstream.orders' external topic
  @EventPattern({ pattern: 'orders', prefix: false })
  async onOrder(order: OrderPayload, context: MessageContext) {
    // context.headers = ALL raw record headers - a Quarkus producer's
    // traceparent / ce-* headers arrive verbatim (OTel picks traceparent up
    // automatically, so the foreign trace continues through your handler)
    // context.messageId = 'mid' header if present, else 'topic:partition:offset'
  }
}
```

**Inbound rules** (topics with a matching `@EventPattern`):

- The dispatch pattern is **always the topic name** — a foreign record's incidental `p` header never steers routing.
- The handler must resolve to exactly the topic name. The `@MessageController` prefix is joined onto `@EventPattern` too, so on a prefixed controller you **must** pass `prefix: false` — otherwise the handler registers as `billing.orders`, the topic is never consumed, and the service still boots green. `listen()` prints a hint naming the shadowed handler.
- `context.headers` exposes **all raw record headers**; `context.messageId` honors a `mid` header, otherwise it falls back to the record position `topic:partition:offset`, which is stable across groups and redeliveries so messageId-dedup still works. Caveat: a replay *tool* that re-produces a DLQ'd record creates a new position and therefore a new id.
- Retry, DLQ (with `origin_stream` = the foreign topic, value + headers preserved) and `context.attempt` work exactly as for own topics.

**Outbound rules** (`emit('topicName')` where the name is configured external):

- The value is plain JSON of your payload, headers are your `options.headers` **verbatim** — no `p`/`mid`/`h`/`ts` envelope keys. If OTel messaging instrumentation is active, its injected `traceparent` goes over as a plain header and the foreign consumer resumes the trace.
- With `keyHeader` set, that header's value becomes the record **key** (partition affinity on the foreign topic); the header itself is still sent.
- External topics are **event-only**: `send()` to an external name rejects immediately with `SEND_FAILED` (no request/reply without an envelope).

**Boot behavior**: the transport **never creates external topics** — they are foreign property. Subscribed external topics are awaited (bounded) at `listen()` and missing ones fail the boot loudly; configured topics with no matching handler are outbound-only, logged and never an error. `fromBeginning: true` reads each external topic from the earliest retained record on the group's first subscribe (start-position pinning is skipped for those topics so the semantic actually holds).

> Note for rolling deploys: changing the `external` list changes the group's subscription. Restart all replicas together for a deterministic assignment instead of mixing generations.

## Configuration

### KafkaConfig

Passed through to the kafkajs `Kafka` constructor: `brokers` (required), `clientId`, `ssl`, `sasl`, `connectionTimeout`, `requestTimeout`, `retry`, `logLevel` — plus Asena's display `name`.

### KafkaMicroserviceOptions

| Option | Default | Description |
|---|---|---|
| `serviceName` | **required** | Consumer group identity, shared by all replicas (Kafka group id is `{topicPrefix}.{serviceName}`) |
| `topicPrefix` | `'asena.ms'` | Topic/group namespace, `[a-zA-Z0-9._-]` only |
| `requestTimeout` | `30000` | Default reply timeout for `send()` |
| `maxRetries` | `3` | Event delivery attempts before DLQ (RPC never retried) |
| `retryBackoffMs` | `5000` | Pause before the failed partition is resumed for the retry fetch |
| `handlerTimeout` | `min(30000, sessionTimeout)` | Per-handler timeout; values above `sessionTimeout` throw at construction |
| `maxInFlight` | `16` | Partitions processed concurrently |
| `drainTimeout` | `10000` | Graceful drain window for `destroy()` |
| `sessionTimeout` | `30000` | Consumer group session timeout (crash detection speed) |
| `heartbeatInterval` | `3000` | Consumer heartbeat (keep ≤ 1/3 of sessionTimeout) |
| `rebalanceTimeout` | `60000` | Max rebalance duration |
| `maxWaitTimeInMs` | `1000` | Idle fetch long-poll (boot readiness / shutdown responsiveness) |
| `eventPartitions` / `requestPartitions` / `replyPartitions` | `4` | Partition counts for transport-created topics |
| `replicationFactor` | `-1` | Broker default |
| `healthCheckIntervalMs` | `5000` | Interval of the active broker probe — one of the two inputs to `isConnected` (see Operational Notes) |
| `external` | — | Foreign-topic interop: `{ topics: (string \| { name, keyHeader? })[], fromBeginning? }` — see [External Topics](#external-topics-interop) |

## Operational Notes

- **Handler duration must stay below `sessionTimeout`** — kafkajs cannot heartbeat while a handler runs; a longer handler gets the member evicted and the record concurrently redelivered elsewhere.
- **`isConnected` means "can serve", not "a broker answers"** — it requires both a passing metadata probe and a reply consumer that has rejoined and is fetching. After a broker outage the probe recovers in milliseconds while the ephemeral reply group can still be rejoining ~20 seconds later, and until it fetches nothing consumes replies, so every `send()` would time out. Expect an instance to stay 503 for the length of that rejoin, and keep the liveness probe more forgiving than the readiness probe.
- **Rolling deploys**: kafkajs uses eager rebalancing — every membership change briefly pauses the whole group. Graceful shutdown (`destroy()`) leaves the group cleanly so the pause is short; SIGKILL costs a full `sessionTimeout`.
- **Ordering**: with the default multi-partition event topic, cross-partition ordering is not preserved. Set `eventPartitions: 1` if you need strict publish order (at the cost of parallelism).
- Under Bun you may see a cosmetic `TimeoutNegativeWarning` from kafkajs's request queue — Bun warns where Node silently clamps negative timers to 1ms; behavior is identical.

## Client Roadmap

**Why kafkajs?** It is the only full-featured Kafka client that actually runs on Bun today. It is also effectively unmaintained (2.2.4 is the final release), which is why every kafkajs call sits behind the `KafkaClientAdapter` interface.

**Why not `@confluentinc/kafka-javascript`?** Confluent's official client ships a KafkaJS-compatibility mode and looks like the natural successor. It was tested directly on Bun 1.3.14 and does not work, for two independent reasons:

1. **It cannot load on Bun.** The addon is built on NAN (the V8 C++ API), not N-API. Bun's ABI (`NODE_MODULE_VERSION` 137) matches a published prebuilt, and that prebuilt downloads and installs cleanly — so this is neither an install-script nor an ABI problem. Loading still dies inside `dlopen`:

   ```
   bun: symbol lookup error: .../confluent-kafka-javascript.node:
        undefined symbol: _ZN2v816FunctionTemplate12SetClassNameENS_5LocalINS_6StringEEE
   ```

   That is `v8::FunctionTemplate::SetClassName`, which JavaScriptCore does not provide. Tracking: [bun#4290](https://github.com/oven-sh/bun/issues/4290), [confluent-kafka-javascript#264](https://github.com/confluentinc/confluent-kafka-javascript/issues/264).

2. **Even on Node it would cost a feature.** Confluent's KafkaJS mode does not support sending metadata with offset commits, and the broker-tracked `context.attempt` protocol is built entirely on offset-commit metadata. `consumer.stop()` and the consumer instrumentation events used for ready-gating and crash detection are also unsupported.

**What would change this:** [confluent-kafka-javascript#471](https://github.com/confluentinc/confluent-kafka-javascript/pull/471) migrates the addon from NAN to N-API. If it lands, blocker 1 disappears; blocker 2 would still need upstream work.

## Contributing

Contributions are welcome! Please open an issue or a pull request on [GitHub](https://github.com/AsenaJs/asena-kafka).

## License

MIT — see [LICENSE](./LICENSE).

## Support

- Documentation: [asena.sh](https://asena.sh)
- Issues: [GitHub Issues](https://github.com/AsenaJs/asena-kafka/issues)