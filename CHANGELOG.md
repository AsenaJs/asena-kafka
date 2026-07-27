# @asenajs/asena-kafka

## 2.0.0

### Major Changes

- `@Kafka` keeps everything the decorated class inherited

  The decorator replaces the class it decorates with a wrapper. The wrapper extended
  `AsenaKafkaService` rather than the target, and the copy loops beside it only walked the target's
  _own_ prototype — so every method, getter and static the class inherited from an intermediate
  base class was dropped, `instanceof` against that base was false, and nothing failed until the
  first call.

  The wrapper now extends the target. The member and metadata copy loops are removed: everything is
  reachable through the prototype chain, and the metadata loop was actively harmful — it flattened
  an inherited `NameKey` onto the wrapper, so a `@Kafka` class extending another `@Kafka` class
  registered under its parent's name and the container promoted the entry to an array.

  The type parameter is now constrained to `AsenaKafkaService`, matching `@Database`. Decorating a
  class that does not extend it used to work by accident.

  Requires `@asenajs/asena` 0.9.0 or later: the wrapper registers under the target's own name, and
  older versions reported that as a circular dependency.

- An RPC reply is no longer lost after a broker outage, and readiness stops claiming otherwise

  Two independent defects made a caller lose a reply permanently while its health endpoint
  returned 200. Measured over 12 broker restarts with a client-only caller (an HTTP gateway
  that only `send()`s) and a responder: 4 round-trips never completed at all and the other 8
  took 7.9–15.8 s, every one of them issued the instant `/healthz` reported healthy.

  **The reply consumer lost its position on rejoin.** The ephemeral reply group subscribes with
  `fromBeginning: false`, and kafkajs never commits an offset for a partition that delivered
  nothing — so on a caller that has served no reply on a partition yet, the group holds no
  committed offset for it and every rejoin re-resolves `latest`. A reply appended while the
  consumer was away was skipped forever: the responder produced it, the topic held it, the group
  was `Stable members=1`, and the caller still timed out. The reply consumer's start position is
  now pinned as a committed offset, the same mechanism the main consumer already used for its own
  topics. The watermark is captured once and never re-captured on a recreate — re-capturing would
  pin past the very reply the outage put at risk. A reply produced during a rejoin is now delivered
  late instead of skipped.

  **Readiness was blind to the reply consumer.** `isConnected` was `connected && lastProbeOk`,
  and `lastProbeOk` came only from a bounded admin metadata fetch, which says nothing about whether
  replies are being consumed. Measured across 12 broker restarts, the probe went green 0.45–0.95 s
  after the broker served again, while the reply consumer's fetch loop stayed stalled for ~21 s
  before its next fetch; every `send()` in that window timed out behind a green endpoint.

  **This changes the documented `healthCheckIntervalMs` contract.** A failed probe still flips
  `isConnected` to false, but it is no longer the only thing that does: `isConnected` now also
  requires the reply consumer to have rejoined its group and to be fetching. A stalled fetch loop
  degrades readiness after 5 s — well before kafkajs raises `CRASH` at ~7.5 s — and the
  `CRASH`/`DISCONNECT`/`STOP` events reset it on top of that. The visible consequence is that an
  instance now reports 503 for as long as its reply consumer takes to rejoin after an outage,
  where it previously reported 200 and failed every request. **Liveness probes must be more
  forgiving than readiness probes**, or an orchestrator will restart instances that were about to
  recover. Both halves hold for a client-only instance, which never sets `running` and depends on
  the reply consumer most.

  Re-running the same 12 broker restarts after the fix: 12 completed, 0 lost. Six finished in under
  32 ms (readiness had waited out the rejoin, so the round-trip was immediate) and six in 3.9–8.5 s.
  The reply group carried a committed offset on every partition in every run, where before three of
  four partitions sat at `-1`.

## 1.0.0

### Major Changes

- 001562d: Initial release of the official Asena Kafka package:

  - `KafkaMicroserviceTransport` — full `MicroserviceTransport` implementation over kafkajs: RPC (request/reply via a shared reply topic + correlation ids), event fan-out with local wildcard matching, seek-based retries with DLQ provenance, and broker-tracked delivery attempts persisted in offset-commit metadata (`context.attempt` survives crashes and rebalances with no side store).
  - Deterministic topic management: explicit creation with partition-leader visibility polling, start-position pinning (a consumer crash-restart can never silently skip records), consumer startup retry against fresh-topic metadata races.
  - `AsenaKafkaService` + `@Kafka` decorator — produce/consume service client with automatic IoC registration, mirroring `@asenajs/asena-redis`.
  - `KafkajsAdapter` behind the `KafkaClientAdapter` seam — kafkajs-compatible clients can be slotted in later.
  - Active broker health probe feeding Asena's health endpoint (`isConnected` → 503 on outage).
  - External-topic interop (`external` option) — consume from and emit to envelope-less foreign topics (Quarkus/SmallRye, CDC pipelines, plain Kafka clients): inbound records dispatch under the topic name with all raw headers exposed (traceparent continuity for OTel), outbound emits publish plain JSON with verbatim headers and optional `keyHeader` partition affinity. Event-only, never created by the transport, with explicit boot-time errors for missing subscribed topics and `fromBeginning` control over the first-subscribe start position.

### Patch Changes

- 001562d: Clearer diagnostics for external topics that end up outbound-only:

  - `listen()` now logs one line per outbound-only external topic instead of one aggregated line, and names a likely mis-prefixed handler when it finds one. Since Asena 0.8 joins the `@MessageController` prefix onto `@EventPattern` as well, a handler written for the foreign topic `orders` on a prefixed controller registers as `billing.orders` and the topic is silently never consumed — the hint points straight at the fix (`prefix: false`).
  - Message and event pattern validation errors mention the `prefix: false` escape hatch.

  Requires `@asenajs/asena` ≥ 0.8.0 for `PatternHandlerIndex.patterns()`.
