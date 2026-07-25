---
'@asenajs/asena-kafka': major
---

Initial release of the official Asena Kafka package:

- `KafkaMicroserviceTransport` — full `MicroserviceTransport` implementation over kafkajs: RPC (request/reply via a shared reply topic + correlation ids), event fan-out with local wildcard matching, seek-based retries with DLQ provenance, and broker-tracked delivery attempts persisted in offset-commit metadata (`context.attempt` survives crashes and rebalances with no side store).
- Deterministic topic management: explicit creation with partition-leader visibility polling, start-position pinning (a consumer crash-restart can never silently skip records), consumer startup retry against fresh-topic metadata races.
- `AsenaKafkaService` + `@Kafka` decorator — produce/consume service client with automatic IoC registration, mirroring `@asenajs/asena-redis`.
- `KafkajsAdapter` behind the `KafkaClientAdapter` seam — kafkajs-compatible clients can be slotted in later.
- Active broker health probe feeding Asena's health endpoint (`isConnected` → 503 on outage).
- External-topic interop (`external` option) — consume from and emit to envelope-less foreign topics (Quarkus/SmallRye, CDC pipelines, plain Kafka clients): inbound records dispatch under the topic name with all raw headers exposed (traceparent continuity for OTel), outbound emits publish plain JSON with verbatim headers and optional `keyHeader` partition affinity. Event-only, never created by the transport, with explicit boot-time errors for missing subscribed topics and `fromBeginning` control over the first-subscribe start position.