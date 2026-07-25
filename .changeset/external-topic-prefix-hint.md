---
'@asenajs/asena-kafka': patch
---

Clearer diagnostics for external topics that end up outbound-only:

- `listen()` now logs one line per outbound-only external topic instead of one aggregated line, and names a likely mis-prefixed handler when it finds one. Since Asena 0.8 joins the `@MessageController` prefix onto `@EventPattern` as well, a handler written for the foreign topic `orders` on a prefixed controller registers as `billing.orders` and the topic is silently never consumed — the hint points straight at the fix (`prefix: false`).
- Message and event pattern validation errors mention the `prefix: false` escape hatch.

Requires `@asenajs/asena` ≥ 0.8.0 for `PatternHandlerIndex.patterns()`.
