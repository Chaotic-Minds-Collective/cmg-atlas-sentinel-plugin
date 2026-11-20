# Sentinel Core Event Consumer

Sentinel can explicitly claim Core outbox events for its existing read-only alert rules. The default exact-match filter is derived from `SENTINEL_DEFAULT_RULES`: `player.died`, `player.killed`, `ship.sunk`, `structure.destroyed`, and `tame.died`. Other event families remain available to their own consumers.

```js
import { createSentinelCoreEventConsumer } from "../src/core-event-consumer.js";
const consumer = createSentinelCoreEventConsumer({
  baseUrl: process.env.CMG_CORE_ORIGIN,
  serviceToken: process.env.CMG_CORE_SERVICE_TOKEN,
  serverId: process.env.CMG_CORE_SERVER_ID,
  resolveScope: (event) => ({
    clusterId: event.clusterId,
    gridId: event.gridId ?? configuredGridByServer[event.serverId],
    serverId: event.serverId
  })
});
const claimed = await consumer.claim({ limit: 1, leaseSeconds: 120 });
for (const item of claimed) {
  // item.alerts contains metadata-only Sentinel alerts.
  // Persist/deliver idempotently using item.outboxEvent.id.
  // Acknowledge only after durable processing succeeds.
  // await consumer.acknowledge(item, "delivered");
}
```

Requires an HTTPS Core origin, a `game_event_consumer` service token scoped to the exact server UUID, and an explicit `resolveScope` callback. Core's cluster and server IDs are checked against the claimed event; the resolver supplies the provider grid ID. This is required because legacy Core events may have a null grid ID and Sentinel's alert contract requires an explicit grid. Do not guess a grid from display names.

The client uses bounded timeouts, disables redirects and caching, validates event IDs, lease tokens, server scope and supported event types, then passes a schema-v1 normalized event to the existing `evaluateSentinelEvent` classifier. Alert objects remain metadata-only; provider payloads and player names are not copied into alerts.

The consumer and processor intentionally allow only one claimed event at a time. Multi-event claims are rejected until a worker can renew every outstanding lease concurrently. Claim and acknowledgement are separate explicit calls. No scheduler, polling loop, persistence, notification delivery, Discord connection, RCON, native ATLAS callback or game mutation is started. The caller owns durable idempotency using the stable Core outbox ID and must acknowledge `delivered` only after durable alert handling succeeds. Failed processing can be released with `acknowledge(item, "failed", errorMessage)`.

Core delivery is at-least-once. A crash after downstream work but before acknowledgement can cause redelivery; exactly-once processing is not claimed. This consumer complements rather than replaces the existing read-only health poller and scheduled health monitor.
