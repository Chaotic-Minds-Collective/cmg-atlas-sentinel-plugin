# Sentinel Core event processor

`processSentinelOutboxBatch` connects the existing explicit Core outbox consumer to a caller-owned durable alert sink.

## Processing contract

1. Claim one event through the configured Sentinel Core event consumer.
2. For the claimed event, call `persistEvent({ idempotencyKey, event, alerts })`.
3. The sink must atomically persist the outbox event ID as its idempotency key and all derived alert records. It must also mark events with an empty alert list as processed.
4. Only after the sink resolves successfully, acknowledge the Core event as `delivered`.
5. If durable processing fails, acknowledge it as `failed`; Core releases the lease and applies its retry backoff.
6. If acknowledgement itself fails, leave the lease alone. Core can reclaim it after expiry, and the sink must safely deduplicate the replay using the same outbox event ID.

## Delivery semantics

This is at-least-once delivery, not exactly-once processing. A crash after the durable sink commits but before Core accepts the acknowledgement can cause a replay. The stable outbox event ID must therefore be used as a unique idempotency key in the sink's transaction.

The processor handles exactly one event per invocation. Multi-event claim arrays are rejected until the processor can renew every outstanding lease concurrently. Invoke it again only after the current event has been durably processed and acknowledged or released for retry. It does not provide storage, start a scheduler, send notifications, invoke RCON, or mutate ATLAS game state.

## Example

```js
import { createSentinelCoreEventConsumer } from "./core-event-consumer.js";
import { processSentinelOutboxBatch } from "./core-event-processor.js";

const consumer = createSentinelCoreEventConsumer({
  baseUrl: process.env.CORE_API_URL,
  serviceToken: process.env.CORE_SERVICE_TOKEN,
  serverId: process.env.CORE_SERVER_ID,
  resolveScope: resolveServerScope
});

const results = await processSentinelOutboxBatch({
  consumer,
  claimOptions: { limit: 1, leaseSeconds: 120 },
  persistEvent: async ({ idempotencyKey, event, alerts }) => {
    // Implement one transaction in the caller's durable store:
    // insert the processed event ID and upsert all alert IDs.
  }
});
```

The caller remains responsible for supplying secure configuration, a durable idempotent sink, and an approved runtime lifecycle.
