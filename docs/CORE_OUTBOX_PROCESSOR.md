# Tribes Core outbox processor

`processTribeOutboxBatch` connects the existing explicit Core outbox consumer to a caller-owned durable persistence boundary.

## Processing contract

1. Claim one event through the configured Tribes Core outbox consumer.
2. For the claimed record, call `persistEvent({ idempotencyKey, event })`.
3. The sink must atomically record the stable Core outbox event ID and the normalized tribe event, plus any derived membership/activity changes.
4. Only after the sink resolves successfully, acknowledge the Core event as `delivered`.
5. If durable processing fails, acknowledge it as `failed`; Core releases the lease and applies its retry backoff.
6. If acknowledgement fails after persistence, leave the lease alone. Core can reclaim it after expiry, and the sink must deduplicate replay using the same outbox event ID.

## Delivery semantics

This is at-least-once delivery, not exactly-once processing. A crash after the durable sink commits but before Core accepts the acknowledgement can cause a replay. The stable outbox event ID must therefore be used as a unique idempotency key in the sink's transaction.

The processor handles exactly one event per invocation. Multi-event claim arrays are rejected until the processor can renew every outstanding lease concurrently. Invoke it again only after the current event has been durably processed and acknowledged or released for retry. It does not provide storage, start a scheduler, change tribe membership in ATLAS, or grant permissions.

## Example

```js
import { createCoreTribeOutboxConsumer } from "./core-outbox-consumer.js";
import { processTribeOutboxBatch } from "./core-outbox-processor.js";

const consumer = createCoreTribeOutboxConsumer({
  baseUrl: process.env.CORE_API_URL,
  serviceToken: process.env.CORE_SERVICE_TOKEN,
  serverId: process.env.CORE_SERVER_ID
});

const results = await processTribeOutboxBatch({
  consumer,
  claimOptions: { limit: 1, leaseSeconds: 120 },
  persistEvent: async ({ idempotencyKey, event }) => {
    // Implement one transaction in the caller's durable store:
    // record idempotencyKey and apply the normalized tribe event.
  }
});
```

The caller remains responsible for secure configuration, durable idempotent storage, and an approved runtime lifecycle. Provider IDs remain opaque strings; display names must never become identity or authorization inputs.
