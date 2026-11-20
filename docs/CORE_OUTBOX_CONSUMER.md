# Core Tribe Event Consumer

The Tribes package exposes an explicitly invoked client for Core's server-scoped event outbox. It requests only `tribe.created`, `tribe.member.joined`, `tribe.member.left` and `tribe.updated`.

```js
import { createCoreTribeOutboxConsumer } from "../src/core-outbox-consumer.js";
const consumer = createCoreTribeOutboxConsumer({
  baseUrl: process.env.CMG_CORE_ORIGIN,
  serviceToken: process.env.CMG_CORE_SERVICE_TOKEN,
  serverId: process.env.CMG_CORE_SERVER_ID
});
const claimed = await consumer.claim({ limit: 1, leaseSeconds: 120 });
for (const event of claimed) {
  // Persist an idempotent receipt using event.outboxEvent.id.
  // Acknowledge only after durable processing succeeds.
  // await consumer.acknowledge(event, "delivered");
}
```

Requires an HTTPS origin, a Core `game_event_consumer` token scoped to the exact server UUID, and Node.js 22 or newer. Requests use bounded timeouts, disable redirects and caching, and validate server scope and event type before normalization.

The consumer and processor intentionally allow only one claimed event at a time. Multi-event claims are rejected until a worker can renew every outstanding lease concurrently. Claim and acknowledgement are separate explicit calls. There is no scheduler, polling loop, persistence, downstream delivery, Discord connection, native ATLAS callback or game mutation. The caller owns durable idempotency using the stable Core outbox ID and must acknowledge `delivered` only after durable work succeeds. `acknowledge(event, "failed", errorMessage)` releases a failed attempt for Core retry. Lease conflicts are surfaced.

Core delivery is at-least-once. Redelivery can occur if a process crashes after downstream work but before acknowledgement. The normalized event is an activity record, not a complete membership snapshot; do not infer membership state or privileged actions from partial events.
