/**
 * Durable processing boundary for Tribes' claimed Core outbox events.
 *
 * The caller supplies the durable sink. It must transactionally record the
 * Core outbox event ID and normalized tribe event using idempotencyKey.
 */
const PERSISTENCE_FAILURE = "Tribes durable event processing failed";

function validateOptions(options) {
  if (!options || typeof options !== "object" || Array.isArray(options)) {
    throw new TypeError("options must be an object");
  }
  if (!options.consumer || typeof options.consumer.claim !== "function" ||
      typeof options.consumer.acknowledge !== "function") {
    throw new TypeError("consumer must provide claim() and acknowledge()");
  }
  if (typeof options.persistEvent !== "function") {
    throw new TypeError("persistEvent must be a function");
  }
  if (options.claimOptions !== undefined &&
      (!options.claimOptions || typeof options.claimOptions !== "object" ||
       Array.isArray(options.claimOptions))) {
    throw new TypeError("claimOptions must be an object when provided");
  }
  return options;
}

/**
 * Claim one event and process it before requesting another lease.
 *
 * persistEvent receives { idempotencyKey, event }. It must commit the event
 * record and its derived membership/activity changes atomically. Core
 * acknowledgement happens only after that durable operation succeeds.
 */
export async function processTribeOutboxBatch(input) {
  const { consumer, persistEvent, claimOptions } = validateOptions(input);
  const claimed = claimOptions === undefined
    ? await consumer.claim()
    : await consumer.claim(claimOptions);

  if (!Array.isArray(claimed)) {
    throw new TypeError("consumer.claim() must return an array");
  }
  if (claimed.length > 1) {
    throw new RangeError("consumer.claim() must return at most one event until concurrent lease renewal is implemented");
  }

  const results = [];
  for (const item of claimed) {
    const eventId = item?.outboxEvent?.id;
    if (typeof eventId !== "string" || eventId.trim() === "" ||
        !item || typeof item !== "object" || Array.isArray(item)) {
      results.push(Object.freeze({ eventId: null, status: "invalid_claim" }));
      continue;
    }

    const { outboxEvent, ...normalizedEvent } = item;
    try {
      await persistEvent(Object.freeze({
        idempotencyKey: eventId,
        event: Object.freeze(normalizedEvent)
      }));
    } catch {
      try {
        await consumer.acknowledge(item, "failed", PERSISTENCE_FAILURE);
        results.push(Object.freeze({ eventId, status: "retry_scheduled" }));
      } catch {
        // The active lease will expire and Core can make the event claimable.
        results.push(Object.freeze({
          eventId,
          status: "retry_pending_lease_expiry"
        }));
      }
      continue;
    }

    try {
      await consumer.acknowledge(item, "delivered");
      results.push(Object.freeze({ eventId, status: "delivered" }));
    } catch {
      // Persistence already committed. A replay is safe only because the
      // caller's durable sink deduplicates by the stable Core outbox event ID.
      results.push(Object.freeze({
        eventId,
        status: "acknowledgement_pending_lease_expiry"
      }));
    }
  }

  return Object.freeze(results);
}
