import test from "node:test";
import assert from "node:assert/strict";
import { processTribeOutboxBatch } from "../src/core-outbox-processor.js";

const event = Object.freeze({
  schemaVersion: 1,
  eventRecordId: "core-record-1",
  sourceEventId: "provider-event-1",
  action: "member.joined",
  occurredAt: "2026-11-16T18:00:00.000Z",
  tribe: Object.freeze({ tribeKey: "cluster-1:tribe-42", providerTribeId: "tribe-42" })
});

function claimed(id) {
  return Object.freeze({
    ...event,
    outboxEvent: Object.freeze({ id })
  });
}

function fakeConsumer(batch, acknowledgeImpl = async () => ({ accepted: true })) {
  const calls = [];
  return {
    calls,
    consumer: {
      async claim(options) {
        calls.push({ type: "claim", options });
        return batch;
      },
      async acknowledge(item, outcome, error) {
        calls.push({ type: "acknowledge", id: item.outboxEvent.id, outcome, error });
        return acknowledgeImpl(item, outcome, error);
      }
    }
  };
}

test("persists normalized tribe events before acknowledging delivery", async () => {
  const { consumer, calls } = fakeConsumer([claimed("outbox-1")]);
  const persisted = [];
  const result = await processTribeOutboxBatch({
    consumer,
    claimOptions: { limit: 10 },
    async persistEvent(record) {
      calls.push({ type: "persist", id: record.idempotencyKey });
      persisted.push(record);
    }
  });

  assert.deepEqual(result, [{ eventId: "outbox-1", status: "delivered" }]);
  assert.deepEqual(calls.map((call) => call.type), ["claim", "persist", "acknowledge"]);
  assert.equal(calls[2].outcome, "delivered");
  assert.equal(persisted[0].idempotencyKey, "outbox-1");
  assert.deepEqual(persisted[0].event, event);
  assert.equal("outboxEvent" in persisted[0].event, false);
});

test("reports durable processing failures to Core for retry", async () => {
  const { consumer, calls } = fakeConsumer([claimed("outbox-2")]);
  const result = await processTribeOutboxBatch({
    consumer,
    async persistEvent() { throw new Error("database unavailable"); }
  });

  assert.equal(result[0].status, "retry_scheduled");
  assert.deepEqual(calls.at(-1), {
    type: "acknowledge",
    id: "outbox-2",
    outcome: "failed",
    error: "Tribes durable event processing failed"
  });
});

test("leaves committed events leased for replay when delivery acknowledgement fails", async () => {
  const { consumer, calls } = fakeConsumer([claimed("outbox-3")], async () => {
    throw new Error("Core unavailable");
  });
  const result = await processTribeOutboxBatch({
    consumer,
    async persistEvent() {}
  });

  assert.equal(result[0].status, "acknowledgement_pending_lease_expiry");
  assert.deepEqual(calls.filter((call) => call.type === "acknowledge")
    .map((call) => call.outcome), ["delivered"]);
});

test("leaves failed events leased when retry acknowledgement fails", async () => {
  const { consumer } = fakeConsumer([claimed("outbox-4")], async () => {
    throw new Error("Core unavailable");
  });
  const result = await processTribeOutboxBatch({
    consumer,
    async persistEvent() { throw new Error("database unavailable"); }
  });

  assert.equal(result[0].status, "retry_pending_lease_expiry");
});

test("continues sequentially after an event persistence failure", async () => {
  const { consumer, calls } = fakeConsumer([
    claimed("outbox-5"),
    claimed("outbox-6")
  ]);
  const result = await processTribeOutboxBatch({
    consumer,
    async persistEvent(record) {
      calls.push({ type: "persist", id: record.idempotencyKey });
      if (record.idempotencyKey === "outbox-5") throw new Error("temporary");
    }
  });

  assert.deepEqual(result.map((item) => item.status), ["retry_scheduled", "delivered"]);
  assert.deepEqual(calls.filter((call) => call.type === "persist")
    .map((call) => call.id), ["outbox-5", "outbox-6"]);
});

test("validates collaborators and claim response", async () => {
  await assert.rejects(
    processTribeOutboxBatch({ consumer: {}, persistEvent() {} }),
    /consumer must provide/
  );
  await assert.rejects(
    processTribeOutboxBatch({ consumer: { claim: async () => [], acknowledge() {} } }),
    /persistEvent must be a function/
  );
  const { consumer } = fakeConsumer(null);
  await assert.rejects(
    processTribeOutboxBatch({ consumer, persistEvent() {} }),
    /must return an array/
  );
});
