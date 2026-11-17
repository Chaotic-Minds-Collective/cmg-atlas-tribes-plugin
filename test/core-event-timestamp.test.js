import test from "node:test";
import assert from "node:assert/strict";
import { normalizeCoreTribeEvent } from "../src/core-event.js";

function event(occurredAt) {
  return {
    eventType: "game.event.received",
    payload: {
      eventRecordId: "core-record-1",
      eventId: "provider-event-1",
      game: "atlas",
      eventType: "tribe.member.joined",
      occurredAt,
      clusterId: "cluster-1",
      gridId: "grid-a",
      serverId: "server-1",
      payload: { providerTribeId: "tribe-42", providerMemberId: "player-7" }
    }
  };
}

test("accepts valid calendar dates and timezone offsets", () => {
  assert.equal(
    normalizeCoreTribeEvent(event("2024-02-29T10:00:00-08:00")).occurredAt,
    "2024-02-29T18:00:00.000Z"
  );
  assert.equal(
    normalizeCoreTribeEvent(event("2026-11-13T18:00:00Z")).occurredAt,
    "2026-11-13T18:00:00.000Z"
  );
});

test("rejects impossible calendar dates and malformed date-time fields", () => {
  for (const value of [
    "2026-02-30T18:00:00Z",
    "2025-02-29T18:00:00Z",
    "2026-13-01T18:00:00Z",
    "2026-11-13T24:00:00Z",
    "2026-11-13T18:60:00Z",
    "2026-11-13",
    "yesterday"
  ]) {
    assert.throws(
      () => normalizeCoreTribeEvent(event(value)),
      /occurredAt must be (a valid )?ISO date-time with timezone/
    );
  }
});
