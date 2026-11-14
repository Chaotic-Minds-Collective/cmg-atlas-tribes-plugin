import test from "node:test";
import assert from "node:assert/strict";
import { reconcileTribeSnapshots } from "../src/reconcile.js";

const base = {
  schemaVersion: 1,
  observedAt: "2026-11-14T10:00:00.000Z",
  scope: { clusterId: "cluster-1", gridId: "grid-a", serverId: "server-1" },
  tribe: {
    providerTribeId: "tribe-1",
    displayName: "Example Tribe",
    members: [
      { providerMemberId: "player-1", displayName: "One" },
      { providerMemberId: "player-2", displayName: "Two" }
    ]
  }
};

function input(overrides = {}) {
  return {
    ...base,
    ...overrides,
    scope: { ...base.scope, ...overrides.scope },
    tribe: { ...base.tribe, ...overrides.tribe }
  };
}

test("produces a read-only membership reconciliation for a newer snapshot", () => {
  const previous = input();
  const next = input({
    observedAt: "2026-11-14T10:01:00.000Z",
    tribe: {
      ...base.tribe,
      members: [
        { providerMemberId: "player-2", displayName: "Two" },
        { providerMemberId: "player-3", displayName: "Three" }
      ]
    }
  });
  const result = reconcileTribeSnapshots(previous, next);
  assert.equal(result.status, "changed");
  assert.deepEqual(result.membership.joined, ["player-3"]);
  assert.deepEqual(result.membership.left, ["player-1"]);
  assert.deepEqual(result.membership.unchanged, ["player-2"]);
  assert.equal(result.snapshot.observedAt, "2026-11-14T10:01:00.000Z");
  assert.ok(Object.isFrozen(result));
  assert.ok(Object.isFrozen(result.membership));
  assert.ok(Object.isFrozen(result.snapshot));
});

test("reports unchanged membership while accepting a newer observation", () => {
  const result = reconcileTribeSnapshots(base, input({
    observedAt: "2026-11-14T10:02:00.000Z",
    scope: { gridId: "grid-b", serverId: "server-2" },
    tribe: { ...base.tribe, displayName: "Renamed Tribe" }
  }));
  assert.equal(result.status, "unchanged");
  assert.deepEqual(result.membership.joined, []);
  assert.deepEqual(result.membership.left, []);
  assert.deepEqual(result.membership.unchanged, ["player-1", "player-2"]);
  assert.equal(result.snapshot.scope.gridId, "grid-b");
});

test("treats an exact replay as idempotent", () => {
  const result = reconcileTribeSnapshots(base, structuredClone(base));
  assert.equal(result.status, "duplicate");
  assert.equal(result.membership, null);
  assert.equal(result.observationChanged, false);
});

test("rejects older observations rather than rolling membership backward", () => {
  assert.throws(() => reconcileTribeSnapshots(
    input({ observedAt: "2026-11-14T10:01:00.000Z" }),
    input({ observedAt: "2026-11-14T10:00:00.000Z" })
  ), /older than the previous observation/);
});

test("rejects conflicting snapshots with the same observation timestamp", () => {
  const conflicting = input({
    tribe: {
      ...base.tribe,
      members: [{ providerMemberId: "different-player" }]
    }
  });
  assert.throws(() => reconcileTribeSnapshots(base, conflicting), /conflicting snapshots/);
});

test("rejects cross-cluster tribe reconciliation", () => {
  assert.throws(() => reconcileTribeSnapshots(base, input({
    scope: { clusterId: "another-cluster" }
  })), /same cluster-scoped tribe/);
});

test("does not mutate caller-owned snapshots", () => {
  const previous = structuredClone(base);
  const next = input({ observedAt: "2026-11-14T10:03:00.000Z" });
  const beforePrevious = structuredClone(previous);
  const beforeNext = structuredClone(next);
  reconcileTribeSnapshots(previous, next);
  assert.deepEqual(previous, beforePrevious);
  assert.deepEqual(next, beforeNext);
});
