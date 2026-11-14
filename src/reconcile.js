import {
  diffTribeMembershipSnapshots,
  normalizeTribeSnapshot
} from "./snapshot.js";

function timestamp(snapshot) {
  return Date.parse(snapshot.observedAt);
}

function sameSnapshot(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

/**
 * Produce a deterministic, read-only reconciliation decision from two provider
 * snapshots. The caller owns storage and delivery; this function has no I/O.
 *
 * Older observations are rejected. An exact replay is idempotent. Conflicting
 * content with the same observation time is rejected rather than guessed.
 */
export function reconcileTribeSnapshots(previousInput, nextInput) {
  const previous = normalizeTribeSnapshot(previousInput);
  const next = normalizeTribeSnapshot(nextInput);

  if (previous.tribe.tribeKey !== next.tribe.tribeKey) {
    throw new TypeError("snapshots must refer to the same cluster-scoped tribe");
  }

  const previousTime = timestamp(previous);
  const nextTime = timestamp(next);

  if (nextTime < previousTime) {
    throw new RangeError("next snapshot is older than the previous observation");
  }

  if (nextTime === previousTime) {
    if (!sameSnapshot(previous, next)) {
      throw new TypeError("conflicting snapshots share the same observation timestamp");
    }
    return Object.freeze({
      status: "duplicate",
      snapshot: previous,
      membership: null,
      observationChanged: false
    });
  }

  const membership = diffTribeMembershipSnapshots(previous, next);
  return Object.freeze({
    status: membership.joined.length || membership.left.length ? "changed" : "unchanged",
    snapshot: next,
    membership,
    observationChanged: true
  });
}
