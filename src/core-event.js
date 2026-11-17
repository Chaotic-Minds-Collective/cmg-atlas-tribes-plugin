const TRIBE_EVENT_TYPES = new Set([
  "tribe.created",
  "tribe.member.joined",
  "tribe.member.left",
  "tribe.updated"
]);

const ISO_DATE_TIME = /^(\d{4})-(\d{2})-(\d{2})T([01]\d|2[0-3]):([0-5]\d):([0-5]\d)(\.\d+)?(Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/;

function object(value, field) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new TypeError(field + " must be an object");
  return value;
}
function requiredString(value, field) {
  if (typeof value !== "string" || value.trim() === "") throw new TypeError(field + " must be a non-empty string");
  return value;
}
function optionalString(value, field) {
  if (value === undefined || value === null) return null;
  return requiredString(value, field);
}
function timestamp(value) {
  if (typeof value !== "string") {
    throw new TypeError("occurredAt must be an ISO date-time with timezone");
  }

  const match = ISO_DATE_TIME.exec(value);
  if (!match) {
    throw new TypeError("occurredAt must be an ISO date-time with timezone");
  }

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const daysInMonth = [
    31,
    year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0) ? 29 : 28,
    31, 30, 31, 30, 31, 31, 30, 31, 30, 31
  ];
  if (month < 1 || month > 12 || day < 1 || day > daysInMonth[month - 1]) {
    throw new TypeError("occurredAt must be a valid ISO date-time with timezone");
  }

  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) {
    throw new TypeError("occurredAt must be a valid ISO date-time with timezone");
  }
  return new Date(parsed).toISOString();
}
function escapeIdPart(value) {
  return value.replaceAll("%", "%25").replaceAll(":", "%3A");
}

/**
 * Normalize the documented Core game.event.received outbox payload.
 * This does not infer native callbacks, build snapshots, or mutate membership.
 */
export function normalizeCoreTribeEvent(input) {
  const envelope = object(input, "event");
  if (envelope.eventType !== "game.event.received") throw new TypeError("event.eventType must be game.event.received");
  const event = object(envelope.payload, "event.payload");
  if (event.game !== "atlas") throw new TypeError("payload.game must be atlas");
  if (!TRIBE_EVENT_TYPES.has(event.eventType)) throw new TypeError("payload.eventType is not a supported tribe event");

  const clusterId = requiredString(event.clusterId, "payload.clusterId");
  const gridId = optionalString(event.gridId, "payload.gridId");
  const serverId = requiredString(event.serverId, "payload.serverId");
  const providerPayload = object(event.payload, "payload.payload");
  const providerTribeId = requiredString(providerPayload.providerTribeId, "payload.payload.providerTribeId");
  const isMembership = event.eventType === "tribe.member.joined" || event.eventType === "tribe.member.left";
  const providerMemberId = isMembership
    ? requiredString(providerPayload.providerMemberId, "payload.payload.providerMemberId")
    : optionalString(providerPayload.providerMemberId, "payload.payload.providerMemberId");
  const occurredAt = timestamp(event.occurredAt);
  const sourceEventId = requiredString(event.eventId, "payload.eventId");
  const eventRecordId = requiredString(event.eventRecordId, "payload.eventRecordId");
  const action = event.eventType === "tribe.member.joined" ? "member.joined"
    : event.eventType === "tribe.member.left" ? "member.left"
    : event.eventType === "tribe.created" ? "tribe.created" : "tribe.updated";

  return Object.freeze({
    schemaVersion: 1,
    eventRecordId,
    sourceEventId,
    action,
    occurredAt,
    correlationId: optionalString(event.correlationId, "payload.correlationId"),
    scope: Object.freeze({ clusterId, gridId, serverId }),
    tribe: Object.freeze({
      tribeKey: [clusterId, providerTribeId].map(escapeIdPart).join(":"),
      providerTribeId,
      providerMemberId,
      displayName: optionalString(providerPayload.displayName, "payload.payload.displayName")
    })
  });
}
