/**
 * Durable processing boundary for Sentinel's claimed Core game events.
 *
 * The caller supplies the durable sink. It must transactionally record the
 * outbox event ID and any derived alerts, using idempotencyKey for replay
 * protection. Core acknowledgement happens only after that operation succeeds.
 */
const PERSISTENCE_FAILURE = "Sentinel event persistence failed";

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
 * Claim one batch and process it sequentially.
 *
 * persistEvent receives { idempotencyKey, event, alerts }. It must commit the
 * processed-event marker and alert records atomically. Empty alert arrays are
 * still passed to the sink so non-alerting events can be durably marked done.
 */
export async function processSentinelOutboxBatch(input) {
  const { consumer, persistEvent, claimOptions } = validateOptions(input);
  const claimed = claimOptions === undefined
    ? await consumer.claim()
    : await consumer.claim(claimOptions);

  if (!Array.isArray(claimed)) {
    throw new TypeError("consumer.claim() must return an array");
  }

  const results = [];
  for (const item of claimed) {
    const eventId = item?.outboxEvent?.id;
    if (typeof eventId !== "string" || eventId.trim() === "" ||
        !item.event || typeof item.event !== "object" ||
        !Array.isArray(item.alerts)) {
      results.push(Object.freeze({ eventId: null, status: "invalid_claim" }));
      continue;
    }

    try {
      await persistEvent(Object.freeze({
        idempotencyKey: eventId,
        event: item.event,
        alerts: item.alerts
      }));
    } catch {
      try {
        await consumer.acknowledge(item, "failed", PERSISTENCE_FAILURE);
        results.push(Object.freeze({
          eventId,
          status: "retry_scheduled",
          alertCount: item.alerts.length
        }));
      } catch {
        // The active lease will expire and Core can make the event claimable.
        results.push(Object.freeze({
          eventId,
          status: "retry_pending_lease_expiry",
          alertCount: item.alerts.length
        }));
      }
      continue;
    }

    try {
      await consumer.acknowledge(item, "delivered");
      results.push(Object.freeze({
        eventId,
        status: "delivered",
        alertCount: item.alerts.length
      }));
    } catch {
      // Persistence already committed. Do not report a processing failure:
      // replay is safe only because the sink uses the stable event ID.
      results.push(Object.freeze({
        eventId,
        status: "acknowledgement_pending_lease_expiry",
        alertCount: item.alerts.length
      }));
    }
  }

  return Object.freeze(results);
}
