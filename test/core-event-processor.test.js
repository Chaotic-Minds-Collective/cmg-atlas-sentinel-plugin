import test from "node:test";
import assert from "node:assert/strict";
import { processSentinelOutboxBatch } from "../src/core-event-processor.js";

const event = Object.freeze({ eventId: "provider-event-1", eventType: "ship.sunk" });
const alert = Object.freeze({ alertId: "cluster:grid:server:event:ship-sunk", ruleId: "ship-sunk" });

function claimed(id, alerts = [alert]) {
  return Object.freeze({
    outboxEvent: Object.freeze({ id }),
    event,
    alerts: Object.freeze(alerts)
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

test("persists each event before acknowledging it as delivered", async () => {
  const { consumer, calls } = fakeConsumer([claimed("outbox-1")]);
  const persisted = [];
  const result = await processSentinelOutboxBatch({
    consumer,
    claimOptions: { limit: 1 },
    async persistEvent(record) {
      calls.push({ type: "persist", id: record.idempotencyKey });
      persisted.push(record);
    }
  });

  assert.deepEqual(result, [{
    eventId: "outbox-1",
    status: "delivered",
    alertCount: 1
  }]);
  assert.deepEqual(calls.map((call) => call.type), ["claim", "persist", "acknowledge"]);
  assert.equal(calls[2].outcome, "delivered");
  assert.equal(persisted[0].idempotencyKey, "outbox-1");
  assert.equal(persisted[0].event, event);
  assert.deepEqual(persisted[0].alerts, [alert]);
});

test("durably marks non-alerting events before delivery acknowledgement", async () => {
  const { consumer, calls } = fakeConsumer([claimed("outbox-empty", [])]);
  let persisted;
  const result = await processSentinelOutboxBatch({
    consumer,
    async persistEvent(record) { persisted = record; }
  });

  assert.equal(persisted.idempotencyKey, "outbox-empty");
  assert.deepEqual(persisted.alerts, []);
  assert.equal(result[0].status, "delivered");
  assert.equal(calls.at(-1).outcome, "delivered");
});

test("reports persistence failures to Core for retry without acknowledging delivery", async () => {
  const { consumer, calls } = fakeConsumer([claimed("outbox-2")]);
  const result = await processSentinelOutboxBatch({
    consumer,
    async persistEvent() { throw new Error("database unavailable"); }
  });

  assert.equal(result[0].status, "retry_scheduled");
  assert.deepEqual(calls.at(-1), {
    type: "acknowledge",
    id: "outbox-2",
    outcome: "failed",
    error: "Sentinel event persistence failed"
  });
});

test("leaves a persisted event leased for idempotent replay when delivery ack fails", async () => {
  const { consumer, calls } = fakeConsumer([claimed("outbox-3")], async () => {
    throw new Error("Core unavailable");
  });
  const result = await processSentinelOutboxBatch({
    consumer,
    async persistEvent() {}
  });

  assert.equal(result[0].status, "acknowledgement_pending_lease_expiry");
  assert.deepEqual(calls.filter((call) => call.type === "acknowledge")
    .map((call) => call.outcome), ["delivered"]);
});

test("leaves a failed event leased when its retry acknowledgement also fails", async () => {
  const { consumer } = fakeConsumer([claimed("outbox-4")], async () => {
    throw new Error("Core unavailable");
  });
  const result = await processSentinelOutboxBatch({
    consumer,
    async persistEvent() { throw new Error("database unavailable"); }
  });

  assert.equal(result[0].status, "retry_pending_lease_expiry");
});

test("rejects multi-event batches until every outstanding lease can be renewed concurrently", async () => {
  const { consumer, calls } = fakeConsumer([claimed("outbox-5"), claimed("outbox-6")]);
  let persisted = 0;
  await assert.rejects(
    processSentinelOutboxBatch({ consumer, async persistEvent() { persisted += 1; } }),
    /at most one event/
  );
  assert.equal(persisted, 0);
  assert.deepEqual(calls.map((call) => call.type), ["claim"]);
});

test("validates required collaborators and claim response", async () => {
  await assert.rejects(
    processSentinelOutboxBatch({ consumer: {}, persistEvent() {} }),
    /consumer must provide/
  );
  await assert.rejects(
    processSentinelOutboxBatch({ consumer: { claim: async () => [], acknowledge() {} } }),
    /persistEvent must be a function/
  );
  const { consumer } = fakeConsumer(null);
  await assert.rejects(
    processSentinelOutboxBatch({ consumer, persistEvent() {} }),
    /must return an array/
  );
});
