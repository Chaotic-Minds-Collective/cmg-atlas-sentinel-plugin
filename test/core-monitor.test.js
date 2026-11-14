import test from "node:test";
import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import {
  createCoreHealthMonitor,
  SENTINEL_CORE_MONITOR_DEFAULTS
} from "../src/core-monitor.js";

function harness(overrides = {}) {
  const calls = { polls: 0, samples: [], errors: [], cleared: [] };
  let tick;
  const monitor = createCoreHealthMonitor({
    intervalMs: 5_000,
    poll: async () => {
      calls.polls += 1;
      return [{ id: calls.polls }];
    },
    onSamples: async (samples) => {
      calls.samples.push(samples);
    },
    onError: async (error) => {
      calls.errors.push(error);
    },
    setIntervalImpl: (callback, intervalMs) => {
      calls.intervalMs = intervalMs;
      tick = callback;
      return "timer-1";
    },
    clearIntervalImpl: (handle) => calls.cleared.push(handle),
    ...overrides
  });
  return { monitor, calls, tick: () => tick() };
}

test("starts with an immediate poll and schedules the configured interval", async () => {
  const { monitor, calls } = harness();
  assert.equal(monitor.running, false);
  assert.equal(monitor.start(), true);
  assert.equal(monitor.running, true);
  assert.equal(calls.intervalMs, 5_000);
  assert.equal(await monitor.pollNow(), true);
  assert.equal(calls.polls, 1);
  assert.equal(calls.samples.length, 1);
  assert.deepEqual(calls.samples[0], [{ id: 1 }]);
  assert.equal(monitor.stop(), true);
});

test("start and stop are idempotent", async () => {
  const { monitor, calls } = harness();
  assert.equal(monitor.start(), true);
  assert.equal(monitor.start(), false);
  await monitor.pollNow();
  assert.equal(monitor.stop(), true);
  assert.equal(monitor.stop(), false);
  assert.deepEqual(calls.cleared, ["timer-1"]);
  assert.equal(monitor.running, false);
});

test("scheduled ticks do not overlap an in-flight poll", async () => {
  let resolvePoll;
  let polls = 0;
  const { monitor, tick } = harness({
    poll: () => {
      polls += 1;
      return new Promise((resolve) => {
        resolvePoll = resolve;
      });
    }
  });
  monitor.start();
  const first = monitor.pollNow();
  tick();
  tick();
  assert.equal(polls, 1);
  resolvePoll([]);
  assert.equal(await first, true);
  assert.equal(polls, 1);
  monitor.stop();
});

test("poll failures are reported and later ticks can recover", async () => {
  let attempt = 0;
  const { monitor, calls, tick } = harness({
    poll: async () => {
      attempt += 1;
      if (attempt === 1) throw new Error("Core unavailable");
      return [];
    }
  });
  monitor.start();
  assert.equal(await monitor.pollNow(), false);
  assert.equal(calls.errors.length, 1);
  assert.match(calls.errors[0].message, /Core unavailable/);
  tick();
  await monitor.pollNow();
  assert.equal(calls.samples.length, 1);
  monitor.stop();
});

test("stopping prevents a late in-flight result from being delivered", async () => {
  let resolvePoll;
  const { monitor, calls } = harness({
    poll: () => new Promise((resolve) => {
      resolvePoll = resolve;
    })
  });
  monitor.start();
  const pending = monitor.pollNow();
  monitor.stop();
  resolvePoll([{ id: "late" }]);
  assert.equal(await pending, true);
  assert.equal(calls.samples.length, 0);
});

test("pollNow requires a running monitor", () => {
  const { monitor } = harness();
  assert.throws(() => monitor.pollNow(), /must be running/);
});

test("validates interval bounds and callback contracts", () => {
  assert.equal(SENTINEL_CORE_MONITOR_DEFAULTS.intervalMs, 60_000);
  for (const intervalMs of [0, 999, 3_600_001, 1.5]) {
    assert.throws(() => createCoreHealthMonitor({
      poll: async () => [],
      onSamples: () => {},
      intervalMs
    }), /intervalMs/);
  }
  assert.throws(() => createCoreHealthMonitor({
    poll: async () => []
  }), /onSamples/);
  assert.throws(() => createCoreHealthMonitor({
    poll: async () => [],
    onSamples: () => {},
    onError: true
  }), /onError/);
});

test("rejects a poll result that is not an array", async () => {
  const { monitor, calls } = harness({
    poll: async () => ({ data: [] })
  });
  monitor.start();
  assert.equal(await monitor.pollNow(), false);
  assert.match(calls.errors[0].message, /array/);
  monitor.stop();
});

test("an error callback failure does not break the monitor", async () => {
  let attempts = 0;
  const { monitor } = harness({
    poll: async () => {
      attempts += 1;
      throw new Error("poll failed");
    },
    onError: () => {
      throw new Error("reporter failed");
    }
  });
  monitor.start();
  assert.equal(await monitor.pollNow(), false);
  assert.equal(monitor.running, true);
  assert.equal(attempts, 1);
  monitor.stop();
});

test("synchronous poll failures without onError recover on a later tick", async () => {
  let attempts = 0;
  const { monitor, calls, tick } = harness({
    onError: undefined,
    poll: () => {
      attempts += 1;
      if (attempts === 1) throw new Error("synchronous failure");
      return [];
    }
  });
  monitor.start();
  // Let the initial operation settle without initiating another poll.
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(monitor.polling, false);
  tick();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(attempts, 2);
  assert.equal(calls.samples.length, 1);
  assert.equal(monitor.polling, false);
  monitor.stop();
  monitor.start();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(attempts, 3);
  monitor.stop();
});

for (const fails of [false, true]) {
  test(`restart discards previous run's ${fails ? "errors" : "samples"} without overlapping polls`, async () => {
    let settle;
    let attempts = 0;
    const { monitor, calls, tick } = harness({
      poll: () => {
        attempts += 1;
        if (attempts > 1) return [{ id: "fresh" }];
        return new Promise((resolve, reject) => {
          settle = () => fails ? reject(new Error("stale")) : resolve([{ id: "stale" }]);
        });
      }
    });
    monitor.start();
    const previous = monitor.pollNow();
    monitor.stop();
    monitor.start();
    const current = monitor.pollNow();
    tick();
    assert.equal(attempts, 1);
    settle();
    assert.equal(await previous, !fails);
    assert.equal(await current, true);
    assert.equal(attempts, 2);
    assert.deepEqual(calls.samples, [[{ id: "fresh" }]]);
    assert.deepEqual(calls.errors, []);
    assert.equal(monitor.polling, false);
    monitor.stop();
  });
}

for (const callback of ["poll", "onSamples", "onError"]) {
  for (const restart of [false, true]) {
    test(`${callback} can await pollNow after an async boundary${restart ? " and restart" : ""}`, { timeout: 1_000 }, async () => {
      let monitor;
      let reentered = false;
      let nestedResult;
      const options = {
        [callback]: async () => {
          if (!reentered) {
            reentered = true;
            await new Promise((resolve) => setImmediate(resolve));
            if (restart) {
              monitor.stop();
              monitor.start();
            }
            nestedResult = await monitor.pollNow();
          }
          return [];
        }
      };
      if (callback === "onError") options.poll = () => { throw new Error("poll failed"); };
      const h = harness(options);
      monitor = h.monitor;
      try {
        monitor.start();
        const pending = monitor.pollNow();
        assert.equal(await pending, callback !== "onError");
        // Drain restart work and verify subsequent ticks also make progress.
        await new Promise((resolve) => setImmediate(resolve));
        assert.equal(nestedResult, false);
        assert.equal(monitor.polling, false);
        h.tick();
        assert.equal(await monitor.pollNow(), callback !== "onError");
        assert.equal(monitor.polling, false);
      } finally {
        monitor.stop();
      }
    });
  }
}


test("idle monitors release async storage only after callbacks settle", async (t) => {
  const enabled = new Set();
  const originalRun = AsyncLocalStorage.prototype.run;
  const originalDisable = AsyncLocalStorage.prototype.disable;
  t.mock.method(AsyncLocalStorage.prototype, "run", function (...args) {
    enabled.add(this);
    return originalRun.apply(this, args);
  });
  t.mock.method(AsyncLocalStorage.prototype, "disable", function (...args) {
    enabled.delete(this);
    return originalDisable.apply(this, args);
  });
  let release;
  const { monitor } = harness({
    onSamples: () => new Promise((resolve) => { release = resolve; })
  });
  assert.equal(enabled.size, 0);
  try {
    monitor.start();
    const pending = monitor.pollNow();
    await new Promise((resolve) => setImmediate(resolve));
    monitor.stop();
    assert.equal(enabled.size, 1, "stop must retain guards for active callbacks");
    release();
    await pending;
    assert.equal(enabled.size, 0);
    // Repeatedly discarded monitors must not accumulate enabled storage.
    for (let i = 0; i < 10; i += 1) {
      const next = harness().monitor;
      next.start();
      await next.pollNow();
      next.stop();
      assert.equal(enabled.size, 0);
    }
    monitor.start();
    const restarted = monitor.pollNow();
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(enabled.size, 1, "restart re-enables callback tracking");
    release();
    await restarted;
    assert.equal(enabled.size, 0);
  } finally {
    release?.();
    monitor.stop();
  }
});
