# CMG Sentinel

Security, operational visibility and administrative coordination for every ATLAS grid.

This Chaotic Minds Gaming (CMG) module extends [Chaotic Minds Gaming Core](https://github.com/Chaotic-Minds-Collective/cmg-atlas-core-plugin) and the [ATLAS integration adapter](https://github.com/Chaotic-Minds-Collective/cmg-atlas-plugin); it does not replace either foundation.

## Feature scope

- Extended RCON with allowlisted typed commands and audit
- Admin/mod capabilities scoped by grid/server
- Health, heartbeat, player counts and telemetry
- Crash detection and recovery coordination
- Death/kill, ship, structure, tribe and rate alerts
- Suspicious activity detection and Discord relay
- Config drift and backup status

## Current implementation

The first server-independent milestone is implemented in `src/index.js`:

- Classifies schema-v1 normalized ATLAS events into deterministic, metadata-only alerts.
- Includes initial rules for player deaths/kills, ship sinking, structure destruction and tame deaths.
- Qualifies alert IDs with cluster, grid, server, source event and rule identifiers.
- Rejects malformed or unsupported event envelopes.
- Does not copy event payloads into alerts or perform network, RCON, notification or game-state actions.

The second server-independent milestone adds `src/health.js`:

- Classifies normalized schema-v1 samples as healthy, degraded, offline or unknown.
- Uses configurable heartbeat warning/offline thresholds (90 seconds and 5 minutes by default).
- Carries cluster, grid and server scope into stable health sample IDs.
- Preserves a null player count as unavailable rather than inventing zero.

The Core contract milestone adds `src/core-heartbeat-adapter.js`:

- Maps the Core `GET /servers` response shape into Sentinel's normalized health sample.
- Distinguishes a fresh `online: false` observation from Core's five-minute stale-heartbeat projection.
- Requires explicit cluster, grid and server scope; Core's current public listing does not return cluster ID or provider grid ID.
- Uses a fake Core response in tests only; it performs no HTTP requests.

The explicit Core event consumer adds `src/core-event-consumer.js`:

- Claims only the five event types covered by Sentinel's current default alert rules, using Core's exact-match event-type filter.
- Validates server scope, event IDs and lease tokens, then invokes the existing metadata-only event classifier.
- Requires an explicit scope resolver because legacy events can lack provider grid IDs; Core cluster/server scope must match the resolver.
- Keeps claim and acknowledgement separate. The caller controls durable alert handling and acknowledgement.
- Does not start a scheduler, persist alerts, send notifications, connect to Discord, invoke RCON or mutate game state.

The read-only Core poller adds `src/core-poller.js`:\n\n- Performs one HTTPS GET to Core's public `/servers` endpoint with a bounded timeout, no-store caching and redirects disabled.\n- Requires an explicit caller-provided cluster/grid/server scope resolver; it does not infer stable IDs from display names.\n- Filters for ATLAS records, validates the response envelope and record count, and maps records through the tested heartbeat adapter.\n- Makes no retries, writes, authenticated requests, RCON calls or notifications.\n\nThese are read-only classifiers and contract adapters. The poller is a callable one-shot operation, not a scheduled monitor or production alert delivery service. A deployment still needs an approved Core origin, explicit scope mapping, secure runtime configuration and isolated connectivity validation.

## Development

Requires Node.js 22 or newer.

```sh
npm run check
npm test
```

GitHub Actions runs the same checks for relevant pushes and pull requests.

## Architecture and dependencies

- Core server registry, scoped RBAC, audit/outbox and plugin health
- ATLAS adapter: verified RCON/health/events
- Connect for alerts; Tribes for context
- Review existing [CMG ATLAS Admin Plugin](https://github.com/Chaotic-Minds-Collective/cmg-atlas-admin-plugin) backend and backup tools

Core remains authoritative for shared identity, authorization and platform records. ATLAS-native calls pass through the shared adapter. This module owns its domain state and must not connect directly to PostgreSQL.

## Safety boundaries

- No arbitrary shell passthrough or command injection.
- Auto-restart is disabled and is not implemented.
- No production server, live Core endpoint, RCON endpoint or third-party Workshop content is accessed.
- Native ATLAS compatibility and feature-specific callbacks remain unverified; unsupported capabilities must remain disabled.


### Scheduled read-only Core health monitor

The next Sentinel milestone adds `src/core-monitor.js`:

- Starts with one immediate poll, then uses a configurable interval (60 seconds by default).
<!-- coderabbit-ai-generated:v1 -->

<!-- coderabbit-ai-generated-section:v1:f2071e75b666096d48469a6a968c20f8b6d85e7419de8a8ce8bb254a16a06fc5 -->

- Prevents overlapping poll requests; concurrent manual and scheduled ticks share the in-flight operation.
- A `pollNow()` call made from an active `poll`, `onSamples`, or `onError` callback (including across async boundaries) resolves to `false` without starting or joining work, avoiding a callback waiting on itself. Calls outside those callbacks continue to share in-flight work.

<!-- /coderabbit-ai-generated:v1 -->
- Delivers samples only while running and suppresses late results after shutdown.
- Reports polling and sample-handler failures through an injected error callback without allowing a failing reporter to stop the scheduler.
- Uses injected polling and timer functions so lifecycle behavior can be tested without contacting Core or waiting on real timers.

This is a library lifecycle coordinator, not an installed background service. The caller must explicitly start it and supply the existing read-only poller plus sample handling. It does not persist health, send alerts, invoke RCON, restart servers, or access a live endpoint.

### Explicit Core event consumer

See [`docs/CORE_EVENT_CONSUMER.md`](docs/CORE_EVENT_CONSUMER.md) for the filtered claim client and caller-controlled acknowledgement contract. It is separate from health polling and does not auto-start.
