# observai-sdk

Lightweight Node.js SDK for [ObservAI] — an AI-powered observability platform that automatically detects production incidents and generates Root Cause Analysis reports.

Add it to any Express app in 3 lines. No changes to your existing routes.

```js
const { ObserveAIClient } = require('observai-sdk');
const client = new ObserveAIClient({ apiKey: 'obs_live_xxx', serviceName: 'checkout-service' });
app.use(client.expressMiddleware());
```

---

## Features

- 🔍 **Auto-captures requests** — method, path, status code, duration, as structured logs and trace spans
- 🚨 **Auto-captures errors** — full stack traces via `expressErrorHandler()`
- 🗃️ **MongoDB/Mongoose query tracing** — via `mongooseMiddleware()`
- 📦 **Batching** — buffers events in memory, flushes every 5s or 100 events, so your app never pays a per-request network cost
- 🔁 **Retry with backoff** — 3 attempts (500ms → 1s → 2s) before a batch is re-queued for the next flush cycle, so a network blip doesn't silently lose data
- 🔒 **Reliable under load** — only one flush is ever in flight at a time; concurrent triggers (timer, batch threshold, manual calls) can no longer race each other or duplicate/lose telemetry
- 🛑 **Graceful shutdown** — buffered telemetry gets a bounded chance to flush on `SIGINT`/`SIGTERM` before the process exits
- 🪶 **Zero disk writes** — everything lives in memory and streams out over HTTP; no log files to manage or rotate

---

## Install

```
npm install github:ShivangiP2005/observai-sdk
```

(Will become `npm install observai-sdk` once published to the public npm registry.)

---

## Quick Start

```js
const express = require('express');
const { ObserveAIClient } = require('observai-sdk');

const client = new ObserveAIClient({
  apiKey: 'obs_live_xxx',           // from your ObservAI project settings
  serviceName: 'checkout-service',
  endpointUrl: 'https://api.observai.dev/api/v1/sdk/ingest',
  environment: 'production',
});

const app = express();

app.use(client.expressMiddleware());     // auto-logs + traces every request

app.get('/', (req, res) => res.send('OK'));

app.use(client.expressErrorHandler());   // auto-captures unhandled errors
app.listen(3000);
```

### With MongoDB

```js
const mongoose = require('mongoose');
mongoose.plugin(client.mongooseMiddleware());   // add BEFORE defining your schemas
```

### Manual capture

```js
client.captureLog('WARN', 'Payment retry attempted', { orderId: 123 });
client.captureException(someError, /* handled */ true);
```

### Manual teardown

If you need to stop the client without exiting the process (e.g. in tests), call `destroy()` — this clears the background timer and removes the shutdown signal listeners:

```js
client.destroy();
```

---

## Configuration

| Option                   | Required | Default                                   | Description                                                     |
| ------------------------ | -------- | ------------------------------------------ | ----------------------------------------------------------------- |
| `apiKey`                 | ✅        | —                                         | Your project's API key                                          |
| `serviceName`             | ✅        | —                                         | Name shown in the ObservAI dashboard                             |
| `endpointUrl`             |          | `http://localhost:8000/api/v1/sdk/ingest` | Where telemetry is sent                                          |
| `environment`             |          | `'production'`                            | Tag for filtering (e.g. `staging`, `production`)                 |
| `maxBatchSize`            |          | `100`                                     | Force a flush after this many buffered events                    |
| `flushIntervalMs`         |          | `5000`                                    | Max time between flushes                                         |
| `requestTimeoutMs`        |          | `8000`                                    | Network timeout per flush attempt                                |
| `maxBufferedItems`        |          | `5000`                                    | Caps total buffered telemetry; oldest items drop first if exceeded during a prolonged outage |
| `shutdownFlushTimeoutMs`  |          | `5000`                                    | Max time to spend flushing on `SIGINT`/`SIGTERM` before exiting   |

---

## How it works

```
Your app runs
     │
     ▼
Request/error happens → captured as an in-memory object (no disk writes)
     │
     ▼
Buffered with recent events
     │
     ▼
Every 5s or 100 events → single in-flight HTTP POST (JSON body, X-API-Key header)
     │
     ▼
ObservAI backend → stores it → AI agents investigate → RCA report
```

---

## Flush reliability

Under high-volume or bursty traffic, multiple things can try to trigger a flush at close to the same time — the 5-second timer, the 100-event threshold, and manual `flush()` calls. Earlier versions of this SDK could let those overlap into two concurrent network requests, which under certain timing could interact badly with the retry logic. This is fixed:

- **Single active flush.** Only one flush's network request is ever in flight. If something else asks for a flush while one is already running, it's queued to run immediately after the current one finishes — nothing is skipped, nothing runs concurrently.
- **Batch isolation.** The moment a flush starts, the events it's sending are separated from the live buffer. Any telemetry captured *while* that flush is in progress goes into a fresh buffer and is safely picked up by the next flush — never lost, never mixed into the batch already in flight.
- **Retry reliability.** Retries resend the exact batch that failed — not a mix of old and newly-arrived events. After 3 failed attempts, the batch is merged back into the live buffer to be retried on the next flush cycle, rather than silently dropped.
- **Bounded buffer.** If the backend is unreachable for an extended period, buffered telemetry is capped (`maxBufferedItems`, default 5000) so memory usage can't grow without limit — the oldest items are dropped first, since the newest telemetry is generally the most relevant for diagnosing what's happening right now.
- **Shutdown flushing.** On `SIGINT`/`SIGTERM`, the SDK gives buffered telemetry a bounded chance (`shutdownFlushTimeoutMs`, default 5s) to flush before the process exits, instead of silently discarding it.

**Known limitation:** if a request actually succeeds on the backend but the client-side timeout fires before the response arrives, the SDK currently has no way to know the data already landed and will retry, sending it again. Avoiding this fully would require an idempotency key that the backend also checks — out of scope for this SDK-only fix, since it would require a backend ingestion change. Worth considering for a future backend update.

### Testing shutdown manually

```
node example/app.js
```

Send some requests to generate telemetry, then press `Ctrl+C`. You should see the SDK attempt a final flush and the process exit promptly (well under `shutdownFlushTimeoutMs`).

---

## Tests

```
node test/reliability.test.js
```

Covers: payload shape and capture correctness, single-flight flush behavior under concurrent triggers, retry/backoff and re-queueing against an unreachable endpoint, buffer bounding during a simulated prolonged outage, and that the shutdown flush loop is time-bounded. No external test framework required.

## License

MIT