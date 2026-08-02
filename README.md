# observai-sdk

Lightweight Node.js SDK for [ObservAI] — an AI-powered observability platform that automatically detects production incidents and generates Root Cause Analysis reports.

Add it to any Express app in 3 lines. No changes to your existing routes.

```javascript
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
- 🔁 **Retry with backoff** — 3 attempts (500ms → 1s → 2s) before a batch is dropped, so a network blip doesn't silently lose data
- 🪶 **Zero disk writes** — everything lives in memory and streams out over HTTP; no log files to manage or rotate

---

## Install

```bash
npm install github:YOUR_USERNAME/observai-sdk
```
(Will become `npm install observai-sdk` once published to the public npm registry.)

---

## Quick Start

```javascript
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

```javascript
const mongoose = require('mongoose');
mongoose.plugin(client.mongooseMiddleware());   // add BEFORE defining your schemas
```

### Manual capture

```javascript
client.captureLog('WARN', 'Payment retry attempted', { orderId: 123 });
client.captureException(someError, /* handled */ true);
```

---

## Configuration

| Option | Required | Default | Description |
|---|---|---|---|
| `apiKey` | ✅ | — | Your project's API key |
| `serviceName` | ✅ | — | Name shown in the ObservAI dashboard |
| `endpointUrl` | | `http://localhost:8000/api/v1/sdk/ingest` | Where telemetry is sent |
| `environment` | | `'production'` | Tag for filtering (e.g. `staging`, `production`) |
| `maxBatchSize` | | `100` | Force a flush after this many buffered events |
| `flushIntervalMs` | | `5000` | Max time between flushes |

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
Every 5s or 100 events → sent as one HTTP POST (JSON body, X-API-Key header)
     │
     ▼
ObservAI backend → stores it → AI agents investigate → RCA report
```

---

## Status

| Feature | Status |
|---|---|
| Express request/error capture | ✅ Tested end-to-end |
| Retry with backoff | ✅ Tested (verified against a killed connection) |
| Mongoose query tracing | ⚠️ Implemented, not yet verified against a live MongoDB instance |

---

## License

MIT
