# observai-sdk (v0.2 — matches the real backend contract)

This version was built by reading the ACTUAL backend code in your friend's repo
(`backend/api/v1/sdk.py` and `backend/schemas/telemetry.py`), not guessed.

## Real contract confirmed
- URL: `POST http://localhost:8000/api/v1/sdk/ingest` (change host once deployed)
- Header: `X-API-Key: <your_api_key>`
- Body: `{ api_key, service_name, environment, logs[], exceptions[], traces[], metrics[], deployments[] }`
- Success: HTTP `202`

## Test it yourself (proves it works before touching the real dummy site)

```bash
npm install
cd example && npm install
node mock-backend.js     # terminal 1 — simulates the real endpoint
node app.js               # terminal 2 — your test app using the SDK
```
Visit `http://localhost:3000/` and `http://localhost:3000/error` — watch the mock
backend terminal print the exact payload it received.

## Wire into your real dummy MERN site

```javascript
const { ObserveAIClient } = require('observai-sdk');

const client = new ObserveAIClient({
  apiKey: 'YOUR_REAL_API_KEY',       // get from your friend / the dashboard
  serviceName: 'my-dummy-store',
  endpointUrl: 'http://localhost:8000/api/v1/sdk/ingest',  // or deployed URL
  environment: 'staging',
});

app.use(client.expressMiddleware());   // auto-logs every request
// ... your existing routes, unchanged ...
app.use(client.expressErrorHandler()); // auto-captures errors
```
Three lines added to your existing app. No route code changes.

## Manual capture (for anything not auto-covered)
```javascript
client.captureLog('WARN', 'Payment retry attempted', { orderId: 123 });
client.captureException(someError, false);
```

## MongoDB / Mongoose auto-tracing

Every schema gets automatic query tracing (find, save, update, delete, count):
```javascript
const mongoose = require('mongoose');
mongoose.plugin(client.mongooseMiddleware());   // add BEFORE defining your models
```

## What's tested vs. not yet tested

- ✅ Express request/error auto-capture — tested end-to-end against a mock server
- ✅ Retry with backoff on flush failure — tested by killing the backend mid-flight
- ⚠️ Mongoose auto-tracing — written the same way as the working Express tracing,
  but NOT yet verified against a real MongoDB connection. Test this yourself once
  wired into the real site, and report back if the trace doesn't appear.
