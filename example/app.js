const express = require('express');
const { ObserveAIClient } = require('../index');

const client = new ObserveAIClient({
  apiKey: 'test_api_key_123',
  serviceName: 'checkout-service',
  endpointUrl: 'http://localhost:8000/api/v1/sdk/ingest',
  environment: 'staging',
  flushIntervalMs: 2000,
});

const app = express();

app.use(client.expressMiddleware()); // auto-logs every request

app.get('/', (req, res) => res.send('App is running'));

app.get('/error', (req, res, next) => {
  next(new Error('Simulated database connection failure'));
});

app.use(client.expressErrorHandler()); // auto-captures errors
app.use((err, req, res, next) => res.status(500).json({ error: 'Internal server error' }));

app.listen(3000, () => console.log('Test app on :3000 — try / and /error'));
