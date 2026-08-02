// Mocks the REAL contract found in backend/api/v1/sdk.py so we can prove
// the SDK sends exactly what that endpoint expects, before your friend's
// actual Supabase-backed server is reachable.
const express = require('express');
const app = express();
app.use(express.json());

app.post('/api/v1/sdk/ingest', (req, res) => {
  const apiKeyHeader = req.headers['x-api-key'];
  const body = req.body;

  console.log('\n--- Mock backend received a request ---');
  console.log('X-API-Key header:', apiKeyHeader);
  console.log('Body keys:', Object.keys(body));
  console.log('service_name:', body.service_name);
  console.log('logs:', JSON.stringify(body.logs, null, 2));
  console.log('exceptions:', JSON.stringify(body.exceptions, null, 2));
  console.log('traces:', JSON.stringify(body.traces, null, 2));

  if (!apiKeyHeader && !body.api_key) {
    return res.status(401).json({ message: 'API Key required' });
  }

  // Mimic the real 202 Accepted response shape
  res.status(202).json({ message: 'Telemetry batch accepted and queued for analysis.', data: {} });
});

app.listen(8000, () => console.log('Mock ObservAI backend listening on :8000'));
