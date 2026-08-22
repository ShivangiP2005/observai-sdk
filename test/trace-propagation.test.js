const assert = require('assert');
const { ObserveAIClient } = require('../index');

async function testTraceCorrelation() {
  const capturedTraces = [];
  const client = new ObserveAIClient({
    apiKey: 'k',
    serviceName: 's',
    flushIntervalMs: 100000,
  });
  // Intercept captureTrace instead of hitting a real network endpoint
  const originalCaptureTrace = client.captureTrace.bind(client);
  client.captureTrace = (args) => {
    capturedTraces.push(args);
    return originalCaptureTrace(args);
  };

  // Simulate registering the Mongoose plugin (no real mongoose/DB needed —
  // just call the plugin the same way mongoose.plugin() would).
  const fakeSchema = { pre: (_re, fn) => (fakeSchema._pre = fn), post: (_re, fn) => (fakeSchema._post = fn) };
  client.mongooseMiddleware()(fakeSchema);

  async function simulateMongooseQuery(op) {
    const fakeQueryThis = { op, model: { collection: { name: 'orders' } } };
    await new Promise((resolve) => fakeSchema._pre.call(fakeQueryThis, resolve));
    // simulate a few real async hops deep, like controller -> service -> model
    await Promise.resolve();
    await new Promise((r) => setTimeout(r, 5));
    await new Promise((resolve) => fakeSchema._post.call(fakeQueryThis, {}, resolve));
  }

  // --- Scenario A: Mongoose query INSIDE an active HTTP request ------------
  const fakeReq = {};
  const fakeRes = { on: () => {}, statusCode: 200 };
  let httpTraceId, httpSpanId;

  await new Promise((resolveMiddleware) => {
    const middleware = client.expressMiddleware();
    middleware(fakeReq, fakeRes, async () => {
      httpTraceId = fakeReq.observai.traceId;
      httpSpanId = fakeReq.observai.spanId;

      // controller -> service -> mongoose, several async hops deep
      await Promise.resolve();
      async function controller() {
        await service();
      }
      async function service() {
        await simulateMongooseQuery('find');
      }
      await controller();

      resolveMiddleware();
    });
  });

  const mongoTraceInsideRequest = capturedTraces.find((t) => t.operationName.startsWith('mongodb.'));

  assert.ok(mongoTraceInsideRequest, 'expected a mongodb trace to be captured');
  assert.strictEqual(
    mongoTraceInsideRequest.traceId,
    httpTraceId,
    `Mongo query should inherit the HTTP request's traceId. Got ${mongoTraceInsideRequest.traceId}, expected ${httpTraceId}`
  );
  assert.strictEqual(
    mongoTraceInsideRequest.parentSpanId,
    httpSpanId,
    `Mongo query should record the HTTP span as its parent. Got ${mongoTraceInsideRequest.parentSpanId}, expected ${httpSpanId}`
  );
  assert.notStrictEqual(
    mongoTraceInsideRequest.spanId,
    httpSpanId,
    'Mongo query should have its OWN span id (a child span), not reuse the HTTP span id'
  );

  // --- Scenario B: Mongoose query OUTSIDE any HTTP request ------------------
  capturedTraces.length = 0;
  await simulateMongooseQuery('find');

  const mongoTraceStandalone = capturedTraces.find((t) => t.operationName.startsWith('mongodb.'));
  assert.ok(mongoTraceStandalone, 'expected a mongodb trace to be captured for standalone query');
  assert.strictEqual(
    mongoTraceStandalone.parentSpanId,
    undefined,
    'Standalone Mongo query (no active HTTP request) should have no parent — it is its own root trace'
  );
  assert.notStrictEqual(
    mongoTraceStandalone.traceId,
    httpTraceId,
    'Standalone Mongo query should NOT reuse a trace id from an earlier, unrelated HTTP request'
  );

  // --- Scenario C: two concurrent HTTP requests don't cross-contaminate ----
  capturedTraces.length = 0;
  const req1 = {};
  const req2 = {};
  const res = { on: () => {}, statusCode: 200 };

  await Promise.all([
    new Promise((resolve) => {
      client.expressMiddleware()(req1, res, async () => {
        await new Promise((r) => setTimeout(r, 10));
        await simulateMongooseQuery('find');
        resolve();
      });
    }),
    new Promise((resolve) => {
      client.expressMiddleware()(req2, res, async () => {
        await new Promise((r) => setTimeout(r, 3));
        await simulateMongooseQuery('save');
        resolve();
      });
    }),
  ]);

  const mongoTraces = capturedTraces.filter((t) => t.operationName.startsWith('mongodb.'));
  assert.strictEqual(mongoTraces.length, 2, 'expected one mongo trace per concurrent request');
  const trace1 = mongoTraces.find((t) => t.operationName.includes('find'));
  const trace2 = mongoTraces.find((t) => t.operationName.includes('save'));
  assert.strictEqual(trace1.traceId, req1.observai.traceId, "request 1's mongo query should match request 1's trace");
  assert.strictEqual(trace2.traceId, req2.observai.traceId, "request 2's mongo query should match request 2's trace");
  assert.notStrictEqual(
    trace1.traceId,
    trace2.traceId,
    'concurrent requests must not cross-contaminate trace ids'
  );

  client.destroy();
  console.log('✓ Mongo query inside HTTP request inherits traceId and correct parent span');
  console.log('✓ Mongo query outside HTTP request falls back to its own root trace (unaffected)');
  console.log('✓ Concurrent HTTP requests do not cross-contaminate trace context');
  console.log('\nAll trace propagation tests passed.');
}

testTraceCorrelation().catch((err) => {
  console.error('✗ FAILED:', err.message);
  process.exit(1);
});