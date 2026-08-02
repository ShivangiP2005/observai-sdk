const axios = require('axios');

/**
 * Node.js port of the backend's own reference client (backend/sdk/client.py).
 * Same buffering behavior, same payload shape, same header — built to match
 * the real /api/v1/sdk/ingest contract exactly, not guessed.
 */
class ObserveAIClient {
  constructor({
    apiKey,
    serviceName,
    endpointUrl = 'http://localhost:8000/api/v1/sdk/ingest',
    environment = 'production',
    maxBatchSize = 100,
    flushIntervalMs = 5000,
  }) {
    if (!apiKey) throw new Error('[observai-sdk] apiKey is required');
    if (!serviceName) throw new Error('[observai-sdk] serviceName is required');

    this.apiKey = apiKey;
    this.serviceName = serviceName;
    this.endpointUrl = endpointUrl;
    this.environment = environment;
    this.maxBatchSize = maxBatchSize;

    this._buffer = { logs: [], exceptions: [], traces: [], metrics: [], deployments: [] };

    // auto-flush on a timer too, so low-traffic apps don't sit on unsent data forever
    this._timer = setInterval(() => this.flush(), flushIntervalMs);
    this._timer.unref?.(); // don't keep the process alive just for this timer
  }

  captureLog(level, message, attributes = {}) {
    this._buffer.logs.push({ level: level.toUpperCase(), message, attributes });
    this._flushIfNeeded();
  }

  captureException(error, handled = false) {
    this._buffer.exceptions.push({
      exception_type: error.name || 'Error',
      message: error.message || String(error),
      stacktrace: error.stack || '',
      handled,
    });
    this._flushIfNeeded();
  }

  captureTrace({ traceId, spanId, parentSpanId, operationName, durationMs, statusCode = 200, attributes = {} }) {
    this._buffer.traces.push({
      trace_id: traceId,
      span_id: spanId,
      parent_span_id: parentSpanId,
      operation_name: operationName,
      duration_ms: durationMs,
      status_code: statusCode,
      attributes,
    });
    this._flushIfNeeded();
  }

  _flushIfNeeded() {
    const total = Object.values(this._buffer).reduce((sum, arr) => sum + arr.length, 0);
    if (total >= this.maxBatchSize) this.flush();
  }

  async flush(attempt = 1) {
    const hasData = Object.values(this._buffer).some((arr) => arr.length > 0);
    if (!hasData) return true;

    // snapshot + clear immediately so new events during the network call
    // go into a fresh buffer instead of being lost or duplicated
    const payload = {
      api_key: this.apiKey,
      service_name: this.serviceName,
      environment: this.environment,
      ...this._buffer,
    };
    this._clearBuffer();

    const maxAttempts = 3;
    try {
      const resp = await axios.post(this.endpointUrl, payload, {
        headers: { 'X-API-Key': this.apiKey },
        timeout: 5000,
      });
      if (resp.status === 202) return true;
      console.error('[observai-sdk] Unexpected status from backend:', resp.status);
      return false;
    } catch (err) {
      if (attempt < maxAttempts) {
        const backoffMs = 500 * 2 ** (attempt - 1); // 500ms, 1000ms, 2000ms
        console.warn(`[observai-sdk] Flush failed (attempt ${attempt}/${maxAttempts}), retrying in ${backoffMs}ms:`, err.message);
        await new Promise((r) => setTimeout(r, backoffMs));
        // put the data back so the retry actually resends it
        this._mergeBack(payload);
        return this.flush(attempt + 1);
      }
      console.error(`[observai-sdk] Flush failed after ${maxAttempts} attempts, dropping batch:`, err.message);
      return false;
    }
  }

  _mergeBack(payload) {
    for (const key of ['logs', 'exceptions', 'traces', 'metrics', 'deployments']) {
      this._buffer[key] = [...payload[key], ...this._buffer[key]];
    }
  }

  _clearBuffer() {
    this._buffer = { logs: [], exceptions: [], traces: [], metrics: [], deployments: [] };
  }

  // Express middleware: auto-captures every request as BOTH a log line and a trace span.
  // Also stamps a trace_id onto the request so DB calls made during this request
  // (via mongooseMiddleware) can be linked back to it.
  expressMiddleware() {
    return (req, res, next) => {
      const start = Date.now();
      const traceId = this._generateId(32);
      const spanId = this._generateId(16);
      req.observai = { traceId, spanId };

      res.on('finish', () => {
        const durationMs = Date.now() - start;
        this.captureLog(
          res.statusCode >= 500 ? 'ERROR' : 'INFO',
          `${req.method} ${req.originalUrl} -> ${res.statusCode}`,
          { duration_ms: durationMs, method: req.method, path: req.originalUrl, status: res.statusCode }
        );
        this.captureTrace({
          traceId,
          spanId,
          operationName: `${req.method} ${req.route ? req.route.path : req.path}`,
          durationMs,
          statusCode: res.statusCode,
          attributes: { 'http.method': req.method, 'http.path': req.originalUrl },
        });
      });
      next();
    };
  }

  _generateId(len) {
    let out = '';
    while (out.length < len) out += Math.random().toString(16).slice(2);
    return out.slice(0, len);
  }

  // Mongoose plugin: auto-captures every DB query as a child trace span,
  // linked to the current request's trace_id when available.
  mongooseMiddleware() {
    const self = this;
    return function plugin(schema) {
      schema.pre(/^find|save|update|delete|count/, function (next) {
        this._observaiStart = Date.now();
        next();
      });
      schema.post(/^find|save|update|delete|count/, function (_doc, next) {
        const durationMs = Date.now() - (this._observaiStart || Date.now());
        self.captureTrace({
          traceId: this._observaiTraceId || self._generateId(32),
          spanId: self._generateId(16),
          operationName: `mongodb.${this.op || 'query'} ${this.model ? this.model.collection.name : ''}`,
          durationMs,
          statusCode: 200,
          attributes: { 'db.system': 'mongodb', 'db.operation': this.op },
        });
        next();
      });
    };
  }

  // Express error handler: auto-captures unhandled errors as exceptions
  expressErrorHandler() {
    return (err, req, res, next) => {
      this.captureException(err, false);
      next(err);
    };
  }
}

module.exports = { ObserveAIClient };
