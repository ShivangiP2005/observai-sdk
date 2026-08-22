const axios = require('axios');
const { AsyncLocalStorage } = require('async_hooks');

/**
 * Node.js port of the backend's own reference client (backend/sdk/client.py).
 * Same buffering behavior, same payload shape, same header — built to match
 * the real /api/v1/sdk/ingest contract exactly, not guessed.
 *
 * Reliability notes (fixed in this version):
 * - Only ONE network flush is ever in flight at a time. If a flush is
 *   triggered (timer / batch-size / manual) while another is still running,
 *   it no longer starts a second concurrent request — it just marks that
 *   another flush should run immediately after the current one finishes.
 * - Retries operate on an isolated snapshot of the batch that failed, not
 *   the live buffer, so a retry can never accidentally pick up unrelated
 *   events that arrived in the meantime (and never duplicates them either).
 * - New telemetry that arrives while a flush is in progress goes into a
 *   fresh buffer and is safely picked up by the next flush — nothing is
 *   lost and nothing is sent twice.
 * - The buffer is bounded (maxBufferedItems). If the backend is down for a
 *   long stretch and failed batches keep re-queuing, the oldest telemetry
 *   is dropped (with a warning) once the cap is hit, instead of letting
 *   memory grow without limit.
 * - On process shutdown (SIGINT/SIGTERM), the SDK loops flush attempts
 *   until the buffer is empty and no flush is pending, bounded by an
 *   overall deadline — so a flush-triggered-during-shutdown-flush can't
 *   quietly get skipped, and the process still can't hang forever.
 *
 * Trace propagation notes (added in this version):
 * - expressMiddleware() runs the rest of the request (next(), and
 *   everything it triggers — sync or async, however many layers deep)
 *   inside an AsyncLocalStorage context holding that request's
 *   { traceId, spanId }. This is Node's built-in mechanism for exactly
 *   this problem: the context correctly survives promises, async/await,
 *   timers, and most callback-based async work, without needing `req` to
 *   be threaded through every controller/service/model function manually.
 * - mongooseMiddleware() reads that active context (if any) via
 *   this._httpContext.getStore(). If a Mongoose operation runs while an
 *   HTTP request is active, it reuses that request's traceId and records
 *   the HTTP span as its parent — a proper child span, not a new root
 *   trace. If no HTTP request is active (e.g. a background job, a script,
 *   startup code), it falls back to generating its own root trace, exactly
 *   as before — standalone Mongoose usage is unaffected.
 */
class ObserveAIClient {
  constructor({
    apiKey,
    serviceName,
    endpointUrl = 'http://localhost:8000/api/v1/sdk/ingest',
    environment = 'production',
    maxBatchSize = 100,
    flushIntervalMs = 5000,
    requestTimeoutMs = 8000, // kept above flushIntervalMs on purpose — see README note below
    maxBufferedItems = 5000, // safety cap across logs+exceptions+traces+metrics+deployments combined
    shutdownFlushTimeoutMs = 5000,
  }) {
    if (!apiKey) throw new Error('[observai-sdk] apiKey is required');
    if (!serviceName) throw new Error('[observai-sdk] serviceName is required');

    this.apiKey = apiKey;
    this.serviceName = serviceName;
    this.endpointUrl = endpointUrl;
    this.environment = environment;
    this.maxBatchSize = maxBatchSize;
    this.requestTimeoutMs = requestTimeoutMs;
    this.maxBufferedItems = maxBufferedItems;
    this.shutdownFlushTimeoutMs = shutdownFlushTimeoutMs;

    this._buffer = { logs: [], exceptions: [], traces: [], metrics: [], deployments: [] };

    // Holds the currently-active HTTP request's { traceId, spanId } for the
    // lifetime of that request's async call chain (see expressMiddleware
    // and mongooseMiddleware below, and the class doc above).
    this._httpContext = new AsyncLocalStorage();

    // --- concurrency control -------------------------------------------
    // _flushInFlight: the Promise of the currently-running flush, or null.
    // _flushAgain: true if a new flush was requested while one was already
    //              running — we honor it right after the current one ends,
    //              instead of firing a second overlapping network request.
    this._flushInFlight = null;
    this._flushAgain = false;
    this._shuttingDown = false;

    // auto-flush on a timer too, so low-traffic apps don't sit on unsent data forever
    this._timer = setInterval(() => {
      this.flush().catch((err) => {
        // flush() already logs failures internally; this catch just stops
        // an unhandled rejection from crashing the host app on a bad tick.
        console.error('[observai-sdk] Unexpected error during scheduled flush:', err.message);
      });
    }, flushIntervalMs);
    this._timer.unref?.(); // don't keep the process alive just for this timer

    // --- graceful shutdown ----------------------------------------------
    // Give the app a real chance to flush pending telemetry on exit,
    // bounded so it can never hang the shutdown indefinitely.
    this._shutdownHandler = () => this._handleShutdown();
    process.on('SIGINT', this._shutdownHandler);
    process.on('SIGTERM', this._shutdownHandler);
  }

  captureLog(level, message, attributes = {}) {
    this._buffer.logs.push({ level: level.toUpperCase(), message, attributes });
    this._trimBufferIfNeeded();
    this._flushIfNeeded();
  }

  captureException(error, handled = false) {
    this._buffer.exceptions.push({
      exception_type: error.name || 'Error',
      message: error.message || String(error),
      stacktrace: error.stack || '',
      handled,
    });
    this._trimBufferIfNeeded();
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
    this._trimBufferIfNeeded();
    this._flushIfNeeded();
  }

  _flushIfNeeded() {
    const total = this._totalBufferedCount();
    if (total >= this.maxBatchSize) {
      this.flush().catch((err) => {
        console.error('[observai-sdk] Unexpected error during threshold flush:', err.message);
      });
    }
  }

  _totalBufferedCount() {
    return Object.values(this._buffer).reduce((sum, arr) => sum + arr.length, 0);
  }

  /**
   * Keeps total buffered telemetry bounded. If the backend is unreachable
   * for a long stretch, failed batches keep re-queuing (see _sendWithRetry)
   * and new events keep arriving — without a cap this would grow memory
   * usage without limit. Once over the cap, the OLDEST items are dropped
   * first (across all telemetry types), since the newest data is generally
   * the most relevant for diagnosing what's happening right now.
   */
  _trimBufferIfNeeded() {
    let total = this._totalBufferedCount();
    if (total <= this.maxBufferedItems) return;

    let toDrop = total - this.maxBufferedItems;
    console.warn(
      `[observai-sdk] Buffered telemetry exceeded ${this.maxBufferedItems} items ` +
        `(backend likely unreachable for a while) — dropping ${toDrop} oldest item(s) to bound memory usage.`
    );

    for (const key of ['logs', 'exceptions', 'traces', 'metrics', 'deployments']) {
      if (toDrop <= 0) break;
      const arr = this._buffer[key];
      const dropFromThis = Math.min(toDrop, arr.length);
      if (dropFromThis > 0) arr.splice(0, dropFromThis); // drop oldest (front = earliest pushed)
      toDrop -= dropFromThis;
    }
  }

  /**
   * Public entry point. Safe to call from anywhere (timer, threshold,
   * manually) at any time — it will never cause two network requests to be
   * in flight simultaneously.
   */
  async flush() {
    // A flush is already running: don't start a second one. Just remember
    // that we should flush again right after this one finishes, so any
    // events sitting in the buffer right now still go out promptly.
    if (this._flushInFlight) {
      this._flushAgain = true;
      return this._flushInFlight;
    }

    this._flushInFlight = this._runFlush().finally(() => {
      this._flushInFlight = null;
      if (this._flushAgain) {
        this._flushAgain = false;
        // Fire-and-forget: don't await inside finally, just kick it off.
        // Any caller awaiting *this* flush() call already got their result;
        // this follow-up run is what the "flush requested during a flush"
        // case is for. (Shutdown deliberately loops on this — see
        // _handleShutdown — so this follow-up can never be silently
        // skipped during exit.)
        this.flush().catch((err) => {
          console.error('[observai-sdk] Unexpected error during follow-up flush:', err.message);
        });
      }
    });

    return this._flushInFlight;
  }

  /**
   * Does the actual work for a single flush cycle: snapshot the current
   * buffer, clear it immediately (so new telemetry has somewhere safe to
   * go), then send that exact snapshot with retries. Retries only ever
   * touch the isolated `batch` snapshot — never the live `this._buffer` —
   * so a retry can't pick up unrelated new events, and new events can't be
   * accidentally sent twice.
   */
  async _runFlush() {
    const hasData = Object.values(this._buffer).some((arr) => arr.length > 0);
    if (!hasData) return true;

    const batch = this._buffer;
    this._clearBuffer();

    return this._sendWithRetry(batch, 1);
  }

  async _sendWithRetry(batch, attempt) {
    const maxAttempts = 3;
    const payload = {
      api_key: this.apiKey,
      service_name: this.serviceName,
      environment: this.environment,
      ...batch,
    };

    try {
      const resp = await axios.post(this.endpointUrl, payload, {
        headers: { 'X-API-Key': this.apiKey },
        timeout: this.requestTimeoutMs,
      });
      if (resp.status === 202) return true;
      console.error('[observai-sdk] Unexpected status from backend:', resp.status);
      return false;
    } catch (err) {
      if (attempt < maxAttempts) {
        const backoffMs = 500 * 2 ** (attempt - 1); // 500ms, 1000ms, 2000ms
        console.warn(
          `[observai-sdk] Flush failed (attempt ${attempt}/${maxAttempts}), retrying in ${backoffMs}ms:`,
          err.message
        );
        await new Promise((r) => setTimeout(r, backoffMs));
        // Retry the SAME batch snapshot — do not touch this._buffer here.
        return this._sendWithRetry(batch, attempt + 1);
      }

      // Out of retries. Per the reliability requirement, don't just drop
      // the data — merge it back into the live buffer so the next
      // scheduled/threshold/manual flush picks it up and tries again.
      // (Bounded by _trimBufferIfNeeded, so a long outage can't cause
      // unbounded memory growth.)
      console.error(
        `[observai-sdk] Flush failed after ${maxAttempts} attempts, re-queuing batch for next flush:`,
        err.message
      );
      this._mergeBack(batch);
      return false;
    }
  }

  _mergeBack(batch) {
    for (const key of ['logs', 'exceptions', 'traces', 'metrics', 'deployments']) {
      this._buffer[key] = [...batch[key], ...this._buffer[key]];
    }
    this._trimBufferIfNeeded();
  }

  _clearBuffer() {
    this._buffer = { logs: [], exceptions: [], traces: [], metrics: [], deployments: [] };
  }

  /**
   * Called on SIGINT/SIGTERM. Loops flush attempts until the buffer is
   * empty AND no flush is in flight or pending, bounded by an overall
   * deadline — so a flush that gets requested WHILE the shutdown flush is
   * running (the "flush again" case) still gets a chance to go out,
   * instead of the process exiting right underneath it. Still guaranteed
   * to exit within shutdownFlushTimeoutMs even if the backend is
   * completely unreachable.
   */
  async _handleShutdown() {
    if (this._shuttingDown) return; // avoid double-handling a second signal
    this._shuttingDown = true;

    const deadline = Date.now() + this.shutdownFlushTimeoutMs;
    try {
      // Keep flushing as long as there's something to send / in flight /
      // queued as a follow-up, or until we run out of time — whichever
      // comes first.
      while (Date.now() < deadline) {
        const stillHasWork = this._totalBufferedCount() > 0 || this._flushInFlight || this._flushAgain;
        if (!stillHasWork) break;

        const remainingMs = Math.max(deadline - Date.now(), 0);
        await Promise.race([this.flush(), new Promise((resolve) => setTimeout(resolve, remainingMs))]);
      }
    } catch (err) {
      console.error('[observai-sdk] Error flushing telemetry during shutdown:', err.message);
    } finally {
      this.destroy();
      process.exit(0);
    }
  }

  /**
   * Stops the background timer and removes shutdown listeners. Call this
   * if you're tearing the client down manually (e.g. in tests) without
   * exiting the process.
   */
  destroy() {
    clearInterval(this._timer);
    process.removeListener('SIGINT', this._shutdownHandler);
    process.removeListener('SIGTERM', this._shutdownHandler);
  }

  // Express middleware: auto-captures every request as BOTH a log line and a trace span.
  // Also runs the rest of the request inside an AsyncLocalStorage context so any
  // Mongoose query triggered during this request — however deep — can correctly
  // attach itself as a child span of this request's trace (see mongooseMiddleware).
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

      this._httpContext.run({ traceId, spanId }, () => {
        next();
      });
    };
  }

  _generateId(len) {
    let out = '';
    while (out.length < len) out += Math.random().toString(16).slice(2);
    return out.slice(0, len);
  }

  // Mongoose plugin: auto-captures every DB query as a child trace span.
  // If it runs inside an active HTTP request (per _httpContext, set up in
  // expressMiddleware above), it inherits that request's traceId and is
  // recorded as a child of the HTTP span — a correlated trace, not a new
  // root. Outside an HTTP request, it falls back to generating its own
  // root trace, same as before.
  mongooseMiddleware() {
    const self = this;
    return function plugin(schema) {
      schema.pre(/^find|save|update|delete|count/, function (next) {
        this._observaiStart = Date.now();
        next();
      });
      schema.post(/^find|save|update|delete|count/, function (_doc, next) {
        const durationMs = Date.now() - (this._observaiStart || Date.now());
        const activeHttpContext = self._httpContext.getStore();

        self.captureTrace({
          traceId: activeHttpContext ? activeHttpContext.traceId : self._generateId(32),
          spanId: self._generateId(16),
          parentSpanId: activeHttpContext ? activeHttpContext.spanId : undefined,
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