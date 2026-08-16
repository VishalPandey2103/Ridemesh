import { ApiError } from './errors.js';

/**
 * Circuit breaker for synchronous inter-service calls.
 *
 * Problem: if pricing-service hangs, every trip-completion request in
 * trip-service blocks a socket for the full timeout. Under load, those
 * blocked requests exhaust trip-service too — a cascading failure. The fix
 * is to fail FAST once a dependency is clearly unhealthy, and let callers
 * run their fallback (e.g. compute fare locally with default rates).
 *
 * State machine (per downstream service):
 *   CLOSED    normal; count consecutive failures.
 *   OPEN      after `failureThreshold` consecutive failures: reject
 *             immediately without touching the network for `resetTimeoutMs`.
 *   HALF_OPEN after the cooldown, let exactly one probe request through.
 *             Success -> CLOSED. Failure -> OPEN again.
 */
class CircuitBreaker {
  constructor({ failureThreshold = 5, resetTimeoutMs = 10000 } = {}) {
    this.failureThreshold = failureThreshold;
    this.resetTimeoutMs = resetTimeoutMs;
    this.failures = 0;
    this.state = 'CLOSED';
    this.openedAt = 0;
    this.probing = false;
  }

  canRequest() {
    if (this.state === 'CLOSED') return true;
    if (this.state === 'OPEN') {
      if (Date.now() - this.openedAt >= this.resetTimeoutMs) {
        this.state = 'HALF_OPEN';
        this.probing = false;
      } else {
        return false;
      }
    }
    // HALF_OPEN: admit a single probe
    if (this.probing) return false;
    this.probing = true;
    return true;
  }

  onSuccess() {
    this.failures = 0;
    this.state = 'CLOSED';
    this.probing = false;
  }

  onFailure() {
    this.failures += 1;
    this.probing = false;
    if (this.state === 'HALF_OPEN' || this.failures >= this.failureThreshold) {
      this.state = 'OPEN';
      this.openedAt = Date.now();
    }
  }
}

const breakers = new Map();
function breakerFor(name) {
  if (!breakers.has(name)) breakers.set(name, new CircuitBreaker());
  return breakers.get(name);
}

export class BreakerOpenError extends Error {
  constructor(name) {
    super(`Circuit open for ${name}`);
    this.breakerOpen = true;
  }
}

/**
 * callService('pricing', 'http://pricing-service:3005/internal/quote', {method, body, headers, timeoutMs})
 * Throws BreakerOpenError (fast-fail) or ApiError (downstream 4xx/5xx/timeout).
 */
export async function callService(name, url, { method = 'GET', body, headers = {}, timeoutMs = 5000 } = {}) {
  const breaker = breakerFor(name);
  if (!breaker.canRequest()) throw new BreakerOpenError(name);

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method,
      headers: { 'content-type': 'application/json', ...headers },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: ctrl.signal
    });
    const text = await res.text();
    const json = text ? JSON.parse(text) : null;
    if (!res.ok) {
      // 4xx = caller bug, not downstream sickness: don't trip the breaker.
      if (res.status >= 500) breaker.onFailure(); else breaker.onSuccess();
      throw new ApiError(res.status, json?.error || `${name} responded ${res.status}`, json?.details);
    }
    breaker.onSuccess();
    return json;
  } catch (e) {
    if (e instanceof ApiError) throw e;
    breaker.onFailure(); // network error / timeout
    throw new ApiError(503, `${name} unreachable: ${e.message}`);
  } finally {
    clearTimeout(timer);
  }
}
