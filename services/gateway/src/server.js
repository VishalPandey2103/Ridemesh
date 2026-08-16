import express from 'express';
import { createLogger, httpLogger } from '@ridemesh/shared/src/logger.js';
import { requireEnv } from '@ridemesh/shared/src/env.js';
import { verifyToken } from '@ridemesh/shared/src/auth.js';
import { initMetrics } from '@ridemesh/shared/src/metrics.js';

/**
 * API Gateway — the single public HTTP entrypoint.
 *
 * Responsibilities (and deliberately nothing more — a gateway that grows
 * business logic becomes a distributed monolith's God object):
 *   1. AuthN once at the edge: verify the JWT, then inject x-user-id /
 *      x-user-role headers. Downstream services trust those headers instead
 *      of re-verifying tokens (verify-once pattern; in prod the internal
 *      network / mTLS enforces that only the gateway can reach services).
 *   2. Routing by path prefix to the owning service.
 *   3. Request-id generation so one ride is greppable across all services.
 *
 * /internal/* routes are NOT proxied — those are service-to-service only.
 */
const env = requireEnv(['PORT', 'JWT_SECRET',
  'USER_SERVICE_URL', 'TRIP_SERVICE_URL', 'PAYMENT_SERVICE_URL', 'LOCATION_SERVICE_URL']);

const logger = createLogger('gateway');
const metrics = initMetrics('gateway');

// Routing table: first matching prefix wins.
const ROUTES = [
  { prefix: '/api/auth', target: env.USER_SERVICE_URL, public: true },
  { prefix: '/api/users', target: env.USER_SERVICE_URL },
  { prefix: '/api/drivers/nearby', target: env.LOCATION_SERVICE_URL },
  { prefix: '/api/trips', target: env.TRIP_SERVICE_URL },
  { prefix: '/api/payments', target: env.PAYMENT_SERVICE_URL }
];

const app = express();
app.use(httpLogger(logger));
app.use(metrics.middleware);
app.get('/metrics', metrics.metricsHandler);
app.get('/health', (_req, res) => res.json({ ok: true, service: 'gateway' }));

// Raw body passthrough: the gateway must not parse/re-serialize JSON it only
// forwards (parsing costs CPU and can subtly mutate payloads).
app.use(express.raw({ type: '*/*', limit: '1mb' }));

app.use(async (req, res) => {
  const route = ROUTES.find((r) => req.path.startsWith(r.prefix));
  if (!route) return res.status(404).json({ error: `No upstream for ${req.path}` });

  // --- authentication ---
  let user = null;
  if (!route.public) {
    const auth = req.headers.authorization || '';
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing bearer token' });
    try {
      const claims = verifyToken(token);
      user = { id: claims.sub, role: claims.role };
    } catch {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }
  }

  // --- proxy ---
  const targetUrl = route.target + req.originalUrl;
  const headers = {
    'content-type': req.headers['content-type'] || 'application/json',
    'x-request-id': req.id
  };
  if (user) {
    headers['x-user-id'] = user.id;
    headers['x-user-role'] = user.role;
  }
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 10000);
  try {
    const upstream = await fetch(targetUrl, {
      method: req.method,
      headers,
      // express.raw yields {} (not a Buffer) when a request has no body —
      // forward only a real, non-empty Buffer or fetch would send junk.
      body: ['GET', 'HEAD'].includes(req.method) || !Buffer.isBuffer(req.body) || !req.body.length
        ? undefined : req.body,
      signal: ctrl.signal
    });
    res.status(upstream.status);
    const ct = upstream.headers.get('content-type');
    if (ct) res.setHeader('content-type', ct);
    res.send(Buffer.from(await upstream.arrayBuffer()));
  } catch (e) {
    logger.error({ reqId: req.id, targetUrl, err: e.message }, 'upstream failed');
    res.status(502).json({ error: 'Upstream service unavailable' });
  } finally {
    clearTimeout(timer);
  }
});

app.listen(env.PORT, () => logger.info({ port: env.PORT }, 'gateway listening'));
