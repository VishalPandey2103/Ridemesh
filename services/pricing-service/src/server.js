import express from 'express';
import { createLogger, httpLogger } from '@ridemesh/shared/src/logger.js';
import { requireEnv, envInt } from '@ridemesh/shared/src/env.js';
import { ApiError, asyncHandler, errorHandler, notFound } from '@ridemesh/shared/src/errors.js';
import { createRedis } from '@ridemesh/shared/src/redis.js';
import { cellForLocation, minuteBucket } from '@ridemesh/shared/src/geo.js';
import { initMetrics } from '@ridemesh/shared/src/metrics.js';
import { computeFare, surgeFromRatio, DEFAULT_RATES } from './fare.js';

/**
 * Pricing Service — deterministic fare math + background surge computation.
 *
 * Surge pipeline (choreographed through Redis counters, no direct coupling):
 *   demand: trip-service calls /internal/surge/observe on every ride request
 *           -> INCR demand:{cell}:{minuteBucket}
 *   supply: location-service SADDs each pinging driver into
 *           supply:{cell}:{minuteBucket} (set => idempotent per driver)
 *   worker: every 60s, for each cell active in the JUST-CLOSED bucket,
 *           multiplier = f(demand/supply), capped at maxSurge, written to
 *           surge:{cell} with a 180s TTL. The TTL is the safety valve: if
 *           the worker dies, stale surge decays to 1.0x within 3 minutes
 *           instead of overcharging a whole city indefinitely.
 *
 * Why compute over the CLOSED bucket (previous minute) instead of the live
 * one: the live bucket is half-filled — reading it mid-minute would
 * systematically underestimate both demand and supply and make surge jitter.
 */
const env = requireEnv(['PORT', 'REDIS_URL']);
const logger = createLogger('pricing-service');
const metrics = initMetrics('pricing-service');
const redis = createRedis(env.REDIS_URL);

const surgeGauge = metrics.gauge('surge_multiplier', 'Current surge multiplier per cell', ['cell']);
const quotes = metrics.counter('fare_quotes_total', 'Fare quotes served');

const SURGE_TTL_S = 180;
const WORKER_INTERVAL_MS = envInt('SURGE_INTERVAL_MS', 60000);

async function currentSurge(cell) {
  const raw = await redis.get(`surge:${cell}`);
  return raw ? parseFloat(raw) : 1.0;
}

const app = express();
app.use(express.json());
app.use(httpLogger(logger));
app.use(metrics.middleware);
app.get('/metrics', metrics.metricsHandler);
app.get('/health', (_req, res) => res.json({ ok: true, service: 'pricing-service' }));

// Fare quote: same math for the upfront estimate and the final bill —
// determinism means trip-service can re-derive and audit any fare.
app.post('/internal/quote', asyncHandler(async (req, res) => {
  const { distanceM, durationS, surge } = req.body || {};
  if (!Number.isFinite(distanceM) || !Number.isFinite(durationS)) {
    throw new ApiError(400, 'distanceM and durationS required');
  }
  quotes.inc({ service: metrics.service });
  res.json(computeFare({ distanceM, durationS, surge }));
}));

// Read + record demand. Called once per ride request by trip-service.
app.post('/internal/surge/observe', asyncHandler(async (req, res) => {
  const { lat, lng } = req.body || {};
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) throw new ApiError(400, 'lat,lng required');
  const cell = cellForLocation(lat, lng);
  const bucket = minuteBucket();
  await redis.incr(`demand:${cell}:${bucket}`);
  await redis.expire(`demand:${cell}:${bucket}`, 180);
  await redis.sadd(`surge:cells:${bucket}`, cell);
  await redis.expire(`surge:cells:${bucket}`, 180);
  res.json({ cell, multiplier: await currentSurge(cell) });
}));

// Read-only surge lookup (rider app preview).
app.get('/api/surge', asyncHandler(async (req, res) => {
  const lat = parseFloat(req.query.lat), lng = parseFloat(req.query.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) throw new ApiError(400, 'lat,lng required');
  const cell = cellForLocation(lat, lng);
  res.json({ cell, multiplier: await currentSurge(cell) });
}));

app.use(notFound);
app.use(errorHandler(logger));

// ---------------------------------------------------------- surge worker ---
async function runSurgeTick() {
  const closedBucket = minuteBucket() - 1;
  const cells = await redis.smembers(`surge:cells:${closedBucket}`);
  for (const cell of cells) {
    const [demandRaw, supply] = await Promise.all([
      redis.get(`demand:${cell}:${closedBucket}`),
      redis.scard(`supply:${cell}:${closedBucket}`)
    ]);
    const demand = parseInt(demandRaw || '0', 10);
    const multiplier = surgeFromRatio(demand, supply, DEFAULT_RATES.maxSurge);
    await redis.set(`surge:${cell}`, String(multiplier), 'EX', SURGE_TTL_S);
    surgeGauge.set({ service: metrics.service, cell }, multiplier);
    if (multiplier > 1) logger.info({ cell, demand, supply, multiplier }, 'surge updated');
  }
}
setInterval(() => runSurgeTick().catch((e) => logger.error({ err: e.message }, 'surge tick failed')),
  WORKER_INTERVAL_MS).unref();

app.listen(env.PORT, () => logger.info({ port: env.PORT }, 'pricing-service listening'));
