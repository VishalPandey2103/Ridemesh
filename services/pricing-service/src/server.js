import express from 'express';
import { createLogger, httpLogger } from '@ridemesh/shared/src/logger.js';
import { requireEnv } from '@ridemesh/shared/src/env.js';
import { ApiError, asyncHandler, errorHandler, notFound } from '@ridemesh/shared/src/errors.js';
import { createRedis } from '@ridemesh/shared/src/redis.js';
import { cellForLocation, minuteBucket } from '@ridemesh/shared/src/geo.js';
import { initMetrics } from '@ridemesh/shared/src/metrics.js';
import { computeFare } from './fare.js';

/**
 * Pricing Service — deterministic fare math + surge lookup.
 *
 * Surge inputs are gathered through Redis counters, with no direct coupling
 * between the services that produce them:
 *   demand: trip-service calls /internal/surge/observe on every ride request
 *           -> INCR demand:{cell}:{minuteBucket}
 *   supply: location-service SADDs each pinging driver into
 *           supply:{cell}:{minuteBucket} (a set => idempotent per driver)
 *
 * The multiplier itself is read from surge:{cell}, defaulting to 1.0x when
 * nothing has been written for that cell.
 */
const env = requireEnv(['PORT', 'REDIS_URL']);
const logger = createLogger('pricing-service');
const metrics = initMetrics('pricing-service');
const redis = createRedis(env.REDIS_URL);

const quotes = metrics.counter('fare_quotes_total', 'Fare quotes served');

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

app.listen(env.PORT, () => logger.info({ port: env.PORT }, 'pricing-service listening'));
