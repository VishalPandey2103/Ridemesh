import http from 'node:http';
import express from 'express';
import { createLogger, httpLogger } from '@ridemesh/shared/src/logger.js';
import { requireEnv } from '@ridemesh/shared/src/env.js';
import { ApiError, asyncHandler, errorHandler, notFound } from '@ridemesh/shared/src/errors.js';
import { userFromHeaders } from '@ridemesh/shared/src/auth.js';
import { initMetrics } from '@ridemesh/shared/src/metrics.js';
import { GeoIndex } from './geo.js';
import { attachSockets } from './sockets.js';

/**
 * Driver Location Service — owns "where is every online driver right now".
 * Hosts both the HTTP query API and the Socket.IO real-time layer (the WS
 * layer lives here because pings ARE location data; scaling this service
 * horizontally scales both, thanks to the Redis adapter).
 */
const env = requireEnv(['PORT', 'JWT_SECRET', 'REDIS_URL', 'GEO_REDIS_URLS',
  'TRIP_SERVICE_URL', 'MATCHING_SERVICE_URL']);
const logger = createLogger('location-service');
const metrics = initMetrics('location-service');
const geo = new GeoIndex({
  geoUrls: env.GEO_REDIS_URLS.split(',').map((s) => s.trim()).filter(Boolean),
  metaUrl: env.REDIS_URL
});

const app = express();
app.use(express.json());
app.use(httpLogger(logger));
app.use(metrics.middleware);
app.use(userFromHeaders);
app.get('/metrics', metrics.metricsHandler);
app.get('/health', asyncHandler(async (_req, res) =>
  res.json({ ok: true, service: 'location-service', onlineDrivers: await geo.onlineCount() })));

// Public (via gateway): riders can see supply around them before requesting.
app.get('/api/drivers/nearby', asyncHandler(async (req, res) => {
  const lat = parseFloat(req.query.lat), lng = parseFloat(req.query.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) throw new ApiError(400, 'lat,lng required');
  const radius = Math.min(parseInt(req.query.radius || '3000', 10), 10000);
  const drivers = await geo.findNearby(lat, lng, radius, 10);
  // Riders get positions, not identities.
  res.json({ count: drivers.length, drivers: drivers.map(({ driverId, ...rest }) => rest) });
}));

// Internal: matching-service candidate discovery (identities included).
app.get('/internal/nearby', asyncHandler(async (req, res) => {
  const lat = parseFloat(req.query.lat), lng = parseFloat(req.query.lng);
  const radius = parseInt(req.query.radius || '3000', 10);
  const limit = parseInt(req.query.limit || '8', 10);
  res.json({ candidates: await geo.findNearby(lat, lng, radius, limit) });
}));

// Internal: matching/trip services drive driver availability transitions.
app.post('/internal/drivers/:id/status', asyncHandler(async (req, res) => {
  const { status, tripId } = req.body || {};
  if (!['online', 'on_trip', 'offline'].includes(status)) throw new ApiError(400, 'bad status');
  await geo.setStatus(req.params.id, status, { tripId });
  res.json({ ok: true });
}));

app.use(notFound);
app.use(errorHandler(logger));

const server = http.createServer(app);
attachSockets({ httpServer: server, geo, env, logger, metrics });
server.listen(env.PORT, () => logger.info({ port: env.PORT }, 'location-service listening (http+ws)'));
