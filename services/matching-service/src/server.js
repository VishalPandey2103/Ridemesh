import express from 'express';
import { Emitter } from '@socket.io/redis-emitter';
import { createLogger, httpLogger } from '@ridemesh/shared/src/logger.js';
import { requireEnv } from '@ridemesh/shared/src/env.js';
import { asyncHandler, errorHandler, notFound } from '@ridemesh/shared/src/errors.js';
import { createRedis } from '@ridemesh/shared/src/redis.js';
import { createBus } from '@ridemesh/shared/src/rabbit.js';
import { EVENTS } from '@ridemesh/shared/src/events.js';
import { seenBefore } from '@ridemesh/shared/src/idempotency.js';
import { initMetrics } from '@ridemesh/shared/src/metrics.js';
import { createDispatcher } from './dispatch.js';

/**
 * Matching / Dispatch Service — stateless except for transient offer state
 * in Redis, which is exactly why it needs no database: an offer is 15
 * seconds of ephemeral coordination, not a domain record. Durable outcomes
 * (assignment) are announced as events and owned by trip-service.
 *
 * The redis-emitter is the trick that lets this socketless service push
 * ride:offer into a driver's browser/app: it publishes the emit on Redis
 * Pub/Sub in the exact packet format the Socket.IO Redis adapter (running
 * inside location-service) consumes and delivers.
 */
const env = requireEnv(['PORT', 'REDIS_URL', 'RABBIT_URL',
  'LOCATION_SERVICE_URL', 'USER_SERVICE_URL']);
const logger = createLogger('matching-service');
const metrics = initMetrics('matching-service');
const redis = createRedis(env.REDIS_URL);
const emitter = new Emitter(createRedis(env.REDIS_URL));
const bus = await createBus({ url: env.RABBIT_URL, service: 'matching-service', logger });

const dispatcher = createDispatcher({ redis, emitter, bus, env, logger, metrics });

// ------------------------------------------------------------ consumers ---
await bus.subscribe({
  queue: 'matching.ride-requested',
  bindings: [EVENTS.RIDE_REQUESTED],
  prefetch: 20, // 20 concurrent dispatch loops max — natural backpressure
  handler: async (payload, _key, props) => {
    if (await seenBefore(redis, props.messageId)) return; // duplicate delivery
    await dispatcher.dispatchTrip(payload);
  }
});

await bus.subscribe({
  queue: 'matching.trip-cancelled',
  bindings: [EVENTS.TRIP_CANCELLED],
  handler: async (payload) => {
    // Flag checked between offers so an in-flight dispatch loop aborts.
    await redis.set(`dispatch:cancelled:${payload.tripId}`, '1', 'EX', 300);
  }
});

// ------------------------------------------------------------------ http ---
const app = express();
app.use(express.json());
app.use(httpLogger(logger));
app.use(metrics.middleware);
app.get('/metrics', metrics.metricsHandler);
app.get('/health', (_req, res) => res.json({ ok: true, service: 'matching-service' }));

// Called by location-service when a driver taps accept/reject.
app.post('/internal/offers/:offerId/respond', asyncHandler(async (req, res) => {
  const { driverId, accept } = req.body || {};
  const out = await dispatcher.respondToOffer(req.params.offerId, driverId, !!accept);
  res.status(out.ok ? 200 : 409).json(out);
}));

app.use(notFound);
app.use(errorHandler(logger));
app.listen(env.PORT, () => logger.info({ port: env.PORT }, 'matching-service listening'));
