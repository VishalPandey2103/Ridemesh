import express from 'express';
import { Emitter } from '@socket.io/redis-emitter';
import { createLogger, httpLogger } from '@ridemesh/shared/src/logger.js';
import { requireEnv } from '@ridemesh/shared/src/env.js';
import { ApiError, asyncHandler, errorHandler, notFound } from '@ridemesh/shared/src/errors.js';
import { userFromHeaders, requireUser } from '@ridemesh/shared/src/auth.js';
import { createRedis } from '@ridemesh/shared/src/redis.js';
import { createBus } from '@ridemesh/shared/src/rabbit.js';
import { EVENTS } from '@ridemesh/shared/src/events.js';
import { callService } from '@ridemesh/shared/src/http.js';
import { enqueueEvent, startOutboxPoller } from '@ridemesh/shared/src/outbox.js';
import { haversineM, cellForLocation } from '@ridemesh/shared/src/geo.js';
import { initMetrics } from '@ridemesh/shared/src/metrics.js';
import { createPool, processedOnce } from './db.js';
import { canTransition } from './state.js';

/**
 * Trip Service — owns the trip aggregate and its lifecycle. It is the saga's
 * "source of facts": every durable state change is committed to trips_db
 * together with an outbox event in one transaction, and downstream services
 * (matching, payment, notification) react to those events. There is no
 * orchestrator — this is a choreographed saga (ADR-003).
 */
const env = requireEnv(['PORT', 'DATABASE_URL', 'REDIS_URL', 'RABBIT_URL',
  'PRICING_SERVICE_URL', 'LOCATION_SERVICE_URL']);
const logger = createLogger('trip-service');
const metrics = initMetrics('trip-service');
const pool = createPool(env.DATABASE_URL);
const emitter = new Emitter(createRedis(env.REDIS_URL));
const bus = await createBus({ url: env.RABBIT_URL, service: 'trip-service', logger });

const tripsCreated = metrics.counter('trips_created_total', 'Trips requested');

// Fallback fare math for when pricing-service is down (breaker open). Same
// default rates; a slightly-imperfect fare beats a stuck trip. All money is
// integer paise — floats never touch currency (Math.round only after the
// single surge multiplication).
const RATES = { basePaise: 3000, perKmPaise: 1500, perMinPaise: 200, minFarePaise: 5000 };
function fallbackFare(distanceM, durationS, surge) {
  const raw = RATES.basePaise
    + Math.round((distanceM / 1000) * RATES.perKmPaise)
    + Math.round((durationS / 60) * RATES.perMinPaise);
  return Math.max(RATES.minFarePaise, Math.round(raw * surge));
}

function pushTripUpdate(trip) {
  emitter.to(`trip:${trip.id}`).emit('trip:update', {
    tripId: trip.id, status: trip.status, driverId: trip.driver_id,
    finalFarePaise: trip.final_fare_paise ? Number(trip.final_fare_paise) : null,
    paymentStatus: trip.payment_status
  });
}

const app = express();
app.use(express.json());
app.use(httpLogger(logger));
app.use(metrics.middleware);
app.use(userFromHeaders);
app.get('/metrics', metrics.metricsHandler);
app.get('/health', (_req, res) => res.json({ ok: true, service: 'trip-service' }));

// ------------------------------------------------------------ request trip
app.post('/api/trips', requireUser, asyncHandler(async (req, res) => {
  if (req.user.role !== 'rider') throw new ApiError(403, 'Only riders request trips');
  const { pickupLat, pickupLng, dropLat, dropLng } = req.body || {};
  for (const v of [pickupLat, pickupLng, dropLat, dropLng]) {
    if (!Number.isFinite(Number(v))) throw new ApiError(400, 'pickupLat/pickupLng/dropLat/dropLng required');
  }

  // SURGE SNAPSHOT AT REQUEST TIME (ADR: snapshot-at-start). The multiplier
  // the rider saw when agreeing to the fare is the one they're billed at —
  // surge drifting during a 40-minute trip must not change the price. The
  // observe call also increments this cell's demand counter (one request =
  // one unit of demand for the surge worker).
  let surge = 1.0;
  let cell = cellForLocation(Number(pickupLat), Number(pickupLng));
  try {
    const s = await callService('pricing', `${env.PRICING_SERVICE_URL}/internal/surge/observe`, {
      method: 'POST', body: { lat: Number(pickupLat), lng: Number(pickupLng) }
    });
    surge = s.multiplier; cell = s.cell;
  } catch (e) {
    logger.warn({ err: e.message }, 'surge observe failed, defaulting to 1.0 (graceful degradation)');
  }

  // Upfront estimate from straight-line distance x 1.4 road-winding factor.
  const straightM = haversineM(Number(pickupLat), Number(pickupLng), Number(dropLat), Number(dropLng));
  const estDistanceM = Math.round(straightM * 1.4);
  const estDurationS = Math.round(estDistanceM / 6);
  let fareEstimatePaise = fallbackFare(estDistanceM, estDurationS, surge);
  try {
    const q = await callService('pricing', `${env.PRICING_SERVICE_URL}/internal/quote`, {
      method: 'POST', body: { distanceM: estDistanceM, durationS: estDurationS, surge }
    });
    fareEstimatePaise = q.farePaise;
  } catch { /* fallback already computed */ }

  // Domain insert + ride.requested event in ONE transaction (outbox).
  const client = await pool.connect();
  let trip;
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(
      `INSERT INTO trips (rider_id, pickup_lat, pickup_lng, drop_lat, drop_lng,
                          cell, surge_multiplier, fare_estimate_paise)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [req.user.id, pickupLat, pickupLng, dropLat, dropLng, cell, surge, fareEstimatePaise]);
    trip = rows[0];
    await enqueueEvent(client, EVENTS.RIDE_REQUESTED, {
      tripId: trip.id, riderId: trip.rider_id,
      pickupLat: Number(pickupLat), pickupLng: Number(pickupLng),
      dropLat: Number(dropLat), dropLng: Number(dropLng),
      surge, fareEstimatePaise
    });
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    throw e;
  } finally {
    client.release();
  }

  tripsCreated.inc({ service: metrics.service });
  res.status(201).json({
    tripId: trip.id, status: trip.status, surge, fareEstimatePaise, cell
  });
}));

// ---------------------------------------------------------------- get trip
app.get('/api/trips/:id', requireUser, asyncHandler(async (req, res) => {
  const { rows } = await pool.query(`SELECT * FROM trips WHERE id = $1`, [req.params.id]);
  const trip = rows[0];
  if (!trip) throw new ApiError(404, 'Trip not found');
  if (trip.rider_id !== req.user.id && trip.driver_id !== req.user.id) {
    throw new ApiError(403, 'Not your trip');
  }
  res.json(trip);
}));

// -------------------------------------- internal: trip fetch (socket auth)
app.get('/internal/trips/:id', asyncHandler(async (req, res) => {
  const { rows } = await pool.query(
    `SELECT id, rider_id, driver_id, status, payment_status FROM trips WHERE id = $1`,
    [req.params.id]);
  if (!rows[0]) throw new ApiError(404, 'Trip not found');
  res.json(rows[0]);
}));

// ---------------------------------------- internal: GPS route breadcrumbs
app.post('/internal/trips/:id/route', asyncHandler(async (req, res) => {
  const { lat, lng } = req.body || {};
  await pool.query(
    `INSERT INTO trip_route_points (trip_id, lat, lng)
     SELECT $1, $2, $3 WHERE EXISTS
       (SELECT 1 FROM trips WHERE id = $1 AND status = 'in_progress')`,
    [req.params.id, lat, lng]);
  res.json({ ok: true });
}));

// ------------------------- internal: driver-driven lifecycle transitions
app.post('/internal/trips/:id/status', asyncHandler(async (req, res) => {
  const { status: next, driverId } = req.body || {};
  if (!['driver_arriving', 'in_progress'].includes(next)) {
    throw new ApiError(400, 'status must be driver_arriving|in_progress');
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(`SELECT * FROM trips WHERE id = $1 FOR UPDATE`, [req.params.id]);
    const trip = rows[0];
    if (!trip) throw new ApiError(404, 'Trip not found');
    if (trip.driver_id !== driverId) throw new ApiError(403, 'Not the assigned driver');
    if (!canTransition(trip.status, next)) {
      throw new ApiError(409, `Illegal transition ${trip.status} -> ${next}`);
    }
    const upd = await client.query(
      `UPDATE trips SET status=$2,
              started_at = CASE WHEN $2 = 'in_progress' THEN now() ELSE started_at END,
              updated_at=now()
       WHERE id=$1 RETURNING *`,
      [trip.id, next]);
    const updated = upd.rows[0];
    await client.query('COMMIT');

    pushTripUpdate(updated);
    res.json({ tripId: updated.id, status: updated.status });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}));

app.use(notFound);
app.use(errorHandler(logger));

// ------------------------------------------------------------- consumers ---
await bus.subscribe({
  queue: 'trip.matching-events',
  bindings: [EVENTS.DRIVER_ASSIGNED, EVENTS.RIDE_UNMATCHED],
  handler: async (payload, key, props) => {
    await processedOnce(pool, props.messageId, async (client) => {
      if (key === EVENTS.DRIVER_ASSIGNED) {
        const { rows } = await client.query(
          `UPDATE trips SET driver_id=$2, status='matched', updated_at=now()
           WHERE id=$1 AND status='requested' RETURNING *`,
          [payload.tripId, payload.driverId]);
        if (rows[0]) pushTripUpdate(rows[0]);
      } else {
        const { rows } = await client.query(
          `UPDATE trips SET status='no_drivers', payment_status='waived', updated_at=now()
           WHERE id=$1 AND status='requested' RETURNING *`,
          [payload.tripId]);
        if (rows[0]) pushTripUpdate(rows[0]);
      }
    });
  }
});

startOutboxPoller({ pool, bus, logger });
app.listen(env.PORT, () => logger.info({ port: env.PORT }, 'trip-service listening'));
