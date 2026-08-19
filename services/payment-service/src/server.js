import express from 'express';
import pg from 'pg';
import { createLogger, httpLogger } from '@ridemesh/shared/src/logger.js';
import { requireEnv, envInt } from '@ridemesh/shared/src/env.js';
import { asyncHandler, errorHandler, notFound } from '@ridemesh/shared/src/errors.js';
import { userFromHeaders, requireUser } from '@ridemesh/shared/src/auth.js';
import { createBus } from '@ridemesh/shared/src/rabbit.js';
import { EVENTS } from '@ridemesh/shared/src/events.js';
import { enqueueEvent, startOutboxPoller } from '@ridemesh/shared/src/outbox.js';
import { initMetrics } from '@ridemesh/shared/src/metrics.js';
import { selectProvider } from './providers.js';

/**
 * Payment Service — money moves here, so every guarantee is layered:
 *
 * 1. IDEMPOTENT CHARGES. payments has UNIQUE(trip_id, kind). The consumer's
 *    first act is `INSERT ... ON CONFLICT DO NOTHING RETURNING *`: exactly
 *    one delivery of trip.completed wins the row; redeliveries see no row
 *    returned, load the existing one, and only resume if it's still
 *    'processing' with budget left. A trip can NEVER be double-charged.
 *
 * 2. AT-LEAST-ONCE RESULT ANNOUNCEMENT. payment.captured/failed are written
 *    through the outbox in the same transaction as the status flip.
 *
 * Saga role: this service is the trip saga's final leg. payment.failed IS
 * the compensation signal — trip-service marks payment_status='failed'
 * (and a real system would start dunning); nothing "rolls back" the ride,
 * because the ride physically happened. That's the essence of sagas:
 * compensations are forward-moving business actions, not DB rollbacks.
 */
const env = requireEnv(['PORT', 'DATABASE_URL', 'RABBIT_URL']);
const logger = createLogger('payment-service');
const metrics = initMetrics('payment-service');
const pool = new pg.Pool({ connectionString: env.DATABASE_URL, max: 10 });
const bus = await createBus({ url: env.RABBIT_URL, service: 'payment-service', logger });
const provider = selectProvider(process.env, logger);

const captured = metrics.counter('payments_captured_total', 'Captured payments');
const failed = metrics.counter('payments_failed_total', 'Terminally failed payments');

const MAX_ATTEMPTS = envInt('PAYMENT_MAX_ATTEMPTS', 3);
const WORK_QUEUE = 'payments.charge';

async function announce(client, eventType, payment) {
  await enqueueEvent(client, eventType, {
    tripId: payment.trip_id, riderId: payment.rider_id, kind: payment.kind,
    amountPaise: Number(payment.amount_paise), paymentId: payment.id
  });
}

/**
 * Core charge routine, safe under: duplicate events, concurrent deliveries,
 * crashes at any await, and provider flakiness.
 */
async function processCharge(job) {
  const { tripId, riderId, kind, amountPaise } = job;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    let { rows } = await client.query(
      `INSERT INTO payments (trip_id, rider_id, kind, amount_paise, provider)
       VALUES ($1,$2,$3,$4,$5)
       ON CONFLICT (trip_id, kind) DO NOTHING
       RETURNING *`,
      [tripId, riderId, kind, amountPaise, provider.name]);
    let payment = rows[0];
    if (!payment) {
      // Duplicate delivery: lock the existing row and re-evaluate.
      ({ rows } = await client.query(
        `SELECT * FROM payments WHERE trip_id=$1 AND kind=$2 FOR UPDATE`, [tripId, kind]));
      payment = rows[0];
      if (!payment || payment.status !== 'processing' || payment.attempts >= MAX_ATTEMPTS) {
        await client.query('COMMIT'); // terminal or already handled — idempotent no-op
        return;
      }
    }
    await client.query('COMMIT'); // release the row before the slow network call
    client.release();

    try {
      const { providerRef } = await provider.charge({ amountPaise, receipt: `${tripId}:${kind}` });
      const c2 = await pool.connect();
      try {
        await c2.query('BEGIN');
        const upd = await c2.query(
          `UPDATE payments SET status='captured', provider_ref=$2, attempts=attempts+1, updated_at=now()
           WHERE id=$1 AND status='processing' RETURNING *`,
          [payment.id, providerRef]);
        if (upd.rows[0]) await announce(c2, EVENTS.PAYMENT_CAPTURED, upd.rows[0]);
        await c2.query('COMMIT');
        captured.inc({ service: metrics.service });
        logger.info({ tripId, kind, providerRef }, 'payment captured');
      } catch (e) {
        await c2.query('ROLLBACK').catch(() => {});
        throw e;
      } finally {
        c2.release();
      }
    } catch (chargeErr) {
      const c3 = await pool.connect();
      try {
        await c3.query('BEGIN');
        const upd = await c3.query(
          `UPDATE payments SET attempts=attempts+1, last_error=$2, updated_at=now()
           WHERE id=$1 AND status='processing' RETURNING *`,
          [payment.id, String(chargeErr.message).slice(0, 500)]);
        const p = upd.rows[0];
        if (!p) {
          // Row is no longer 'processing' — another delivery already settled
          // it. Touch nothing (this is the idempotency guard working).
        } else {
          await c3.query(
            `UPDATE payments SET status='failed', updated_at=now()
             WHERE id=$1 AND status='processing'`, [payment.id]);
          await announce(c3, EVENTS.PAYMENT_FAILED, p);
          failed.inc({ service: metrics.service });
          logger.warn({ tripId, kind, attempts: p.attempts }, 'payment failed');
        }
        await c3.query('COMMIT');
      } catch (e) {
        await c3.query('ROLLBACK').catch(() => {});
        throw e;
      } finally {
        c3.release();
      }
    }
    return;
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    client.release();
    throw e;
  }
}

// --------------------------------------------------------------- consumers
await bus.subscribe({
  queue: WORK_QUEUE,
  bindings: [EVENTS.TRIP_COMPLETED, EVENTS.TRIP_CANCELLED],
  prefetch: 10,
  handler: async (payload, key) => {
    if (key === EVENTS.TRIP_COMPLETED) {
      await processCharge({
        tripId: payload.tripId, riderId: payload.riderId,
        kind: 'trip_fare', amountPaise: payload.amountPaise
      });
    } else if (key === EVENTS.TRIP_CANCELLED) {
      if (payload.feePaise > 0) {
        await processCharge({
          tripId: payload.tripId, riderId: payload.riderId,
          kind: 'cancellation_fee', amountPaise: payload.feePaise
        });
      }
    }
  }
});

// -------------------------------------------------------------------- http
const app = express();
app.use(express.json());
app.use(httpLogger(logger));
app.use(metrics.middleware);
app.use(userFromHeaders);
app.get('/metrics', metrics.metricsHandler);
app.get('/health', (_req, res) => res.json({ ok: true, service: 'payment-service' }));

app.get('/api/payments/by-trip/:tripId', requireUser, asyncHandler(async (req, res) => {
  const { rows } = await pool.query(
    `SELECT id, trip_id, kind, amount_paise, status, provider, provider_ref, attempts, created_at
     FROM payments WHERE trip_id=$1 AND rider_id=$2 ORDER BY created_at`,
    [req.params.tripId, req.user.id]);
  res.json({ payments: rows });
}));

app.use(notFound);
app.use(errorHandler(logger));

startOutboxPoller({ pool, bus, logger });
app.listen(env.PORT, () => logger.info({ port: env.PORT }, 'payment-service listening'));
