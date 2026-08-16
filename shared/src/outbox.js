/**
 * Transactional Outbox — guaranteed at-least-once event publishing.
 *
 * The problem it solves (dual-write problem): a service that does
 *     await db.commit();        // 1
 *     await rabbit.publish();   // 2
 * can crash between 1 and 2. The trip is "completed" in Postgres but
 * payment-service never hears about it — money is never collected and no
 * retry will ever fire. Reversing the order is worse: publish succeeds,
 * commit fails, and we charge for a trip that doesn't exist. There is no
 * distributed transaction across Postgres and RabbitMQ (2PC is unsupported
 * and, at scale, undesirable — it couples availability of both systems).
 *
 * The fix: make the event part of the SAME Postgres transaction.
 *     BEGIN;
 *       UPDATE trips SET status='completed' ...;
 *       INSERT INTO outbox(event_type, payload) VALUES (...);
 *     COMMIT;                                  -- atomic: both or neither
 * A background poller then reads unpublished rows, publishes to RabbitMQ
 * with broker confirms, and marks them published. Crash anywhere => the row
 * is still there => the poller retries => at-least-once delivery. Consumers
 * therefore MUST be idempotent (processed_messages tables / Redis dedupe) —
 * that's the standard pairing: outbox on the producer, dedupe on the consumer.
 *
 * FOR UPDATE SKIP LOCKED lets multiple poller replicas run without stepping
 * on each other: each replica claims a disjoint batch of rows.
 */

/** Call inside an open transaction (same `client` as the domain mutation). */
export async function enqueueEvent(client, eventType, payload) {
  await client.query(
    `INSERT INTO outbox (event_type, payload) VALUES ($1, $2)`,
    [eventType, JSON.stringify(payload)]
  );
}

export function startOutboxPoller({ pool, bus, logger, intervalMs = 1000, batchSize = 50 }) {
  let running = false;
  const timer = setInterval(async () => {
    if (running) return; // don't overlap ticks if a batch is slow
    running = true;
    try {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const { rows } = await client.query(
          `SELECT id, event_type, payload FROM outbox
           WHERE published_at IS NULL
           ORDER BY id
           LIMIT $1
           FOR UPDATE SKIP LOCKED`,
          [batchSize]
        );
        for (const row of rows) {
          // messageId = outbox.<id> is stable across retries, so consumer
          // dedupe collapses redeliveries of the same outbox row.
          await bus.publish(row.event_type, row.payload, { messageId: `outbox.${row.id}` });
          await client.query(`UPDATE outbox SET published_at = now() WHERE id = $1`, [row.id]);
        }
        await client.query('COMMIT');
        if (rows.length) logger.info({ count: rows.length }, 'outbox published');
      } catch (e) {
        await client.query('ROLLBACK').catch(() => {});
        logger.error({ err: e.message }, 'outbox poll failed (will retry)');
      } finally {
        client.release();
      }
    } finally {
      running = false;
    }
  }, intervalMs);
  timer.unref?.();
  return () => clearInterval(timer);
}
