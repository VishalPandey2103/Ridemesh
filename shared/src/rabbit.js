import amqp from 'amqplib';
import { EXCHANGE } from './events.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Reconnecting RabbitMQ client (same shape as the InfraCore client, extended
 * with confirm-channel publishing and dead-letter queues).
 *
 * Design notes:
 * - One topic exchange (`ride.events`). Producers publish routing keys like
 *   `trip.completed`; consumers bind queues with patterns. Topic exchanges
 *   decouple producers from consumers: payment-service can start consuming
 *   `trip.completed` tomorrow without trip-service changing a line.
 * - publish() uses a ConfirmChannel and resolves only after the broker acks
 *   the message onto disk (persistent + durable). This is the "at-least-once
 *   from the broker" half; the outbox pattern (shared/outbox.js) provides the
 *   "at-least-once from the producer's DB transaction" half.
 * - Every consumer queue gets a companion dead-letter queue (`<queue>.dlq`).
 *   A handler that throws => nack(requeue=false) => message lands in the DLQ
 *   instead of poison-looping the consumer. Inspect DLQs in the Rabbit UI.
 * - On connection loss we reconnect with backoff and replay every subscribe()
 *   that was registered, so consumers survive a broker restart.
 */
export async function createBus({ url, service, logger }) {
  let conn = null;
  let ch = null;
  let closing = false;
  const subscriptions = []; // replayed after reconnect

  async function connect() {
    for (;;) {
      try {
        conn = await amqp.connect(url);
        ch = await conn.createConfirmChannel();
        await ch.assertExchange(EXCHANGE, 'topic', { durable: true });
        conn.on('close', () => {
          if (closing) return;
          logger.warn('rabbit connection closed, reconnecting in 2s');
          setTimeout(async () => {
            await connect();
            for (const s of subscriptions) await startConsumer(s);
          }, 2000);
        });
        conn.on('error', (e) => logger.error({ err: e.message }, 'rabbit connection error'));
        logger.info('rabbit connected');
        return;
      } catch (e) {
        logger.warn({ err: e.message }, 'rabbit connect failed, retrying in 3s');
        await sleep(3000);
      }
    }
  }

  async function startConsumer({ queue, bindings, handler, prefetch }) {
    const dlq = `${queue}.dlq`;
    await ch.assertQueue(dlq, { durable: true });
    await ch.assertQueue(queue, {
      durable: true,
      arguments: { 'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': dlq }
    });
    for (const b of bindings) await ch.bindQueue(queue, EXCHANGE, b);
    await ch.prefetch(prefetch); // backpressure: at most N unacked messages in flight
    await ch.consume(queue, async (msg) => {
      if (!msg) return;
      let payload;
      try {
        payload = JSON.parse(msg.content.toString());
      } catch {
        logger.error({ queue }, 'unparseable message -> DLQ');
        return ch.nack(msg, false, false);
      }
      try {
        await handler(payload, msg.fields.routingKey, msg.properties);
        ch.ack(msg);
      } catch (e) {
        logger.error({ queue, key: msg.fields.routingKey, err: e.message }, 'handler failed -> DLQ');
        ch.nack(msg, false, false);
      }
    });
    logger.info({ queue, bindings }, 'consumer started');
  }

  await connect();

  return {
    /** Publish to the topic exchange; resolves after broker confirm. */
    publish(routingKey, payload, { messageId } = {}) {
      const body = Buffer.from(JSON.stringify(payload));
      return new Promise((resolve, reject) => {
        ch.publish(
          EXCHANGE,
          routingKey,
          body,
          {
            persistent: true,
            contentType: 'application/json',
            messageId: messageId || crypto.randomUUID(),
            timestamp: Date.now(),
            appId: service
          },
          (err) => (err ? reject(err) : resolve())
        );
      });
    },

    /** Send directly to a named queue (used for the payment retry queue). */
    sendToQueue(queue, payload, options = {}) {
      const body = Buffer.from(JSON.stringify(payload));
      return new Promise((resolve, reject) => {
        ch.sendToQueue(queue, body, { persistent: true, contentType: 'application/json', ...options }, (err) =>
          err ? reject(err) : resolve()
        );
      });
    },

    async subscribe({ queue, bindings, handler, prefetch = 10 }) {
      const sub = { queue, bindings, handler, prefetch };
      subscriptions.push(sub);
      await startConsumer(sub);
    },

    /** Escape hatch for advanced topology (TTL retry queues etc.). */
    raw() {
      return ch;
    },

    async close() {
      closing = true;
      try { await ch?.close(); await conn?.close(); } catch { /* ignore */ }
    }
  };
}
