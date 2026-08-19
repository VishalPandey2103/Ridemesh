import express from 'express';
import { createLogger, httpLogger } from '@ridemesh/shared/src/logger.js';
import { requireEnv } from '@ridemesh/shared/src/env.js';
import { errorHandler, notFound } from '@ridemesh/shared/src/errors.js';
import { createRedis } from '@ridemesh/shared/src/redis.js';
import { createBus } from '@ridemesh/shared/src/rabbit.js';
import { seenBefore } from '@ridemesh/shared/src/idempotency.js';
import { initMetrics } from '@ridemesh/shared/src/metrics.js';

/**
 * Notification Service — the system's "ears": binds `#` (every routing key
 * on the topic exchange) and fans each event out to channel adapters. In
 * this build the adapters log structured payloads (SMS/email/push stubs);
 * swapping in Twilio/SES/FCM later touches ONLY this service — that
 * isolation is the entire reason notifications get their own service
 * instead of every service sprinkling sendSMS() calls inline.
 *
 * Being a pure consumer it can lag, crash, or be redeployed with ZERO
 * impact on the ride path — the queue absorbs the difference. That's the
 * async-decoupling story in one service.
 */
const env = requireEnv(['PORT', 'REDIS_URL', 'RABBIT_URL']);
const logger = createLogger('notification-service');
const metrics = initMetrics('notification-service');
const redis = createRedis(env.REDIS_URL);
const bus = await createBus({ url: env.RABBIT_URL, service: 'notification-service', logger });

const sent = metrics.counter('notifications_sent_total', 'Notifications dispatched', ['channel', 'event']);

// --- channel adapters (log fallback; replace with FCM/Twilio/SES) --------
const channels = {
  push: (to, title, body) => logger.info({ channel: 'push', to, title, body }, 'PUSH'),
  sms: (to, body) => logger.info({ channel: 'sms', to, body }, 'SMS'),
  email: (to, subject, body) => logger.info({ channel: 'email', to, subject, body }, 'EMAIL')
};

// Event -> human message templates. Keys mirror shared EVENTS values.
const templates = {
  'ride.requested': (p) => ({ to: p.riderId, title: 'Finding your driver', body: `Fare estimate Rs ${(p.fareEstimatePaise / 100).toFixed(2)}${p.surge > 1 ? ` (${p.surge}x surge)` : ''}` }),
  'ride.driver_assigned': (p) => ({ to: p.riderId, title: 'Driver assigned', body: `Your driver is ${(p.distToPickupM / 1000).toFixed(1)} km away` }),
  'ride.unmatched': (p) => ({ to: p.riderId, title: 'No drivers available', body: 'Please try again in a few minutes' }),
  'trip.completed': (p) => ({ to: p.riderId, title: 'Trip completed', body: `Fare Rs ${(p.amountPaise / 100).toFixed(2)} — payment processing` }),
  'trip.cancelled': (p) => ({ to: p.riderId, title: 'Trip cancelled', body: p.feePaise > 0 ? `Cancellation fee Rs ${(p.feePaise / 100).toFixed(2)} applies` : 'No fee charged' }),
  'payment.captured': (p) => ({ to: p.riderId, title: 'Payment successful', body: `Rs ${(p.amountPaise / 100).toFixed(2)} charged for your trip` }),
  'payment.failed': (p) => ({ to: p.riderId, title: 'Payment failed', body: 'We could not process your payment. Please update your payment method.' }),
  'payment.refunded': (p) => ({ to: p.riderId, title: 'Refund processed', body: `Rs ${(p.amountPaise / 100).toFixed(2)} refunded` })
};

await bus.subscribe({
  queue: 'notifications.all-events',
  bindings: ['#'], // topic wildcard: every event, present and future
  handler: async (payload, key, props) => {
    if (await seenBefore(redis, props.messageId)) return; // dedupe on redelivery
    const render = templates[key];
    if (!render) return; // unknown/internal event — ignore silently
    const msg = render(payload);
    channels.push(msg.to, msg.title, msg.body);
    channels.sms(msg.to, `${msg.title}: ${msg.body}`);
    sent.inc({ service: metrics.service, channel: 'push', event: key });
    sent.inc({ service: metrics.service, channel: 'sms', event: key });
    if (key.startsWith('payment.')) { // receipts additionally go to email
      channels.email(msg.to, msg.title, msg.body);
      sent.inc({ service: metrics.service, channel: 'email', event: key });
    }
  }
});

const app = express();
app.use(express.json());
app.use(httpLogger(logger));
app.use(metrics.middleware);
app.get('/metrics', metrics.metricsHandler);
app.get('/health', (_req, res) => res.json({ ok: true, service: 'notification-service' }));
app.use(notFound);
app.use(errorHandler(logger));
app.listen(env.PORT, () => logger.info({ port: env.PORT }, 'notification-service listening'));
