import { callService } from '@ridemesh/shared/src/http.js';
import { acquireLock, releaseLock } from '@ridemesh/shared/src/lock.js';
import { EVENTS } from '@ridemesh/shared/src/events.js';
import { rankCandidates } from './rank.js';

/**
 * Dispatch engine — the heart of matching.
 *
 * Flow per ride.requested event:
 *   1. Ask location-service for nearby online drivers (Redis GEO under it).
 *   2. Enrich with ratings (user-service batch API), rank.
 *   3. For up to MAX_CANDIDATES, one at a time (sequential offers, the
 *      Uber/Ola model — broadcast-to-all causes accept-races and driver
 *      annoyance):
 *        a. Acquire the distributed lock lock:driver:{id}. If another trip's
 *           dispatcher holds it, skip — that driver is being offered a ride
 *           RIGHT NOW. This lock is what prevents double-dispatch.
 *        b. Emit ride:offer into the driver's Socket.IO room via the Redis
 *           emitter (we hold no sockets; location-service delivers it).
 *        c. Block up to 15s for the response. Implementation: the respond
 *           endpoint LPUSHes "accept"/"reject" onto offer:resp:{offerId};
 *           we BRPOP with a 15s timeout on a dedicated connection. BRPOP is
 *           push-based blocking — zero polling, exact wakeup, and the
 *           timeout IS the offer expiry. (A dedicated duplicate connection
 *           is required: a blocked connection can serve nothing else.)
 *        d. accept  -> mark driver on_trip, publish ride.driver_assigned, done.
 *           reject/timeout -> release lock, notify driver UI, next candidate.
 *   4. Exhausted -> publish ride.unmatched (trip-service marks no_drivers).
 *
 * Offer state lives in Redis with a TTL (offer:{id}) so a late "accept"
 * after timeout is verifiably rejected rather than silently honored.
 */
const OFFER_TIMEOUT_S = 15;
const MAX_CANDIDATES = 5;
const SEARCH_RADIUS_M = 4000;
const LOCK_TTL_MS = (OFFER_TIMEOUT_S + 5) * 1000; // outlive the offer window

export function createDispatcher({ redis, emitter, bus, env, logger, metrics }) {
  const dispatchStarted = metrics.counter('dispatch_started_total', 'Dispatch attempts');
  const dispatchMatched = metrics.counter('dispatch_matched_total', 'Successful matches');
  const dispatchUnmatched = metrics.counter('dispatch_unmatched_total', 'No-driver outcomes');
  const offersSent = metrics.counter('offers_sent_total', 'Offers sent to drivers');

  async function fetchCandidates(trip) {
    const { candidates } = await callService('location',
      `${env.LOCATION_SERVICE_URL}/internal/nearby?lat=${trip.pickupLat}&lng=${trip.pickupLng}&radius=${SEARCH_RADIUS_M}&limit=8`);
    if (!candidates.length) return [];
    // Enrich with ratings; if user-service is down, rank on distance alone
    // (graceful degradation — a worse ranking beats no dispatch).
    try {
      const ids = candidates.map((c) => c.driverId).join(',');
      const { users } = await callService('user', `${env.USER_SERVICE_URL}/internal/users?ids=${ids}`);
      const byId = new Map(users.map((u) => [u.id, u]));
      return candidates.map((c) => ({ ...c, rating: Number(byId.get(c.driverId)?.rating ?? 4.5) }));
    } catch (e) {
      logger.warn({ err: e.message }, 'rating enrichment failed, ranking on distance');
      return candidates;
    }
  }

  function waitForResponse(offerId, timeoutS) {
    // Dedicated blocking connection per wait; cheap at dev scale, and the
    // pattern is what matters (prod would pool blocking connections).
    const blocking = redis.duplicate();
    return blocking
      .brpop(`offer:resp:${offerId}`, timeoutS)
      .then((res) => (res ? res[1] : 'timeout'))
      .finally(() => blocking.disconnect());
  }

  async function dispatchTrip(trip) {
    dispatchStarted.inc({ service: metrics.service });
    logger.info({ tripId: trip.tripId }, 'dispatch started');

    const ranked = rankCandidates(await fetchCandidates(trip));
    for (const candidate of ranked.slice(0, MAX_CANDIDATES)) {
      // Rider may have cancelled while we were offering to earlier drivers.
      if (await redis.get(`dispatch:cancelled:${trip.tripId}`)) {
        logger.info({ tripId: trip.tripId }, 'dispatch aborted: trip cancelled');
        return;
      }

      const lockKey = `lock:driver:${candidate.driverId}`;
      const lockToken = await acquireLock(redis, lockKey, LOCK_TTL_MS);
      if (!lockToken) continue; // driver is mid-offer for another trip

      const offerId = crypto.randomUUID();
      await redis.set(`offer:${offerId}`,
        JSON.stringify({ tripId: trip.tripId, driverId: candidate.driverId, state: 'pending' }),
        'EX', OFFER_TIMEOUT_S + 10);

      offersSent.inc({ service: metrics.service });
      emitter.to(`driver:${candidate.driverId}`).emit('ride:offer', {
        offerId,
        tripId: trip.tripId,
        pickup: { lat: trip.pickupLat, lng: trip.pickupLng },
        drop: { lat: trip.dropLat, lng: trip.dropLng },
        fareEstimatePaise: trip.fareEstimatePaise,
        surge: trip.surge,
        distToPickupM: candidate.distM,
        expiresInMs: OFFER_TIMEOUT_S * 1000
      });

      const outcome = await waitForResponse(offerId, OFFER_TIMEOUT_S);

      if (outcome === 'accept') {
        // Flip availability BEFORE announcing: once on_trip, the driver
        // vanishes from every future findNearby, closing the race where a
        // second dispatcher sees them between assign and status-flip.
        await callService('location',
          `${env.LOCATION_SERVICE_URL}/internal/drivers/${candidate.driverId}/status`,
          { method: 'POST', body: { status: 'on_trip', tripId: trip.tripId } });
        await bus.publish(EVENTS.DRIVER_ASSIGNED, {
          tripId: trip.tripId, riderId: trip.riderId,
          driverId: candidate.driverId, offerId, distToPickupM: candidate.distM
        });
        // Tell the driver's device it won the offer (rider learns via the
        // trip:update push that trip-service emits on consuming the event).
        emitter.to(`driver:${candidate.driverId}`).emit('trip:assigned', {
          tripId: trip.tripId,
          pickup: { lat: trip.pickupLat, lng: trip.pickupLng },
          drop: { lat: trip.dropLat, lng: trip.dropLng }
        });
        await releaseLock(redis, lockKey, lockToken); // safe: status now guards
        dispatchMatched.inc({ service: metrics.service });
        logger.info({ tripId: trip.tripId, driverId: candidate.driverId }, 'matched');
        return;
      }

      // reject or timeout: clean up and cascade to the next candidate.
      await redis.del(`offer:${offerId}`);
      await releaseLock(redis, lockKey, lockToken);
      if (outcome === 'timeout') {
        emitter.to(`driver:${candidate.driverId}`).emit('ride:offer_expired', { offerId });
      }
      logger.info({ tripId: trip.tripId, driverId: candidate.driverId, outcome }, 'candidate declined');
    }

    dispatchUnmatched.inc({ service: metrics.service });
    await bus.publish(EVENTS.RIDE_UNMATCHED, { tripId: trip.tripId, riderId: trip.riderId });
    logger.info({ tripId: trip.tripId }, 'no drivers available');
  }

  /** Respond endpoint logic: atomically flip pending -> accepted/rejected. */
  async function respondToOffer(offerId, driverId, accept) {
    const raw = await redis.get(`offer:${offerId}`);
    if (!raw) return { ok: false, reason: 'offer expired' };
    const offer = JSON.parse(raw);
    if (offer.driverId !== driverId) return { ok: false, reason: 'not your offer' };
    if (offer.state !== 'pending') return { ok: false, reason: `offer already ${offer.state}` };

    offer.state = accept ? 'accepted' : 'rejected';
    await redis.set(`offer:${offerId}`, JSON.stringify(offer), 'EX', 60);
    // Wake the blocked dispatcher. If it already timed out, this push sits
    // in a key that expires — harmless by design.
    await redis.lpush(`offer:resp:${offerId}`, accept ? 'accept' : 'reject');
    await redis.expire(`offer:resp:${offerId}`, 60);
    return { ok: true, state: offer.state, tripId: offer.tripId };
  }

  return { dispatchTrip, respondToOffer };
}
