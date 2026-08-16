import { Server } from 'socket.io';
import { createAdapter } from '@socket.io/redis-adapter';
import { createRedis } from '@ridemesh/shared/src/redis.js';
import { verifyToken } from '@ridemesh/shared/src/auth.js';
import { callService } from '@ridemesh/shared/src/http.js';

/**
 * Real-time layer (Socket.IO over the location service's HTTP server).
 *
 * Why Socket.IO + Redis adapter instead of raw `ws`:
 * A single WebSocket server can hold ~tens of thousands of connections, but
 * the moment you run TWO instances behind a load balancer you hit the
 * cross-server delivery problem: rider R is connected to instance A, the
 * event about R's trip is produced on instance B (or in matching-service,
 * which holds no sockets at all). The Redis adapter solves this: every
 * `io.to(room).emit(...)` is published on Redis Pub/Sub, every instance
 * subscribes, and whichever instance actually holds the socket delivers it.
 * That single property makes the layer horizontally scalable — and it's why
 * matching-service and trip-service can push to clients via the
 * @socket.io/redis-emitter without owning any connections.
 *
 * Rooms as the addressing scheme:
 *   driver:{driverId} — exactly one driver's device(s); offers go here.
 *   trip:{tripId}     — everyone watching a trip (the rider); status
 *                       updates go here.
 *
 * Server->client events:  ride:offer, ride:offer_expired, trip:update
 * Client->server events:  offer:response, trip:status, trip:subscribe
 */
export function attachSockets({ httpServer, geo, env, logger, metrics }) {
  const pubClient = createRedis(env.REDIS_URL);
  const subClient = createRedis(env.REDIS_URL);
  const io = new Server(httpServer, { cors: { origin: '*' } });
  io.adapter(createAdapter(pubClient, subClient));

  const wsConnections = metrics.gauge('ws_connections', 'Open WebSocket connections', ['role']);

  // --- handshake auth: no valid JWT, no socket. --------------------------
  io.use((socket, next) => {
    try {
      const claims = verifyToken(socket.handshake.auth?.token || '');
      socket.data.user = { id: claims.sub, role: claims.role, name: claims.name };
      next();
    } catch {
      next(new Error('unauthorized'));
    }
  });

  io.on('connection', async (socket) => {
    const { id: userId, role } = socket.data.user;
    wsConnections.inc({ service: metrics.service, role });
    logger.info({ userId, role }, 'ws connected');

    if (role === 'driver') {
      socket.join(`driver:${userId}`);
      // Reconnect-safe: only flip to online if not mid-trip (a driver whose
      // app dropped during a trip must come back as on_trip, not online).
      const current = await geo.getStatus(userId);
      if (current !== 'on_trip') await geo.setStatus(userId, 'online');
    }

    // ------------------------------------------------- offer:response ----
    // Driver accepted/rejected an offer. Matching-service owns offer state,
    // so we forward and let it validate (offer may already be expired).
    socket.on('offer:response', async (msg, ack) => {
      if (role !== 'driver') return;
      try {
        const out = await callService('matching',
          `${env.MATCHING_SERVICE_URL}/internal/offers/${msg?.offerId}/respond`, {
            method: 'POST',
            body: { driverId: userId, accept: !!msg?.accept }
          });
        ack?.(out);
      } catch (e) {
        ack?.({ error: e.message });
      }
    });

    // ---------------------------------------------------- trip:status ----
    // Driver progresses the trip: driver_arriving -> in_progress -> completed.
    // Trip-service enforces the state machine; we only attach identity.
    socket.on('trip:status', async (msg, ack) => {
      if (role !== 'driver') return;
      try {
        const out = await callService('trip',
          `${env.TRIP_SERVICE_URL}/internal/trips/${msg?.tripId}/status`, {
            method: 'POST',
            body: { status: msg?.status, driverId: userId }
          });
        ack?.(out);
      } catch (e) {
        ack?.({ error: e.message });
      }
    });

    // -------------------------------------------------- trip:subscribe ---
    // Rider asks for live updates. Authorize against trip ownership before
    // joining the room — otherwise anyone could watch any trip.
    socket.on('trip:subscribe', async (msg, ack) => {
      try {
        const trip = await callService('trip',
          `${env.TRIP_SERVICE_URL}/internal/trips/${msg?.tripId}`);
        if (trip.rider_id !== userId && trip.driver_id !== userId) {
          return ack?.({ error: 'not your trip' });
        }
        socket.join(`trip:${msg.tripId}`);
        ack?.({ ok: true, trip });
      } catch (e) {
        ack?.({ error: e.message });
      }
    });

    socket.on('disconnect', async () => {
      wsConnections.dec({ service: metrics.service, role });
      if (role === 'driver') {
        const status = await geo.getStatus(userId);
        // Keep on_trip drivers "present" so a flaky network mid-trip doesn't
        // strand the trip; pure online drivers go offline (and out of GEO).
        if (status !== 'on_trip') await geo.setStatus(userId, 'offline');
      }
      logger.info({ userId, role }, 'ws disconnected');
    });
  });

  return io;
}
