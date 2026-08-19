/**
 * demo-ride.js — one complete ride, end to end, against a running stack.
 *
 *   docker compose up --build   (wait for services)
 *   npm run demo
 *
 * What it exercises (the full saga):
 *   register rider + driver -> driver connects socket + pings location ->
 *   rider requests trip -> surge snapshot + fare estimate -> matching
 *   offers the driver (watch the 15s window) -> bot accepts -> lifecycle
 *   driver_arriving -> in_progress (GPS breadcrumbs) -> completed ->
 *   trip.completed event -> payment charge (retries on mock failures) ->
 *   payment.captured -> trip.payment_status = 'captured'.
 *
 * Pure client-side: talks only to the gateway (HTTP) and location-service
 * (WebSocket), exactly like real rider/driver apps would.
 */
import { io } from 'socket.io-client';

const GATEWAY = process.env.GATEWAY_URL || 'http://localhost:8080';
const WS = process.env.WS_URL || 'http://localhost:3002';

// MG Road -> Koramangala, Bangalore
const PICKUP = { lat: 12.9758, lng: 77.6045 };
const DROP = { lat: 12.9352, lng: 77.6245 };

const log = (who, msg, extra = '') =>
  console.log(`${new Date().toISOString().slice(11, 19)} [${who.padEnd(7)}] ${msg}`, extra);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(path, { method = 'GET', token, body } = {}) {
  const res = await fetch(GATEWAY + path, {
    method,
    headers: {
      'content-type': 'application/json',
      ...(token ? { authorization: `Bearer ${token}` } : {})
    },
    body: body ? JSON.stringify(body) : undefined
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${method} ${path} -> ${res.status}: ${JSON.stringify(json)}`);
  return json;
}

async function main() {
  const suffix = Date.now();

  // ------------------------------------------------- 1. register both users
  const rider = await api('/api/auth/register', {
    method: 'POST',
    body: { name: 'Demo Rider', phone: `+9190000${suffix % 100000}`, password: 'secret123', role: 'rider' }
  });
  log('rider', `registered id=${rider.user.id}`);

  const driver = await api('/api/auth/register', {
    method: 'POST',
    body: {
      name: 'Demo Driver', phone: `+9198000${suffix % 100000}`, password: 'secret123',
      role: 'driver', vehicleNo: 'KA01AB1234', licenseNo: 'DL-2020-0042'
    }
  });
  log('driver', `registered id=${driver.user.id} (kyc auto-approved for demo)`);
  await api(`/api/users/${driver.user.id}/kyc`, {
    method: 'POST', token: driver.token, body: { status: 'approved' }
  });

  // ------------------------------------ 2. driver bot: connect + go online
  const dsock = io(WS, { auth: { token: driver.token }, transports: ['websocket'] });
  await new Promise((res, rej) => { dsock.on('connect', res); dsock.on('connect_error', rej); });
  log('driver', 'socket connected, pinging location every 4s');

  let pos = { ...PICKUP, lat: PICKUP.lat + 0.004, lng: PICKUP.lng + 0.004 }; // ~600m away
  let onTrip = false;
  const pinger = setInterval(() => dsock.emit('driver:ping', pos), 4000);
  dsock.emit('driver:ping', pos); // immediate first ping

  // Auto-accept incoming offers.
  dsock.on('ride:offer', (offer) => {
    log('driver', `OFFER received for trip=${offer.tripId} fare~Rs${(offer.fareEstimatePaise / 100).toFixed(0)} — accepting`);
    dsock.emit('offer:response', { offerId: offer.offerId, accept: true });
  });

  // Scripted lifecycle once assigned: arrive -> start -> drive -> complete.
  dsock.on('trip:assigned', async ({ tripId }) => {
    onTrip = true;
    log('driver', `assigned trip=${tripId}, driving to pickup`);
    await sleep(2000);
    dsock.emit('trip:status', { tripId, status: 'driver_arriving' });
    await sleep(2000);
    pos = { ...PICKUP };
    dsock.emit('trip:status', { tripId, status: 'in_progress' });
    log('driver', 'trip started, moving toward drop (breadcrumbs -> route)');
    // Interpolate pickup -> drop in 6 steps so trip_route_points fills up.
    for (let i = 1; i <= 6; i++) {
      await sleep(2000);
      pos = {
        lat: PICKUP.lat + (DROP.lat - PICKUP.lat) * (i / 6),
        lng: PICKUP.lng + (DROP.lng - PICKUP.lng) * (i / 6)
      };
      dsock.emit('driver:ping', pos);
    }
    await sleep(1000);
    dsock.emit('trip:status', { tripId, status: 'completed' });
    log('driver', 'trip completed');
  });

  await sleep(2500); // let the first pings land in the GEO index

  // ---------------------------------------------- 3. rider requests a trip
  const trip = await api('/api/trips', {
    method: 'POST', token: rider.token,
    body: { pickupLat: PICKUP.lat, pickupLng: PICKUP.lng, dropLat: DROP.lat, dropLng: DROP.lng }
  });
  log('rider', `trip requested id=${trip.tripId} estimate=Rs${(trip.fareEstimatePaise / 100).toFixed(2)} surge=${trip.surge}x cell=${trip.cell}`);

  // Live updates over the rider's own socket.
  const rsock = io(WS, { auth: { token: rider.token }, transports: ['websocket'] });
  await new Promise((res) => rsock.on('connect', res));
  rsock.emit('trip:subscribe', { tripId: trip.tripId });
  rsock.on('trip:update', (u) => log('rider', `trip:update -> ${u.status}${u.paymentStatus ? ` (payment: ${u.paymentStatus})` : ''}`));
  rsock.on('driver:location', () => process.stdout.write('.')); // live GPS dots

  // -------------------------------- 4. poll until payment settles (or 90s)
  const deadline = Date.now() + 90_000;
  let finalTrip;
  while (Date.now() < deadline) {
    await sleep(3000);
    finalTrip = await api(`/api/trips/${trip.tripId}`, { token: rider.token });
    if (finalTrip.status === 'completed' && ['captured', 'failed'].includes(finalTrip.payment_status)) break;
    if (['no_drivers', 'cancelled'].includes(finalTrip.status)) break;
  }

  console.log('\n' + '='.repeat(62));
  log('result', `status=${finalTrip.status} payment=${finalTrip.payment_status}`);
  if (finalTrip.final_fare_paise) {
    log('result', `distance=${(finalTrip.distance_m / 1000).toFixed(2)}km duration=${finalTrip.duration_s}s fare=Rs${(Number(finalTrip.final_fare_paise) / 100).toFixed(2)} (surge ${finalTrip.surge_multiplier}x)`);
  }
  const { payments } = await api(`/api/payments/by-trip/${trip.tripId}`, { token: rider.token });
  for (const p of payments) {
    log('result', `payment kind=${p.kind} status=${p.status} attempts=${p.attempts} provider=${p.provider}`);
  }
  console.log('='.repeat(62));
  console.log('Inspect the same ride in: Jaeger http://localhost:16686 | RabbitMQ http://localhost:15672 | Grafana http://localhost:3000');

  clearInterval(pinger);
  dsock.close(); rsock.close();
  process.exit(finalTrip.status === 'completed' ? 0 : 1);
}

main().catch((e) => { console.error('DEMO FAILED:', e.message); process.exit(1); });
