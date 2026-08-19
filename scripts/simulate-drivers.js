/**
 * simulate-drivers.js — spins up N driver bots (default 25) that:
 *   - register + auto-approve KYC through the gateway
 *   - connect Socket.IO, ping a random-walk position every 4s across
 *     Bangalore's bounding box
 *   - auto-accept any ride offer, then run a scripted trip lifecycle
 *
 * Usage:
 *   npm run simulate:drivers            # 25 drivers
 *   DRIVERS=200 npm run simulate:drivers
 *
 * Pair with the k6 script (rider request load) to watch dispatch behavior
 * under pressure: match rate, offer timeouts, surge climbing in Grafana.
 */
import { io } from 'socket.io-client';

const GATEWAY = process.env.GATEWAY_URL || 'http://localhost:8080';
const WS = process.env.WS_URL || 'http://localhost:3002';
const N = parseInt(process.env.DRIVERS || '25', 10);

// Bangalore bounding box (matches shared/src/geo.js BANGALORE_BBOX)
const BBOX = { minLat: 12.85, maxLat: 13.10, minLng: 77.45, maxLng: 77.75 };
const rand = (a, b) => a + Math.random() * (b - a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(path, { method = 'GET', token, body } = {}) {
  const res = await fetch(GATEWAY + path, {
    method,
    headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) },
    body: body ? JSON.stringify(body) : undefined
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${method} ${path} -> ${res.status}`);
  return json;
}

let matched = 0, completed = 0;

async function spawnDriver(i) {
  const { user, token } = await api('/api/auth/register', {
    method: 'POST',
    body: {
      name: `Sim Driver ${i}`, phone: `+91${(7000000000 + Math.floor(Math.random() * 999999999))}`.slice(0, 13),
      password: 'secret123', role: 'driver',
      vehicleNo: `KA0${(i % 9) + 1}SIM${1000 + i}`, licenseNo: `DL-SIM-${i}`
    }
  });
  await api(`/api/users/${user.id}/kyc`, { method: 'POST', token, body: { status: 'approved' } });

  const sock = io(WS, { auth: { token }, transports: ['websocket'] });
  await new Promise((res, rej) => { sock.on('connect', res); sock.on('connect_error', rej); });

  let pos = { lat: rand(BBOX.minLat, BBOX.maxLat), lng: rand(BBOX.minLng, BBOX.maxLng) };
  let busy = false;

  // Random walk: ~±0.001 deg (~100m) per 4s tick when idle.
  setInterval(() => {
    if (!busy) {
      pos.lat = Math.min(BBOX.maxLat, Math.max(BBOX.minLat, pos.lat + rand(-0.001, 0.001)));
      pos.lng = Math.min(BBOX.maxLng, Math.max(BBOX.minLng, pos.lng + rand(-0.001, 0.001)));
    }
    sock.emit('driver:ping', pos);
  }, 4000);
  sock.emit('driver:ping', pos);

  sock.on('ride:offer', (offer) => sock.emit('offer:response', { offerId: offer.offerId, accept: true }));

  sock.on('trip:assigned', async ({ tripId }) => {
    busy = true; matched++;
    await sleep(1500);
    sock.emit('trip:status', { tripId, status: 'driver_arriving' });
    await sleep(1500);
    sock.emit('trip:status', { tripId, status: 'in_progress' });
    for (let s = 0; s < 4; s++) { // short simulated ride with breadcrumbs
      await sleep(2000);
      pos.lat += rand(-0.002, 0.002); pos.lng += rand(-0.002, 0.002);
      sock.emit('driver:ping', pos);
    }
    sock.emit('trip:status', { tripId, status: 'completed' });
    completed++; busy = false;
  });
}

console.log(`Spawning ${N} driver bots against ${GATEWAY} / ${WS} ...`);
let ok = 0;
for (let i = 0; i < N; i++) {
  spawnDriver(i).then(() => ok++).catch((e) => console.error(`driver ${i} failed: ${e.message}`));
  await sleep(120); // stagger registrations to be gentle on the gateway
}
setInterval(() => console.log(`[fleet] online~${ok}/${N} matched=${matched} completed=${completed}`), 5000);
