// k6 load test — ride request throughput.
//
// Run the driver fleet FIRST so requests have supply to match against:
//   DRIVERS=200 npm run simulate:drivers      (terminal 1)
//   k6 run scripts/k6/ride-load.js            (terminal 2)
//
// Stages ramp arrival rate 5 -> 50 -> 100 req/s. Raise the targets until
// p95 breaks or errors climb — record that breaking point in the README
// (that number, and WHY it breaks there, is the interview evidence).
//
// What to watch while it runs:
//   Grafana  : http_request_duration per service, dispatch_matched_total
//   RabbitMQ : queue depth on matching.ride-requested (backpressure gauge)
//   Prometheus: rate(trips_created_total[1m]) vs rate(dispatch_matched_total[1m])
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const GATEWAY = __ENV.GATEWAY_URL || 'http://localhost:8080';

const tripCreated = new Counter('trips_created');
const tripMatched = new Counter('trips_matched');
const tripUnmatched = new Counter('trips_unmatched');
const matchLatency = new Trend('match_latency_ms', true);

export const options = {
  scenarios: {
    ride_requests: {
      executor: 'ramping-arrival-rate',
      startRate: 5,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 400,
      stages: [
        { duration: '30s', target: 5 },    // warm-up
        { duration: '1m', target: 25 },
        { duration: '1m', target: 50 },
        { duration: '1m', target: 100 },   // push it
        { duration: '30s', target: 0 }
      ]
    }
  },
  thresholds: {
    http_req_duration: ['p(95)<800'],       // API latency SLO
    http_req_failed: ['rate<0.02'],         // <2% errors
    match_latency_ms: ['p(95)<20000']       // matched within 20s at p95
  }
};

// Register a pool of riders once; VUs share them round-robin (registration
// is not what we're load-testing).
export function setup() {
  const riders = [];
  for (let i = 0; i < 50; i++) {
    const res = http.post(`${GATEWAY}/api/auth/register`, JSON.stringify({
      name: `k6 rider ${i}`, phone: `+91${6000000000 + Math.floor(Math.random() * 999999999)}`,
      password: 'secret123', role: 'rider'
    }), { headers: { 'Content-Type': 'application/json' } });
    if (res.status === 201) riders.push(res.json('token'));
  }
  if (!riders.length) throw new Error('setup failed: could not register riders');
  return { riders };
}

const rand = (a, b) => a + Math.random() * (b - a);

export default function (data) {
  const token = data.riders[Math.floor(Math.random() * data.riders.length)];
  const auth = { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` } };

  // Random pickup/drop inside Bangalore's bbox.
  const body = JSON.stringify({
    pickupLat: rand(12.85, 13.10), pickupLng: rand(77.45, 77.75),
    dropLat: rand(12.85, 13.10), dropLng: rand(77.45, 77.75)
  });

  const start = Date.now();
  const res = http.post(`${GATEWAY}/api/trips`, body, auth);
  const created = check(res, { 'trip created (201)': (r) => r.status === 201 });
  if (!created) return;
  tripCreated.add(1);
  const tripId = res.json('tripId');

  // Poll the trip up to ~24s for a dispatch outcome.
  for (let i = 0; i < 8; i++) {
    sleep(3);
    const t = http.get(`${GATEWAY}/api/trips/${tripId}`, auth);
    if (t.status !== 200) continue;
    const status = t.json('status');
    if (status !== 'requested') {
      if (status === 'no_drivers') tripUnmatched.add(1);
      else { tripMatched.add(1); matchLatency.add(Date.now() - start); }
      return;
    }
  }
}
