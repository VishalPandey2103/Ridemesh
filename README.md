# RideMesh — Distributed Ride-Hailing Backend

Eight Node.js/Express microservices coordinating a complete ride lifecycle —
geospatial driver matching, real-time WebSocket dispatch, choreographed
sagas over RabbitMQ, transactional outbox, distributed locks, surge
pricing, idempotent payments with retry, and full observability
(Prometheus + Grafana + Jaeger) — all orchestrated with one
`docker compose up`.

Built as a placement portfolio project designed to survive a 60-minute
system-design deep-dive. Every non-obvious decision has an ADR in
`docs/adr/`; every tricky block of code carries the reasoning inline.

---

## Architecture

```
                                ┌─────────────┐
        riders / drivers ──────►│  API Gateway │ :8080
        (HTTP + JWT)            │ verify-once  │
                                │ rate limit   │
                                └──┬───┬───┬───┘
              ┌────────────────────┘   │   └──────────────────┐
              ▼                        ▼                      ▼
        ┌───────────┐          ┌────────────┐          ┌────────────┐
        │   User    │          │    Trip    │          │  Payment   │
        │  :3001    │          │   :3004    │          │   :3006    │
        │ users_db  │          │  trips_db  │          │payments_db │
        └───────────┘          └─────┬──────┘          └─────┬──────┘
                                     │ outbox                │ outbox
   drivers ── WebSocket ──┐          ▼                       ▼
   (pings every 4s)       │   ╔═══════════════════════════════════╗
              ▼           │   ║   RabbitMQ  topic exch. ride.events║
        ┌────────────┐    │   ╚═╦═════════╦═════════════════╦═════╝
        │  Location  │◄───┘     ║         ║                 ║
        │   :3002    │          ▼         ▼                 ▼
        │ Redis GEO  │    ┌──────────┐ ┌─────────┐   ┌──────────────┐
        │ Socket.IO  │◄───┤ Matching │ │ Pricing │   │ Notification │
        │ +Redis adpt│    │  :3003   │ │  :3005  │   │    :3007     │
        └────────────┘    │ locks +  │ │ fare +  │   │  push/sms/   │
          ▲               │ offers   │ │ surge   │   │  email stubs │
          │ ride:offer via│          │ │ worker  │   └──────────────┘
          │ Redis emitter └──────────┘ └─────────┘
          │
        riders subscribe trip:{id} → live driver GPS + status pushes
```

Synchronous (HTTP, gateway-mediated or internal): auth, trip CRUD,
`find_nearby`, fare quotes, offer responses. Asynchronous (RabbitMQ
events): everything that advances the saga — `ride.requested`,
`ride.driver_assigned`, `ride.unmatched`, `trip.completed`,
`trip.cancelled`, `payment.captured`, `payment.failed`, `payment.refunded`.

## One ride, end to end

1. Rider `POST /api/trips` → trip-service snapshots surge for the pickup
   cell (pricing-service; also counts demand), computes an upfront
   estimate, inserts the trip **and** a `ride.requested` outbox row in one
   Postgres transaction.
2. Outbox poller publishes; matching-service consumes, pulls nearby online
   drivers from location-service (sharded Redis GEO), enriches with
   ratings, ranks by ETA + rating + idle-time credit.
3. For up to 5 candidates sequentially: acquire `lock:driver:{id}`
   (SET NX PX + Lua-fenced release), push `ride:offer` into the driver's
   socket room through the Redis emitter, block on `BRPOP offer:resp:{id}`
   for 15s. Reject/timeout → release, next candidate.
4. Accept → driver flipped to `on_trip` in the GEO status index (vanishes
   from future searches) → `ride.driver_assigned` → trip `matched`, rider
   gets a live `trip:update` push.
5. Driver's socket drives `driver_arriving → in_progress → completed`
   (state machine enforced server-side); in-progress pings stream to the
   rider's `trip:{id}` room and land as `trip_route_points` breadcrumbs.
6. Completion sums breadcrumb Haversine distance, gets the final fare from
   pricing (same deterministic math as the estimate; **local fallback if
   pricing is down** — circuit breaker), commits `completed` + a
   `trip.completed` outbox row atomically.
7. Payment-service consumes: `INSERT .. ON CONFLICT DO NOTHING` on
   `UNIQUE(trip_id, kind)` makes the charge idempotent; provider = mock
   (10% simulated failures) or Razorpay test mode if keys are set. Failures
   park in a TTL retry queue that dead-letters back to the work queue
   (max 3 attempts) → `payment.captured` / `payment.failed`.
8. Trip-service records `payment_status`; notification-service (bound to
   `#`) narrates every step to push/SMS/email stubs. Jaeger shows the whole
   ride as one trace.

## Run it

```bash
docker compose up --build          # full stack (first build takes a while)
# UIs:  RabbitMQ :15672 (guest/guest) · Prometheus :9090 · Grafana :3000 · Jaeger :16686

npm install                        # host-side deps for scripts/tests
npm run demo                       # one scripted ride, end to end, with live logs
DRIVERS=100 npm run simulate:drivers   # random-walk auto-accepting fleet
k6 run scripts/k6/ride-load.js     # ramping ride-request load (needs k6)
npm test                           # pure unit tests: geo, fare, rank, state
```

Expected `npm run demo` tail:

```
[result ] status=completed payment=captured
[result ] distance=5.61km duration=147s fare=Rs142.50 (surge 1x)
[result ] payment kind=trip_fare status=captured attempts=1 provider=mock
```

## Design decisions (digest — full reasoning in docs/adr/)

**RabbitMQ over Kafka (ADR-001).** The workload is task distribution with
per-consumer queues, wildcard routing, and TTL-based retry — RabbitMQ
natives. Kafka earns its complexity when replay, multi-team fan-out, or
>100k msg/s appear; the location-ping firehose is the flow to migrate
first.

**Redis GEO over PostGIS (ADR-002).** Driver positions are ephemeral hot
state rewritten every 4s — durability buys nothing, write throughput is
everything. GEO's sorted-set geohash turns 2-D proximity into O(log N)
1-D range scans; keys are sharded by geohash prefix across
`GEO_REDIS_URLS` so no single Redis eats a whole city.

**Choreographed saga over orchestration (ADR-003).** No coordinator to
bottleneck or couple against; the flow is the event contract. Compensations
are forward actions (fee charge, payment_status='failed'), never
rollbacks — the ride physically happened.

**Outbox over publish-after-commit (ADR-004).** Dual-write is a crash away
from divergence. Domain row + event row commit atomically; a
`SKIP LOCKED` poller publishes. Delivery is at-least-once, so every
consumer dedupes (`processed_messages` claim in the same transaction as
its own mutation) — exactly-once *effect*.

**Single-Redis lock, not Redlock (ADR-005).** It's an efficiency lock:
double-grant worst case is a duplicate *offer*, and the offer CAS + status
flip make double-*booking* impossible regardless. Kleppmann's fencing
critique is answered by not resting correctness on the lock at all.

**Surge snapshotted at request time.** The multiplier the rider agreed to
is the one billed; surge drifting mid-trip must not reprice a 40-minute
ride. Cells are geohash-6; a worker recomputes demand/supply per cell each
minute, capped at 3.0x, TTL'd so stale surge decays to 1.0x if the worker
dies.

**Money is integer paise everywhere.** `BIGINT` in Postgres, `Number`
integers in JS, one `Math.round` at the single float touchpoint (surge).
Breakdown components round individually so receipts always sum.

**One DB per service, one Postgres container.** `users_db` / `trips_db` /
`payments_db` — no cross-service joins possible, ownership enforced by
connection string. Single container is a laptop-RAM concession; prod is
three instances (see docs/deploy-aws.md).

**Verify-once JWT at the gateway.** Downstream services trust
`x-user-id`/`x-user-role` headers because only the gateway is reachable —
the standard trusted-perimeter trade, stated explicitly.

## Failure drills (what to try)

```bash
docker compose stop pricing-service   # trips still complete: breaker opens,
                                      # trip-service uses local fallback fare
docker compose stop payment-service   # rides unaffected; trip.completed
                                      # queues up; charges drain on restart
docker compose stop redis             # dispatch pauses; sockets reconnect
                                      # and GEO index rebuilds from pings
MOCK_FAIL_RATE=0.9 …                  # watch the TTL retry queue work:
                                      # attempts climb, then payment.failed
```

## What's real vs. simplified (honest gaps)

- Distributed lock is single-Redis with token fencing, not multi-node
  Redlock — deliberate, argued in ADR-005.
- Grafana ships with the datasource provisioned but no prebuilt dashboards;
  Prometheus metrics (`dispatch_matched_total`, `http_request_duration_*`,
  `surge_multiplier`, `payments_*`) are ready to chart.
- KYC approval is an open endpoint for demo convenience; real flow needs an
  admin role + document verification.
- ETA = distance / 6 m/s — no road routing (OSRM/Valhalla is the upgrade).
- Integration tests via testcontainers, Kafka migration of the ping
  firehose, MQTT driver transport, and the Go location-service rewrite are
  roadmap items, not present.
- k6 numbers depend on the host; run the ramp, find your breaking point,
  and record it here — the number plus the bottleneck story is the
  interview asset.

## Repo map

```
shared/src/            events · logger · errors · auth · redis · rabbit(DLQ)
                       geo(geohash from scratch) · lock · http(breaker)
                       metrics · idempotency · outbox
services/
  gateway/             verify-once JWT · Redis rate limit · routing
  user-service/        riders/drivers · bcrypt · KYC · batch ratings
  location-service/    sharded Redis GEO · Socket.IO + Redis adapter
  matching-service/    ranking · distributed locks · offer engine (BRPOP)
  trip-service/        state machine · outbox · route recording · fares
  pricing-service/     deterministic fare · surge worker
  payment-service/     idempotent charges · TTL retry queue · refunds
  notification-service/ '#' consumer · channel stubs
scripts/               demo-ride · simulate-drivers · k6/ride-load
infra/                 init-db.sql · prometheus.yml · grafana provisioning
docs/adr/              001-005 (broker · geo · saga · outbox · lock)
docs/deploy-aws.md     EC2 + ALB + Route 53 walkthrough
```
