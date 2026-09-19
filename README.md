# RideMesh

A small distributed ride-hailing backend built to mimic the main components of a real platform: rider and driver accounts, geo-aware driver tracking, matching, trip state handling, fare calculation, payment processing, and event-driven notifications.

This repo is mostly a local learning/demo system. It runs as a set of Node.js services behind Docker and gives you a working end-to-end flow without pretending to be production-ready.

## What it does

The service layout is straightforward:

- `gateway` accepts requests from clients and validates JWTs before routing internally
- `user-service` handles rider/driver identity and basic profile data
- `location-service` tracks live driver positions over WebSockets and Redis GEO
- `matching-service` finds nearby drivers and pushes ride offers
- `trip-service` manages trip lifecycle, states, and outbox events
- `pricing-service` calculates fares and handles surge pricing
- `payment-service` processes charges and retries failed attempts
- `notification-service` listens for events and emits stub push/SMS/email messages

The whole stack is stitched together with Redis, RabbitMQ, Postgres, and Docker Compose.

## Tech stack

- Node.js
- Express
- Socket.IO
- PostgreSQL
- Redis
- RabbitMQ
- Prometheus + Grafana + Jaeger
- Docker Compose

## Architecture

```text
                               ┌─────────────────────┐
                               │      Clients        │
                               │  Riders / Drivers   │
                               └──────────┬──────────┘
                                          │ HTTP + WebSockets
                                          ▼
                               ┌─────────────────────┐
                               │     API Gateway      │
                               │     :8080            │
                               │ JWT + routing       │
                               └───────┬─────────────┘
                                       │
        ┌──────────────────────────────┼──────────────────────────────┐
        │                              │                              │
        ▼                              ▼                              ▼
┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│  User Service   │          │ Location Service│          │ Matching Service│
│  :3001          │◄────────►│  :3002          │◄────────►│  :3003          │
│ users_db        │          │ Redis GEO       │          │ Driver offers   │
│ auth + profiles │          │ live GPS + SSE  │          │ ranking + locks │
└─────────────────┘          └─────────────────┘          └────────┬────────┘
                                                                       │
                                                                       ▼
                                                            ┌─────────────────┐
                                                            │   Trip Service   │
                                                            │   :3004          │
                                                            │ trip state +     │
                                                            │ outbox events    │
                                                            └──────┬──────────┘
                                                                   │
                      ┌───────────────────────────────┬───────────────────────┐
                      │                               │                       │
                      ▼                               ▼                       ▼
         ┌─────────────────┐            ┌─────────────────┐     ┌────────────────────┐
         │ Pricing Service │            │ Payment Service │     │ Notification Svc   │
         │ :3005           │            │ :3006           │     │ :3007              │
         │ surge + fare    │            │ idempotent      │     │ push / SMS / email │
         └─────────────────┘            │ retries + logs  │     └────────────────────┘
                                        └─────────────────┘

                              ┌─────────────────────┐
                              │      RabbitMQ        │
                              │  async event flow   │
                              └──────────┬──────────┘
                                         │
                                         ▼
                              ┌─────────────────────┐
                              │       Redis         │
                              │ GEO index + pubsub │
                              └─────────────────────┘

                              ┌─────────────────────┐
                              │     PostgreSQL      │
                              │ one DB per service  │
                              └─────────────────────┘

Observability:
- Prometheus collects app metrics
- Grafana visualizes them
- Jaeger tracks request traces
``` 

The main idea is simple: HTTP handles the request path, while RabbitMQ and Redis handle the async coordination between services.

## Typical ride flow

1. A rider creates a trip through the gateway.
2. The trip service records the trip and emits a `ride.requested` event.
3. The matching service looks up nearby drivers in Redis GEO and sends offers.
4. A driver accepts, and the trip moves into a matched state.
5. Driver location updates keep flowing while the trip is active.
6. When the trip ends, pricing calculates the final fare.
7. Payment service attempts the charge, retries on failure, and records outcome.
8. Notification service emits state updates and user-facing messages.

This is intentionally event-driven rather than a single monolithic flow.

## Local setup

From the repo root:

```bash
npm install

docker compose up --build
```

Then you can use the app locally:

```bash
npm run demo
npm test
```

Useful scripts:

```bash
npm run demo                # one scripted end-to-end ride
npm run simulate:drivers    # fake drivers moving around and accepting rides
npm run up                  # docker compose up --build -d
npm run down                # docker compose down -v
npm run logs                # tail service logs
```

## Ports and UIs

After starting the stack:

- Gateway: http://localhost:8080
- RabbitMQ UI: http://localhost:15672 (guest / guest)
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
- Jaeger: http://localhost:16686

## Repo structure

```text
shared/
  src/          common auth, errors, redis, outbox, metrics, geo helpers
  test/         shared logic tests

services/
  gateway/
  user-service/
  location-service/
  matching-service/
  trip-service/
  pricing-service/
  payment-service/
  notification-service/

scripts/
  demo-ride.js
  simulate-drivers.js
  k6/

infra/
  init-db.sql
  prometheus.yml
  grafana/

docs/
  adr/
  deploy-aws.md
```

## Design notes

A few decisions are worth knowing if you are reading the code:

- Events are used to coordinate cross-service steps instead of one giant orchestrator.
- Redis GEO is used for fast nearby-driver lookup rather than a database spatial index.
- Payment charging is idempotent so retries do not double-charge.
- Each service owns its own Postgres database, which keeps boundaries explicit.
- The project includes ADRs in `docs/adr/` to explain the architecture choices.

## Caveats

This is a demo project, not a production platform:

- it runs everything in a single local Docker stack
- ETA is simplified rather than road-aware
- the gateway is treated as the trusted boundary
- some flows are intentionally simplified for learning and testing

That said, the codebase is useful for understanding how a ride-hailing system can be structured around async events, geo indexes, and service boundaries.

## Helpful reading

- `docs/adr/` for the design rationale
- `docs/deploy-aws.md` for infrastructure notes
- `scripts/demo-ride.js` for the full example flow
- `services/*/test/*.test.js` for the behavior checks

