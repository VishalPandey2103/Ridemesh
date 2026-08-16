-- One DATABASE per stateful service (microservices data-ownership principle:
-- no service may query another service's tables; the only integration paths
-- are APIs and events). One Postgres *container* hosts all three databases in
-- dev; in production each would be its own instance.

CREATE DATABASE users_db;
CREATE DATABASE trips_db;
CREATE DATABASE payments_db;

-- ---------------------------------------------------------------- users_db
\connect users_db

CREATE TABLE users (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  role          text NOT NULL CHECK (role IN ('rider', 'driver')),
  name          text NOT NULL,
  phone         text NOT NULL UNIQUE,
  password_hash text NOT NULL,
  -- driver-only verification fields (separate flows: riders verify a phone,
  -- drivers additionally need vehicle + license + KYC review)
  vehicle_no    text,
  license_no    text,
  kyc_status    text NOT NULL DEFAULT 'not_applicable'
                CHECK (kyc_status IN ('not_applicable', 'pending', 'approved', 'rejected')),
  rating        numeric(3,2) NOT NULL DEFAULT 4.50,
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX users_role_idx ON users (role);

-- ---------------------------------------------------------------- trips_db
\connect trips_db

CREATE TABLE trips (
  id                     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  rider_id               uuid NOT NULL,
  driver_id              uuid,
  status                 text NOT NULL DEFAULT 'requested' CHECK (status IN
                         ('requested','matched','driver_arriving','in_progress',
                          'completed','cancelled','no_drivers')),
  pickup_lat             double precision NOT NULL,
  pickup_lng             double precision NOT NULL,
  drop_lat               double precision NOT NULL,
  drop_lng               double precision NOT NULL,
  cell                   text NOT NULL,               -- geohash6 of pickup
  surge_multiplier       numeric(4,2) NOT NULL DEFAULT 1.00, -- snapshot at request time
  fare_estimate_paise    bigint NOT NULL DEFAULT 0,
  final_fare_paise       bigint,
  cancellation_fee_paise bigint NOT NULL DEFAULT 0,
  distance_m             integer,
  duration_s             integer,
  payment_status         text NOT NULL DEFAULT 'pending'
                         CHECK (payment_status IN ('pending','captured','failed','refunded','waived')),
  requested_at           timestamptz NOT NULL DEFAULT now(),
  started_at             timestamptz,                 -- entered in_progress
  completed_at           timestamptz,
  updated_at             timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX trips_rider_idx  ON trips (rider_id, requested_at DESC);
CREATE INDEX trips_driver_idx ON trips (driver_id, requested_at DESC);
CREATE INDEX trips_status_idx ON trips (status);

-- GPS breadcrumbs recorded during in_progress; source of truth for billed
-- distance (sum of haversine between consecutive points).
CREATE TABLE trip_route_points (
  id      bigserial PRIMARY KEY,
  trip_id uuid NOT NULL REFERENCES trips(id),
  lat     double precision NOT NULL,
  lng     double precision NOT NULL,
  ts      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX route_trip_idx ON trip_route_points (trip_id, id);

-- Transactional outbox (see shared/src/outbox.js for the why).
CREATE TABLE outbox (
  id           bigserial PRIMARY KEY,
  event_type   text NOT NULL,
  payload      jsonb NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Consumer-side idempotency: INSERT ... ON CONFLICT DO NOTHING inside the
-- same transaction as the domain mutation => exactly-once *effect* on top of
-- at-least-once delivery.
CREATE TABLE processed_messages (
  message_id   text PRIMARY KEY,
  processed_at timestamptz NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------- payments_db
\connect payments_db

CREATE TABLE payments (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trip_id      uuid NOT NULL,
  rider_id     uuid NOT NULL,
  kind         text NOT NULL CHECK (kind IN ('trip_fare','cancellation_fee')),
  amount_paise bigint NOT NULL CHECK (amount_paise >= 0),
  status       text NOT NULL DEFAULT 'processing'
               CHECK (status IN ('processing','captured','failed','refunded')),
  provider     text NOT NULL,
  provider_ref text,
  attempts     int NOT NULL DEFAULT 0,
  last_error   text,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now(),
  -- THE idempotency guarantee: one charge per (trip, kind), no matter how
  -- many times trip.completed is redelivered or the endpoint is retried.
  UNIQUE (trip_id, kind)
);

CREATE TABLE outbox (
  id           bigserial PRIMARY KEY,
  event_type   text NOT NULL,
  payload      jsonb NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE processed_messages (
  message_id   text PRIMARY KEY,
  processed_at timestamptz NOT NULL DEFAULT now()
);
