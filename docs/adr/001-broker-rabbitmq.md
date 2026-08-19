# ADR-001: RabbitMQ over Kafka as the event broker

**Status:** accepted

## Context
The trip lifecycle is a choreographed saga: trip, matching, payment, and
notification services coordinate exclusively through events. The broker
choice shapes delivery semantics, operational load, and what the codebase
teaches.

## Options considered
1. **Kafka** — partitioned log, replayable history, consumer groups,
   per-key ordering. The industry default at ride-hailing scale (Uber runs
   trillions of Kafka messages/day).
2. **RabbitMQ** — smart broker/dumb consumer, topic-exchange routing,
   per-queue TTL + dead-lettering, mature management UI.
3. **Redis Streams** — lightweight log; already have Redis. Weaker
   tooling, consumer-group ergonomics rougher, conflates cache and broker
   failure domains.

## Decision
RabbitMQ with a single topic exchange (`ride.events`), durable queues per
consumer, and DLQs.

## Rationale
- The workload is **task distribution** (each event processed once by each
  consumer group), not stream analytics or replay — RabbitMQ's native
  model. Kafka's replay/retention superpowers go unused here.
- Routing-key wildcards give the notification service `#` for free;
  Kafka would need one-topic-per-event or client-side filtering.
- The retry topology payments needs (TTL queue dead-lettering back to the
  work queue) is built-in; Kafka needs hand-rolled retry topics.
- Operational surface on a laptop: one container, no KRaft/ZooKeeper, a
  management UI that makes queue depth visible during load tests.
- Builds on prior RabbitMQ experience (InfraCore) — depth over novelty.

## Consequences / when Kafka wins
No event replay: a new consumer cannot rebuild state from history (would
pair Kafka with event sourcing for that). At >100k msg/s or with multiple
teams consuming the same firehose, Kafka's partitioned log and consumer
groups justify their cost. The location-ping firehose is the natural first
flow to migrate — high-volume, loss-tolerant, order-per-driver maps to
partition-per-driver-key.
