# ADR-004: Transactional outbox for event publishing

**Status:** accepted

## Context
"Commit the DB row, then publish the event" is a lie under failure: crash
between the two and the system diverges forever (trip completed, payment
never asked). Publish-first inverts the lie (charging for an uncommitted
trip). This is the dual-write problem: two systems, no shared transaction.

## Options considered
1. **Publish after commit** — simple, silently loses events on crash.
2. **Distributed transaction (2PC)** — amqp + pg don't share a
   coordinator in practice; blocks on coordinator failure; nobody runs
   this for broker+DB.
3. **Transactional outbox** — write domain row AND event row in ONE local
   transaction; a poller publishes pending outbox rows and marks them.
4. **CDC (Debezium)** — tail the WAL, publish changes. The "big" version
   of outbox; heavy infra for this scale.

## Decision
Outbox tables in trips_db and payments_db + in-process poller
(`FOR UPDATE SKIP LOCKED` batch claim, publish with confirms, mark
published).

## Semantics
Atomicity moves entirely inside Postgres — the event exists iff the domain
change committed. Delivery becomes **at-least-once** (poller can crash
after publish, before mark), which is why every consumer deduplicates:
`processed_messages` insert-once keyed on the stable outbox messageId
(`outbox.<row-id>`), checked in the SAME transaction as the consumer's own
mutation. Outbox (exactly-once intent) + idempotent consumers
(exactly-once effect) is the standard pairing.

## Consequences
Publish latency = poll interval (500ms, fine). SKIP LOCKED lets multiple
service replicas poll the same outbox without double-claiming rows.
