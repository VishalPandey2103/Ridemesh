# ADR-005: Redis distributed lock (single-instance) for dispatch

**Status:** accepted

## Context
Two concurrent ride requests must not offer the same driver
simultaneously — driver-side double-booking. The critical section (offer
window, ≤15s) spans processes, so an in-process mutex is useless.

## Options considered
1. **Postgres row lock** — `SELECT ... FOR UPDATE` on the driver row.
   Correct but holds a DB connection hostage for 15s per offer, and drags
   users_db into matching's hot path (ownership violation).
2. **Single-Redis lock** — `SET key token NX PX ttl`; release via Lua
   compare-and-delete (only the token holder may delete, so an expired
   lock's late release can't kill someone else's lock). TTL bounds the
   damage of a crashed holder.
3. **Redlock** — the same lock acquired on a majority of 5 independent
   Redis nodes; survives single-node loss.

## Decision
Option 2, implemented from scratch in `shared/src/lock.js` (SET NX PX +
Lua release + token fencing).

## The Redlock debate (interview material)
Kleinberg–Martin exchange, condensed: Martin Kleppmann showed Redlock
cannot guarantee mutual exclusion under process pauses/clock jumps without
fencing tokens enforced by the resource; Antirez countered that for
efficiency locks (avoiding duplicate work) it's fine. Our lock is an
**efficiency** lock: if it ever double-grants, the worst case is two
offers to one driver, and the offer-response CAS (`state: pending ->
accepted` exactly once) plus the location-status flip make double-BOOKING
still impossible. Correctness does not rest on the lock — the lock just
prevents wasted offers. That layering is the actual lesson.

## Consequences
Single Redis = lock availability tied to that node (acceptable; dispatch
already needs Redis). TTL must exceed the offer window (20s > 15s).
