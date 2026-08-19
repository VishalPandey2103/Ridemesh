# ADR-003: Choreographed saga for the trip lifecycle

**Status:** accepted

## Context
A ride spans four services and minutes of wall-time: no ACID transaction
can cover request -> match -> ride -> charge. Sagas replace one atomic
transaction with a sequence of local transactions + compensating actions.

## Options considered
1. **Orchestration** — a central saga coordinator commands each step and
   tracks state. Explicit flow, easier to see; but the orchestrator is a
   coupling point, a bottleneck, and a single conceptual owner of every
   business rule.
2. **Choreography** — each service reacts to events and emits its own;
   flow emerges from subscriptions. Loose coupling, natural fit for a
   topic exchange; flow is implicit (mitigated by tracing + this doc).

## Decision
Choreography over the `ride.events` topic exchange.

## The saga, concretely
trip-service `ride.requested` -> matching consumes, offers, publishes
`ride.driver_assigned` | `ride.unmatched` -> trip-service updates status ->
driver completes -> trip-service `trip.completed` -> payment charges ->
`payment.captured` | `payment.failed` -> trip-service records
payment_status; notification shadows everything.

## Compensation ≠ rollback
The ride physically happened; nothing can un-happen it. Compensations are
forward business actions: payment failure -> `payment.failed` -> trip
marked `payment_status='failed'` (dunning in a real system); rider cancel
after assignment -> cancellation-fee charge + driver released back online.

## Consequences
Adding a step = adding a subscriber (no orchestrator edit). The flow lives
in event contracts, so `shared/src/events.js` is the single source of
truth and Jaeger traces are the runtime flowchart.
