# ADR-002: Redis GEO for driver location indexing

**Status:** accepted

## Context
Dispatch needs `find_nearby(lat, lng, radius)` over a fleet whose positions
churn every 4 seconds. Write rate dominates: 10k drivers = 2.5k writes/s
sustained, reads only on ride requests.

## Options considered
1. **Naive scan** — positions in a table/hash, Haversine over all N per
   query. O(N) per read; correct baseline, dies at city scale.
2. **Postgres PostGIS** — GiST-indexed geography column. Precise,
   transactional, but every ping is a disk-bound UPDATE + index write;
   vacuum churn under this write pattern is punishing.
3. **Redis GEO** — GEOADD/GEOSEARCH on an in-memory sorted set scored by
   52-bit interleaved geohash. O(log N) writes, memory-speed radius reads.
4. **In-process H3 grid** — cell -> driver-set maps in app memory. Fastest,
   but state dies with the process and can't be shared across instances.

## Decision
Redis GEO, keyed per geohash-prefix shard (`drivers:geo:{prefix}`), with a
parallel `driver:{id}:status` hash for online/on_trip/offline filtering.

## Rationale
Driver positions are **ephemeral hot state** — stale in 4s, rebuildable
from the next ping. Durability (PostGIS's strength) buys nothing; write
throughput and read latency are everything. A sorted set keyed by geohash
turns 2-D proximity into 1-D range scans: GEOSEARCH computes the 9-cell
neighborhood covering the radius and range-scans each, giving
O(log N + M) instead of O(N).

## Sharding
A city-wide single key concentrates all writes on one Redis. Keying by
geohash prefix and hashing that prefix across `GEO_REDIS_URLS` spreads
load; queries fan out to shards touched by the search circle and merge.
One URL in dev; the code path is shard-count agnostic.

## Consequences
Redis restart = empty index until drivers re-ping (~4s, acceptable).
Geohash cells are rectangles, not circles — GEOSEARCH over-scans then
filters by true Haversine distance (correctness preserved, minor waste).
