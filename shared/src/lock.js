/**
 * Distributed lock on a single Redis instance.
 *
 * Why dispatch needs this: two ride requests arriving 50ms apart can both see
 * driver D as the nearest candidate. Without mutual exclusion both matching
 * workers send D an offer -> D accepts one, the other trip believes it also
 * has D -> double-dispatch. A row lock in Postgres can't help because the
 * "resource" (a driver's attention for 15 seconds) isn't a DB row owned by
 * the matching service. So we lock in Redis, the shared low-latency store.
 *
 * Correctness mechanics (this is the single-instance core of Redlock):
 * 1. SET key <random-token> NX PX <ttl>
 *    - NX: only succeeds if nobody holds it (atomic test-and-set).
 *    - PX: auto-expiry. If the matching worker crashes mid-offer, the lock
 *      frees itself after ttl instead of deadlocking the driver forever.
 *    - random token: proves ownership at release time.
 * 2. Release with a Lua script that deletes ONLY if the stored value equals
 *    our token. Plain DEL is a bug: if our lock expired and another worker
 *    acquired it, DEL would release *their* lock. GET+DEL in two commands has
 *    the same race; Lua makes compare-and-delete atomic.
 *
 * Full Redlock quorums across 5 Redis nodes exist for surviving Redis node
 * failure; the Kleppmann vs antirez debate is summarized in
 * docs/adr/005-distributed-lock.md. For dispatch, a wrongly-expired lock is
 * an annoyance (a driver sees two offers), not data corruption — the trip
 * assignment itself is still serialized by the DB — so single-instance
 * locking with TTL is the right cost/benefit here.
 */
const RELEASE_LUA = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end`;

export async function acquireLock(redis, key, ttlMs) {
  const token = crypto.randomUUID();
  const ok = await redis.set(key, token, 'PX', ttlMs, 'NX');
  return ok === 'OK' ? token : null;
}

export async function releaseLock(redis, key, token) {
  if (!token) return false;
  const res = await redis.eval(RELEASE_LUA, 1, key, token);
  return res === 1;
}

/** Convenience wrapper when the critical section is a single function. */
export async function withLock(redis, key, ttlMs, fn) {
  const token = await acquireLock(redis, key, ttlMs);
  if (!token) return { acquired: false };
  try {
    return { acquired: true, result: await fn() };
  } finally {
    await releaseLock(redis, key, token);
  }
}
