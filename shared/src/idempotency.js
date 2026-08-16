/**
 * Redis-backed "have I seen this before" check for consumers that do NOT
 * have their own Postgres (matching, notification). Services with a DB use
 * a processed_messages table instead (stronger: dedupe commits atomically
 * with the domain mutation — see trip/payment db.js).
 *
 * SET NX with TTL is atomic: the first caller wins, everyone else sees the
 * key and skips. TTL bounds memory; RabbitMQ redeliveries happen within
 * seconds-to-minutes, so 24h of dedupe memory is generous.
 */
export async function seenBefore(redis, key, ttlSec = 86400) {
  const ok = await redis.set(`dedupe:${key}`, '1', 'EX', ttlSec, 'NX');
  return ok !== 'OK'; // null => key already existed => duplicate
}
