import { createRedis } from '@ridemesh/shared/src/redis.js';
import { cellForLocation, minuteBucket } from '@ridemesh/shared/src/geo.js';

/**
 * Sharded Redis GEO index of online drivers.
 *
 * Why Redis GEO: GEOADD stores members in a sorted set whose score is a
 * 52-bit interleaved geohash of (lng,lat). GEOSEARCH turns "drivers within
 * 3km" into a handful of sorted-set range scans over the geohash boxes that
 * cover the circle — O(log N + M) in memory, no disk, ~sub-millisecond. A
 * naive alternative (scan every driver, haversine each) is O(N) per query
 * and melts at 10k drivers x 1k requests/s. PostGIS could also answer this,
 * but pings arrive every 4s per driver: that write rate belongs in RAM with
 * TTL semantics, not in the transactional database (ADR-002).
 *
 * Why sharding: one Redis instance is single-threaded; at very large driver
 * counts the GEO zset for a whole megacity becomes a hot key. We partition
 * by geography: shard = hash(geohash4-prefix of the driver's position) mod N.
 * All drivers in the same ~39km area land on the same shard, so a nearby
 * search only needs the shard(s) covering the query area — we conservatively
 * fan out to all shards and merge (correct even when a radius straddles a
 * prefix boundary; with N small the fan-out cost is negligible). Set
 * GEO_REDIS_URLS to a comma-separated list to actually run multiple shards;
 * with one URL the code path is identical.
 *
 * Movement across shards: when a driver's prefix changes we ZREM from the
 * old shard and GEOADD to the new — remembered in driver:shard:{id} on the
 * meta client.
 */
const GEO_KEY = 'drivers:geo';
const SHARD_PREFIX_LEN = 4;

function hashStr(s) {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
  return Math.abs(h);
}

export class GeoIndex {
  constructor({ geoUrls, metaUrl }) {
    this.shards = geoUrls.map((u) => createRedis(u));
    this.meta = createRedis(metaUrl); // status, trip mapping, supply counters
  }

  shardIndexFor(lat, lng) {
    const prefix = cellForLocation(lat, lng, SHARD_PREFIX_LEN);
    return hashStr(prefix) % this.shards.length;
  }

  async upsertDriver(driverId, lat, lng) {
    const idx = this.shardIndexFor(lat, lng);
    const prevRaw = await this.meta.get(`driver:shard:${driverId}`);
    const prev = prevRaw === null ? null : parseInt(prevRaw, 10);
    if (prev !== null && prev !== idx) {
      await this.shards[prev].zrem(GEO_KEY, driverId); // left the old area
    }
    await this.shards[idx].geoadd(GEO_KEY, lng, lat, driverId);
    await this.meta.set(`driver:shard:${driverId}`, String(idx));

    // Surge supply signal: which distinct drivers pinged in this cell this
    // minute. SADD is idempotent per driver, so 15 pings = 1 unit of supply.
    const cell = cellForLocation(lat, lng, 6);
    const bucket = minuteBucket();
    await this.meta.sadd(`supply:${cell}:${bucket}`, driverId);
    await this.meta.expire(`supply:${cell}:${bucket}`, 180);
    await this.meta.sadd(`surge:cells:${bucket}`, cell);
    await this.meta.expire(`surge:cells:${bucket}`, 180);
  }

  async removeDriver(driverId) {
    const prevRaw = await this.meta.get(`driver:shard:${driverId}`);
    if (prevRaw !== null) await this.shards[parseInt(prevRaw, 10)].zrem(GEO_KEY, driverId);
    await this.meta.del(`driver:shard:${driverId}`);
  }

  /**
   * Find nearest ONLINE drivers. Fan out GEOSEARCH to every shard, merge by
   * distance, then filter by status in one pipelined MGET.
   */
  async findNearby(lat, lng, radiusM = 3000, limit = 10) {
    const perShard = await Promise.all(
      this.shards.map((r) =>
        r.geosearch(GEO_KEY, 'FROMLONLAT', lng, lat, 'BYRADIUS', radiusM, 'm',
          'ASC', 'COUNT', limit, 'WITHCOORD', 'WITHDIST')
          .catch(() => [])
      )
    );
    const merged = perShard
      .flat()
      .map(([driverId, dist, [dLng, dLat]]) => ({
        driverId, distM: Math.round(parseFloat(dist)), lat: parseFloat(dLat), lng: parseFloat(dLng)
      }))
      .sort((a, b) => a.distM - b.distM);

    if (!merged.length) return [];
    const statuses = await this.meta.mget(merged.map((c) => `driver:status:${c.driverId}`));
    const idles = await this.meta.mget(merged.map((c) => `driver:lastTripEnd:${c.driverId}`));
    const now = Date.now();
    return merged
      .map((c, i) => ({
        ...c,
        status: statuses[i] || 'offline',
        idleS: idles[i] ? Math.max(0, Math.floor((now - parseInt(idles[i], 10)) / 1000)) : 0
      }))
      .filter((c) => c.status === 'online')
      .slice(0, limit);
  }

  // ------------------------------------------------------------- status ---
  async setStatus(driverId, status, { tripId } = {}) {
    await this.meta.set(`driver:status:${driverId}`, status);
    if (status === 'on_trip' && tripId) {
      await this.meta.set(`driver:trip:${driverId}`, tripId);
    }
    if (status === 'online') {
      await this.meta.del(`driver:trip:${driverId}`);
      await this.meta.set(`driver:lastTripEnd:${driverId}`, String(Date.now()));
    }
    if (status === 'offline') {
      await this.meta.del(`driver:trip:${driverId}`);
      await this.removeDriver(driverId);
    }
  }

  getStatus(driverId) { return this.meta.get(`driver:status:${driverId}`); }
  getActiveTrip(driverId) { return this.meta.get(`driver:trip:${driverId}`); }

  async onlineCount() {
    // Dev-scale introspection for /health and metrics (KEYS is O(N); a prod
    // system would maintain a counter instead).
    const keys = await this.meta.keys('driver:status:*');
    if (!keys.length) return 0;
    const vals = await this.meta.mget(keys);
    return vals.filter((v) => v === 'online' || v === 'on_trip').length;
  }
}
