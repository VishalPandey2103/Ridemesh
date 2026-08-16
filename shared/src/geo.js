/**
 * Geospatial primitives, written from scratch so the mechanics are visible.
 *
 * GEOHASH — what it is and why we use it:
 * A geohash interleaves the binary expansions of longitude and latitude and
 * base32-encodes the bits. Two properties make it useful:
 *   1. Prefix = containment. Every point whose geohash starts with "tdr1w"
 *      lies inside the same ~5km x 5km rectangle. Truncating the hash zooms
 *      out; extending it zooms in. That gives us hierarchical "cells" for
 *      free — exactly what surge pricing needs (demand/supply per cell).
 *   2. Cells are stable string keys, so they work as Redis keys
 *      (`surge:tdr1w6`) and as shard-routing keys (hash the prefix, pick a
 *      Redis instance). H3 (Uber's hex grid) improves on edge-distortion and
 *      neighbor uniformity; geohash is the simpler mental model and drop-in
 *      replaceable via cellForLocation().
 *
 * Precision table (approx cell size at the equator):
 *   4 -> 39km, 5 -> 4.9km, 6 -> 1.2km, 7 -> 152m
 * We use precision 6 for surge cells (city-block-ish granularity) and the
 * 4-char prefix as the shard key (city-area granularity).
 */
const BASE32 = '0123456789bcdefghjkmnpqrstuvwxyz';

export function geohashEncode(lat, lng, precision = 6) {
  let idx = 0;
  let bit = 0;
  let evenBit = true; // start with longitude, per the geohash spec
  let hash = '';
  let latMin = -90, latMax = 90, lonMin = -180, lonMax = 180;

  while (hash.length < precision) {
    if (evenBit) {
      const mid = (lonMin + lonMax) / 2;
      if (lng >= mid) { idx = idx * 2 + 1; lonMin = mid; }
      else { idx = idx * 2; lonMax = mid; }
    } else {
      const mid = (latMin + latMax) / 2;
      if (lat >= mid) { idx = idx * 2 + 1; latMin = mid; }
      else { idx = idx * 2; latMax = mid; }
    }
    evenBit = !evenBit;
    if (++bit === 5) { // 5 bits per base32 character
      hash += BASE32[idx];
      bit = 0;
      idx = 0;
    }
  }
  return hash;
}

/** The cell abstraction the rest of the system talks to. Swap to H3 here. */
export function cellForLocation(lat, lng, precision = 6) {
  return geohashEncode(lat, lng, precision);
}

/** Great-circle distance in meters (spherical earth, fine for city scale). */
export function haversineM(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return Math.round(2 * R * Math.asin(Math.sqrt(a)));
}

/** 60-second bucket id — surge demand/supply counters are keyed per bucket. */
export function minuteBucket(ts = Date.now()) {
  return Math.floor(ts / 60000);
}

/** Bounding box used by simulators and k6 (roughly Bengaluru). */
export const BANGALORE_BBOX = { latMin: 12.85, latMax: 13.10, lngMin: 77.45, lngMax: 77.75 };

export function randomPointInBBox(bbox = BANGALORE_BBOX) {
  return {
    lat: bbox.latMin + Math.random() * (bbox.latMax - bbox.latMin),
    lng: bbox.lngMin + Math.random() * (bbox.lngMax - bbox.lngMin)
  };
}
