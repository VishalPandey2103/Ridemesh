import test from 'node:test';
import assert from 'node:assert/strict';
import { geohashEncode, cellForLocation, haversineM } from '../src/geo.js';

test('geohash matches known reference values', () => {
  // Canonical spec test vector (Jutland, Denmark)
  assert.equal(geohashEncode(57.64911, 10.40744, 11), 'u4pruydqqvj');
  // MG Road, Bangalore (verified against an independent implementation)
  assert.equal(geohashEncode(12.9758, 77.6045, 6), 'tdr1vf');
});

test('nearby points share a prefix, far points do not', () => {
  const a = geohashEncode(12.9758, 77.6045, 6);
  const b = geohashEncode(12.9760, 77.6047, 6); // ~30m away
  const c = geohashEncode(28.6139, 77.2090, 6); // Delhi
  assert.equal(a, b);
  assert.notEqual(a.slice(0, 3), c.slice(0, 3));
});

test('cellForLocation returns precision-6 cell', () => {
  assert.equal(cellForLocation(12.9758, 77.6045).length, 6);
});

test('haversine is accurate for a known pair', () => {
  // MG Road -> Koramangala ~= 5.5-6.5 km straight line
  const d = haversineM(12.9758, 77.6045, 12.9352, 77.6245);
  assert.ok(d > 4500 && d < 6500, `got ${d}`);
});

test('haversine of identical points is zero', () => {
  assert.equal(Math.round(haversineM(12.9758, 77.6045, 12.9758, 77.6045)), 0);
});
