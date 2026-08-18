import test from 'node:test';
import assert from 'node:assert/strict';
import { computeFare, surgeFromRatio, DEFAULT_RATES } from '../src/fare.js';

test('base fare math: 5km, 15min, no surge', () => {
  const { farePaise, breakdown } = computeFare({ distanceM: 5000, durationS: 900 });
  // 3000 + 5*1500 + 15*200 = 3000 + 7500 + 3000 = 13500
  assert.equal(farePaise, 13500);
  assert.equal(breakdown.distancePaise, 7500);
  assert.equal(breakdown.timePaise, 3000);
});

test('surge multiplies the subtotal, rounded to integer paise', () => {
  const { farePaise } = computeFare({ distanceM: 5000, durationS: 900, surge: 1.5 });
  assert.equal(farePaise, Math.round(13500 * 1.5));
});

test('surge is clamped to maxSurge', () => {
  const capped = computeFare({ distanceM: 5000, durationS: 900, surge: 99 });
  const atMax = computeFare({ distanceM: 5000, durationS: 900, surge: DEFAULT_RATES.maxSurge });
  assert.equal(capped.farePaise, atMax.farePaise);
});

test('minimum fare floor applies to short trips', () => {
  const { farePaise, breakdown } = computeFare({ distanceM: 300, durationS: 60 });
  assert.equal(farePaise, DEFAULT_RATES.minFarePaise);
  assert.equal(breakdown.minFareApplied, true);
});

test('fare is always an integer (no float paise ever)', () => {
  for (const surge of [1, 1.1, 1.37, 2.9]) {
    const { farePaise } = computeFare({ distanceM: 3333, durationS: 777, surge });
    assert.ok(Number.isInteger(farePaise));
  }
});

test('surgeFromRatio: balanced or oversupplied market has no surge', () => {
  assert.equal(surgeFromRatio(5, 10), 1.0);
  assert.equal(surgeFromRatio(10, 10), 1.0);
  assert.equal(surgeFromRatio(0, 0), 1.0);
});

test('surgeFromRatio: excess demand raises multiplier, capped at max', () => {
  assert.equal(surgeFromRatio(20, 10), 1.5);  // 2x demand -> 1.5
  assert.equal(surgeFromRatio(30, 10), 2.0);  // 3x demand -> 2.0
  assert.equal(surgeFromRatio(500, 10), 3.0); // capped
  assert.equal(surgeFromRatio(7, 0), 3.0);    // zero supply -> max
});
