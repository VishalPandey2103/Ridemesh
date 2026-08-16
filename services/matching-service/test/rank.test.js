import test from 'node:test';
import assert from 'node:assert/strict';
import { scoreCandidate, rankCandidates } from '../src/rank.js';

test('closer driver scores better (lower) than farther, all else equal', () => {
  const near = scoreCandidate({ distM: 500, rating: 4.5, idleS: 0 });
  const far = scoreCandidate({ distM: 3000, rating: 4.5, idleS: 0 });
  assert.ok(near < far);
});

test('higher rating beats lower rating at equal distance', () => {
  const good = scoreCandidate({ distM: 1000, rating: 5.0, idleS: 0 });
  const bad = scoreCandidate({ distM: 1000, rating: 3.0, idleS: 0 });
  assert.ok(good < bad);
});

test('idle credit ramps linearly and saturates at 10 min idle', () => {
  const fresh = scoreCandidate({ distM: 1000, rating: 4.5, idleS: 0 });
  const idle = scoreCandidate({ distM: 1000, rating: 4.5, idleS: 60 });
  const atCap = scoreCandidate({ distM: 1000, rating: 4.5, idleS: 600 });
  const beyondCap = scoreCandidate({ distM: 1000, rating: 4.5, idleS: 6000 });
  assert.ok(idle < fresh);                 // waiting earns credit
  assert.equal(beyondCap, atCap);          // credit saturates at the 600s cap
  assert.equal(fresh - atCap, 90);         // max credit = 90s of ETA
});

test('rankCandidates sorts ascending by score', () => {
  const ranked = rankCandidates([
    { driverId: 'far', distM: 4000, rating: 4.5, idleS: 0 },
    { driverId: 'near', distM: 300, rating: 4.5, idleS: 0 },
    { driverId: 'mid', distM: 1500, rating: 4.5, idleS: 0 }
  ]);
  assert.deepEqual(ranked.map((c) => c.driverId), ['near', 'mid', 'far']);
});
