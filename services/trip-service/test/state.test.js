import test from 'node:test';
import assert from 'node:assert/strict';
import { canTransition, TRANSITIONS, FEE_ON_CANCEL } from '../src/state.js';

test('happy path transitions are all legal', () => {
  assert.ok(canTransition('requested', 'matched'));
  assert.ok(canTransition('matched', 'driver_arriving'));
  assert.ok(canTransition('driver_arriving', 'in_progress'));
  assert.ok(canTransition('in_progress', 'completed'));
});

test('illegal jumps are rejected', () => {
  assert.ok(!canTransition('requested', 'completed'));
  assert.ok(!canTransition('requested', 'in_progress'));
  assert.ok(!canTransition('matched', 'completed'));
  assert.ok(!canTransition('in_progress', 'cancelled')); // no mid-ride cancel
});

test('terminal states have no exits', () => {
  for (const s of ['completed', 'cancelled', 'no_drivers']) {
    assert.equal(TRANSITIONS[s].length, 0);
  }
});

test('cancellation fee applies only after a driver is committed', () => {
  assert.ok(!FEE_ON_CANCEL.has('requested'));
  assert.ok(FEE_ON_CANCEL.has('matched'));
  assert.ok(FEE_ON_CANCEL.has('driver_arriving'));
});
