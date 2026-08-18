/**
 * Fare math — pure, integer-paise, unit-tested.
 *
 * Money rules (non-negotiable): amounts are integers in paise end-to-end.
 * Floats appear exactly once — the surge multiplier — and are eliminated by
 * a single Math.round at the end. Intermediate per-km / per-min components
 * are individually rounded so the breakdown always sums to the total (a
 * receipt whose lines don't add up is a support ticket).
 */
export const DEFAULT_RATES = {
  basePaise: 3000,     // Rs 30 flag-drop
  perKmPaise: 1500,    // Rs 15 / km
  perMinPaise: 200,    // Rs 2 / min
  minFarePaise: 5000,  // Rs 50 floor
  maxSurge: 3.0        // circuit-breaker cap on surge (ADR: bounded surge)
};

export function computeFare({ distanceM, durationS, surge = 1.0, rates = DEFAULT_RATES }) {
  const s = Math.min(Math.max(Number(surge) || 1, 1), rates.maxSurge);
  const distancePaise = Math.round((distanceM / 1000) * rates.perKmPaise);
  const timePaise = Math.round((durationS / 60) * rates.perMinPaise);
  const subtotalPaise = rates.basePaise + distancePaise + timePaise;
  const surgedPaise = Math.round(subtotalPaise * s);
  const farePaise = Math.max(rates.minFarePaise, surgedPaise);
  return {
    farePaise,
    breakdown: {
      basePaise: rates.basePaise,
      distancePaise,
      timePaise,
      surgeApplied: s,
      surgedPaise,
      minFareApplied: farePaise !== surgedPaise
    }
  };
}

/**
 * Supply/demand ratio -> multiplier. Piecewise-linear and capped: below
 * balance (d <= s) no surge; above it, each 1.0 of excess demand-per-driver
 * adds 0.5x, capped at maxSurge. Zero supply with nonzero demand = max.
 */
export function surgeFromRatio(demand, supply, maxSurge = DEFAULT_RATES.maxSurge) {
  if (demand <= 0) return 1.0;
  if (supply <= 0) return maxSurge;
  const ratio = demand / supply;
  const m = 1 + Math.max(0, ratio - 1) * 0.5;
  return Math.min(maxSurge, Math.round(m * 100) / 100);
}
