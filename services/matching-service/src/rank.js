/**
 * Candidate ranking — pure function so it's trivially unit-testable and the
 * scoring policy can evolve without touching dispatch plumbing.
 *
 * Lower score = dispatched first. Three signals:
 *   1. ETA proxy: straight-line distance / assumed 6 m/s urban speed. Real
 *      systems (Uber DISCO) use routing-engine ETAs; distance is the honest
 *      dev-scale stand-in and the term is isolated so it can be swapped.
 *   2. Rating: each star below 5.0 costs 60s of equivalent ETA. Keeps a
 *      4.9 driver 200m further away competitive with a 4.2 driver next door.
 *   3. Idle fairness: up to 90s of ETA credit for drivers who've waited
 *      longest (capped at 10 min). Without this, drivers parked at hotspot
 *      edges starve — a real marketplace-health concern, not a nicety.
 */
export function scoreCandidate({ distM, rating = 4.5, idleS = 0 }) {
  const etaS = distM / 6;
  const ratingPenaltyS = (5 - Math.min(rating, 5)) * 60;
  const idleCreditS = Math.min(idleS, 600) / 600 * 90;
  return etaS + ratingPenaltyS - idleCreditS;
}

export function rankCandidates(candidates) {
  return [...candidates]
    .map((c) => ({ ...c, score: scoreCandidate(c) }))
    .sort((a, b) => a.score - b.score);
}
