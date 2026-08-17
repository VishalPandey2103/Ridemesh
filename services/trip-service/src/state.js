/**
 * Trip lifecycle state machine.
 *
 * Encoding legal transitions as data (not scattered ifs) gives one place to
 * audit, one place to test, and makes illegal transitions impossible rather
 * than improbable. The DB CHECK constraint is the last line of defense; this
 * map is the first.
 *
 *   requested -> matched | cancelled | no_drivers
 *   matched   -> driver_arriving | cancelled
 *   driver_arriving -> in_progress | cancelled
 *   in_progress -> completed          (no cancel mid-ride in this model)
 *   completed / cancelled / no_drivers are terminal.
 */
export const TRANSITIONS = {
  requested: ['matched', 'cancelled', 'no_drivers'],
  matched: ['driver_arriving', 'cancelled'],
  driver_arriving: ['in_progress', 'cancelled'],
  in_progress: ['completed'],
  completed: [],
  cancelled: [],
  no_drivers: []
};

export function canTransition(from, to) {
  return (TRANSITIONS[from] || []).includes(to);
}

/** Statuses in which a rider cancel incurs a fee (driver already en route). */
export const FEE_ON_CANCEL = new Set(['matched', 'driver_arriving']);
