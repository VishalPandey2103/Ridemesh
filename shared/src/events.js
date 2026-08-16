// Single source of truth for the event-driven layer.
// One topic exchange; routing keys are namespaced "domain.event".
export const EXCHANGE = 'ride.events';

export const EVENTS = {
  RIDE_REQUESTED: 'ride.requested',            // trip-service   -> matching-service
  DRIVER_ASSIGNED: 'ride.driver_assigned',     // matching       -> trip-service
  RIDE_UNMATCHED: 'ride.unmatched',            // matching       -> trip-service (no drivers)
  TRIP_COMPLETED: 'trip.completed',            // trip-service   -> payment-service
  TRIP_CANCELLED: 'trip.cancelled',            // trip-service   -> matching + payment (cancellation fee)
  PAYMENT_CAPTURED: 'payment.captured',        // payment        -> trip-service + notification
  PAYMENT_FAILED: 'payment.failed',
  PAYMENT_REFUNDED: 'payment.refunded'
};
