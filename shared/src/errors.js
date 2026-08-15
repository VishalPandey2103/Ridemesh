export class ApiError extends Error {
  constructor(status, message, details) {
    super(message);
    this.status = status;
    this.details = details;
  }
}

// Wrap async route handlers so thrown/rejected errors reach errorHandler
// instead of crashing the process (Express 4 does not catch async rejections).
export const asyncHandler = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

export function errorHandler(logger) {
  return (err, req, res, _next) => {
    const status = err.status || 500;
    if (status >= 500) logger.error({ reqId: req.id, err: err.message, stack: err.stack }, 'unhandled error');
    res.status(status).json({ error: err.message || 'Internal error', details: err.details });
  };
}

export function notFound(req, res) {
  res.status(404).json({ error: `No route ${req.method} ${req.path}` });
}
