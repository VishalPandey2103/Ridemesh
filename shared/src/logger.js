import pino from 'pino';

export function createLogger(service) {
  return pino({
    name: service,
    level: process.env.LOG_LEVEL || 'info',
    base: { service },
    timestamp: pino.stdTimeFunctions.isoTime
  });
}

// Minimal structured HTTP access log with request-id propagation.
// The gateway generates x-request-id; every service echoes it so one ride
// can be grepped across all services (poor man's tracing; Jaeger adds spans).
export function httpLogger(logger) {
  return (req, res, next) => {
    req.id = req.headers['x-request-id'] || crypto.randomUUID();
    res.setHeader('x-request-id', req.id);
    const start = process.hrtime.bigint();
    res.on('finish', () => {
      const ms = Number(process.hrtime.bigint() - start) / 1e6;
      logger.info({ reqId: req.id, method: req.method, url: req.originalUrl, status: res.statusCode, ms: +ms.toFixed(1) }, 'http');
    });
    next();
  };
}
