import client from 'prom-client';

/**
 * Prometheus metrics, identical across services so one Grafana dashboard
 * templates over `service`. prom-client keeps counters in-process; Prometheus
 * scrapes GET /metrics on each service every 5s (see infra/prometheus.yml).
 */
export function initMetrics(service) {
  const register = new client.Registry();
  register.setDefaultLabels({ service });
  client.collectDefaultMetrics({ register }); // event loop lag, heap, CPU

  const httpRequests = new client.Counter({
    name: 'http_requests_total',
    help: 'HTTP requests',
    labelNames: ['service', 'method', 'route', 'status'],
    registers: [register]
  });
  const httpDuration = new client.Histogram({
    name: 'http_request_duration_seconds',
    help: 'HTTP request latency',
    labelNames: ['service', 'method', 'route', 'status'],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5],
    registers: [register]
  });

  const middleware = (req, res, next) => {
    const end = httpDuration.startTimer();
    res.on('finish', () => {
      const route = req.route?.path ? req.baseUrl + req.route.path : req.path;
      const labels = { service, method: req.method, route, status: res.statusCode };
      httpRequests.inc(labels);
      end(labels);
    });
    next();
  };

  const metricsHandler = async (_req, res) => {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
  };

  // Business metrics are registered by services on this same registry.
  const counter = (name, help, labelNames = []) =>
    new client.Counter({ name, help, labelNames: ['service', ...labelNames], registers: [register] });
  const gauge = (name, help, labelNames = []) =>
    new client.Gauge({ name, help, labelNames: ['service', ...labelNames], registers: [register] });

  return { register, middleware, metricsHandler, counter, gauge, service };
}
