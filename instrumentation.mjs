/**
 * OpenTelemetry bootstrap, loaded before any service code via
 * `node --import ./instrumentation.mjs src/server.js`.
 *
 * Auto-instrumentation monkey-patches http/express/pg/ioredis/amqplib at
 * module-load time, which is why this must run FIRST — it injects/extracts
 * the W3C `traceparent` header on every outbound/inbound hop, so one ride's
 * gateway->trip->pricing->rabbit->matching->payment path stitches into a
 * single Jaeger trace without touching business code.
 *
 * No-op unless TRACING_ENABLED=true, so unit tests and bare `node` runs
 * stay dependency-free.
 */
if (process.env.TRACING_ENABLED === 'true') {
  const { NodeSDK } = await import('@opentelemetry/sdk-node');
  const { getNodeAutoInstrumentations } = await import('@opentelemetry/auto-instrumentations-node');
  const { OTLPTraceExporter } = await import('@opentelemetry/exporter-trace-otlp-http');

  const sdk = new NodeSDK({
    serviceName: process.env.OTEL_SERVICE_NAME || 'ridemesh-service',
    traceExporter: new OTLPTraceExporter({
      url: process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://jaeger:4318/v1/traces'
    }),
    instrumentations: [getNodeAutoInstrumentations({
      '@opentelemetry/instrumentation-fs': { enabled: false } // noisy, useless spans
    })]
  });
  sdk.start();
  process.on('SIGTERM', () => sdk.shutdown().finally(() => process.exit(0)));
}
