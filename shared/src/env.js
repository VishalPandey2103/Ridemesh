// Fail-fast environment validation: a service that boots with missing config
// will fail in confusing ways later. Crash immediately at startup instead.
export function requireEnv(keys) {
  const missing = keys.filter((k) => !process.env[k]);
  if (missing.length) {
    console.error(`[env] FATAL: missing required env vars: ${missing.join(', ')}`);
    process.exit(1);
  }
  const out = {};
  for (const k of keys) out[k] = process.env[k];
  return out;
}

export function envInt(key, fallback) {
  const v = process.env[key];
  return v === undefined ? fallback : parseInt(v, 10);
}
