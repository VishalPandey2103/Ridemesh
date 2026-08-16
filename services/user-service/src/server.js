import express from 'express';
import bcrypt from 'bcryptjs';
import pg from 'pg';
import { createLogger, httpLogger } from '@ridemesh/shared/src/logger.js';
import { requireEnv } from '@ridemesh/shared/src/env.js';
import { signToken, userFromHeaders } from '@ridemesh/shared/src/auth.js';
import { ApiError, asyncHandler, errorHandler, notFound } from '@ridemesh/shared/src/errors.js';
import { initMetrics } from '@ridemesh/shared/src/metrics.js';

/**
 * User Service — owns identity for both entity types.
 *
 * Riders and drivers share the users table but register differently: a rider
 * needs only name/phone/password, while a driver additionally submits
 * vehicle_no + license_no and starts in kyc_status='pending'.
 */
const env = requireEnv(['PORT', 'DATABASE_URL', 'JWT_SECRET']);
const logger = createLogger('user-service');
const pool = new pg.Pool({ connectionString: env.DATABASE_URL, max: 10 });
const metrics = initMetrics('user-service');

const app = express();
app.use(express.json());
app.use(httpLogger(logger));
app.use(metrics.middleware);
app.use(userFromHeaders);
app.get('/metrics', metrics.metricsHandler);
app.get('/health', (_req, res) => res.json({ ok: true, service: 'user-service' }));

// ---------------------------------------------------------------- register
app.post('/api/auth/register', asyncHandler(async (req, res) => {
  const { role, name, phone, password, vehicleNo, licenseNo } = req.body || {};
  if (!['rider', 'driver'].includes(role)) throw new ApiError(400, 'role must be rider|driver');
  if (!name || !phone || !password) throw new ApiError(400, 'name, phone, password required');
  if (role === 'driver' && (!vehicleNo || !licenseNo)) {
    throw new ApiError(400, 'drivers must provide vehicleNo and licenseNo');
  }

  // bcrypt with cost 10: ~100ms of deliberate slowness so a leaked hash dump
  // can't be brute-forced at GPU speed. Never store plaintext or fast hashes.
  const hash = await bcrypt.hash(password, 10);
  try {
    const { rows } = await pool.query(
      `INSERT INTO users (role, name, phone, password_hash, vehicle_no, license_no, kyc_status)
       VALUES ($1,$2,$3,$4,$5,$6,$7)
       RETURNING id, role, name, phone, kyc_status, rating`,
      [role, name, phone, hash,
       role === 'driver' ? vehicleNo : null,
       role === 'driver' ? licenseNo : null,
       role === 'driver' ? 'pending' : 'not_applicable']
    );
    const user = rows[0];
    res.status(201).json({ user, token: signToken(user) });
  } catch (e) {
    if (e.code === '23505') throw new ApiError(409, 'phone already registered');
    throw e;
  }
}));

// ------------------------------------------------------------------- login
app.post('/api/auth/login', asyncHandler(async (req, res) => {
  const { phone, password } = req.body || {};
  const { rows } = await pool.query(`SELECT * FROM users WHERE phone = $1`, [phone]);
  const user = rows[0];
  // Same error for "no such user" and "bad password" => no account enumeration.
  if (!user || !(await bcrypt.compare(password || '', user.password_hash))) {
    throw new ApiError(401, 'Invalid credentials');
  }
  const { password_hash, ...safe } = user;
  res.json({ user: safe, token: signToken(user) });
}));

app.use(notFound);
app.use(errorHandler(logger));
app.listen(env.PORT, () => logger.info({ port: env.PORT }, 'user-service listening'));
