import jwt from 'jsonwebtoken';
import { ApiError } from './errors.js';

const SECRET = () => process.env.JWT_SECRET;

export function signToken(user) {
  // sub = user id. Role drives gateway-level authorization and socket routing.
  return jwt.sign({ sub: user.id, role: user.role, name: user.name }, SECRET(), { expiresIn: '2d' });
}

export function verifyToken(token) {
  return jwt.verify(token, SECRET());
}

// Services behind the gateway trust the identity headers the gateway injects
// after verifying the JWT once at the edge (verify-once pattern).
export function userFromHeaders(req, _res, next) {
  const id = req.headers['x-user-id'];
  const role = req.headers['x-user-role'];
  if (id) req.user = { id, role };
  next();
}

export function requireUser(req, _res, next) {
  if (!req.user) return next(new ApiError(401, 'Authentication required'));
  next();
}

export function requireRole(role) {
  return (req, _res, next) => {
    if (!req.user) return next(new ApiError(401, 'Authentication required'));
    if (req.user.role !== role) return next(new ApiError(403, `Requires role ${role}`));
    next();
  };
}
