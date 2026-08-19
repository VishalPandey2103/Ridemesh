# One image for all eight services. Compose picks the entrypoint per service
# via `command:`. Trade-off: bigger image than per-service builds, but one
# build, one cache, zero drift between services — right call for a monorepo
# where every service shares the same shared/ module anyway.
FROM node:20-alpine
WORKDIR /app

# Install with only manifests present -> Docker layer cache survives source
# edits; npm re-runs only when a package.json changes.
COPY package.json package-lock.json* ./
COPY shared/package.json ./shared/
COPY services/gateway/package.json ./services/gateway/
COPY services/user-service/package.json ./services/user-service/
COPY services/location-service/package.json ./services/location-service/
COPY services/matching-service/package.json ./services/matching-service/
COPY services/trip-service/package.json ./services/trip-service/
COPY services/pricing-service/package.json ./services/pricing-service/
COPY services/payment-service/package.json ./services/payment-service/
COPY services/notification-service/package.json ./services/notification-service/
RUN npm install --workspaces --include-workspace-root --omit=dev 2>/dev/null || npm install --omit=dev

COPY . .
ENV NODE_ENV=production
