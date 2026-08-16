import Redis from 'ioredis';

export function createRedis(url, opts = {}) {
  const client = new Redis(url, { maxRetriesPerRequest: null, ...opts });
  client.on('error', (e) => console.error('[redis]', e.message));
  return client;
}
