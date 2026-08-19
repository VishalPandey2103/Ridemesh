/**
 * Payment provider abstraction. Two implementations behind one interface:
 *
 *   mock     — default. Simulates latency and a configurable failure rate so
 *              the retry/outbox/refund machinery is exercisable offline.
 *   razorpay — real Razorpay REST (test mode) via fetch + HTTP basic auth
 *              (key_id:key_secret). Creates an Order, then simulates capture
 *              (a real client app would complete checkout; server-side we
 *              demonstrate the order+capture shape and refunds). Selected
 *              automatically when RAZORPAY_KEY_ID/SECRET are present.
 *
 * The interface is deliberately tiny: charge() and refund(), both taking a
 * `receipt` (our trip-scoped idempotent id). Provider-level idempotency
 * (Razorpay honors receipt uniqueness on orders) is the SECOND line of
 * defense; the first is our payments(trip_id, kind) UNIQUE constraint.
 */
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

export function createMockProvider({ failRate = 0.1 } = {}) {
  return {
    name: 'mock',
    async charge({ amountPaise, receipt }) {
      await sleep(150 + Math.random() * 250); // network-ish latency
      if (Math.random() < failRate) {
        const err = new Error('mock gateway declined (simulated)');
        err.retryable = true;
        throw err;
      }
      return { providerRef: `mock_pay_${receipt}_${Date.now()}` };
    },
    async refund({ providerRef }) {
      await sleep(120);
      return { providerRef: `mock_rfnd_${providerRef}` };
    }
  };
}

export function createRazorpayProvider({ keyId, keySecret }) {
  const auth = 'Basic ' + Buffer.from(`${keyId}:${keySecret}`).toString('base64');
  const base = 'https://api.razorpay.com/v1';
  async function rz(path, body) {
    const res = await fetch(base + path, {
      method: 'POST',
      headers: { authorization: auth, 'content-type': 'application/json' },
      body: JSON.stringify(body)
    });
    const json = await res.json();
    if (!res.ok) {
      const err = new Error(json?.error?.description || `razorpay ${res.status}`);
      err.retryable = res.status >= 500; // 4xx (bad key, bad amount) won't heal by retrying
      throw err;
    }
    return json;
  }
  return {
    name: 'razorpay',
    async charge({ amountPaise, receipt }) {
      const order = await rz('/orders', {
        amount: amountPaise, currency: 'INR', receipt, payment_capture: 1
      });
      return { providerRef: order.id };
    },
    async refund({ providerRef, amountPaise }) {
      const r = await rz(`/payments/${providerRef}/refund`, { amount: amountPaise });
      return { providerRef: r.id };
    }
  };
}

export function selectProvider(env, logger) {
  if (env.RAZORPAY_KEY_ID && env.RAZORPAY_KEY_SECRET) {
    logger.info('payment provider: razorpay (test mode)');
    return createRazorpayProvider({ keyId: env.RAZORPAY_KEY_ID, keySecret: env.RAZORPAY_KEY_SECRET });
  }
  logger.info('payment provider: mock');
  return createMockProvider({ failRate: parseFloat(process.env.MOCK_FAIL_RATE || '0.1') });
}
