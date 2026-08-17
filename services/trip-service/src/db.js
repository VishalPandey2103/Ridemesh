import pg from 'pg';

export function createPool(databaseUrl) {
  return new pg.Pool({ connectionString: databaseUrl, max: 10 });
}

/**
 * Consumer-side exactly-once effect: claim the messageId and run the domain
 * mutation in ONE transaction. If the claim fails (duplicate), skip; if the
 * handler throws, both the claim and the mutation roll back together, so the
 * redelivery gets a clean retry. This is the DB-backed sibling of the Redis
 * seenBefore() helper, and the correct choice whenever a Postgres write is
 * involved anyway.
 */
export async function processedOnce(pool, messageId, fn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rowCount } = await client.query(
      `INSERT INTO processed_messages (message_id) VALUES ($1)
       ON CONFLICT (message_id) DO NOTHING`,
      [messageId]
    );
    if (rowCount === 0) { // already processed
      await client.query('ROLLBACK');
      return false;
    }
    await fn(client);
    await client.query('COMMIT');
    return true;
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}
