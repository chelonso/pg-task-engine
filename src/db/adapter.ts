import type { DatabaseAdapter } from '../types.js';

/**
 * Normalizes input db into a full DatabaseAdapter.
 * If input already implements DatabaseAdapter, returns it.
 * If it has .rpc, wraps it as a Supabase-compatible adapter.
 */
export function resolveDatabaseAdapter(db: unknown): DatabaseAdapter {
  if (!db || typeof db !== 'object') {
    throw new Error('TaskEngine: Invalid database client provided.');
  }

  // Already a conforming DatabaseAdapter
  if ('rpc' in db && typeof (db as DatabaseAdapter).rpc === 'function') {
    return db as DatabaseAdapter;
  }

  throw new Error(
    'TaskEngine: Provided db instance does not implement DatabaseAdapter. Use createSupabaseAdapter, createPrismaAdapter, createDrizzleAdapter, or createPgAdapter.'
  );
}
