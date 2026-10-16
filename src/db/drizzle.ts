import type { DatabaseAdapter, JobRecord } from '../types.js';

export interface DrizzleClientLike {
  execute<T = any>(query: any): Promise<{ rows?: T[]; [key: string]: any }>;
}

export function createDrizzleAdapter(
  drizzleDb: DrizzleClientLike,
  options?: { schema?: string }
): DatabaseAdapter<DrizzleClientLike> {
  const schema = options?.schema || 'common';

  const runSql = async <T = any>(sqlQuery: string, params: any[] = []): Promise<T[]> => {
    // Drizzle execute accepts query object or raw string depending on driver
    const result: any = await drizzleDb.execute({
      sql: sqlQuery,
      params
    });

    if (Array.isArray(result)) return result;
    if (result && Array.isArray(result.rows)) return result.rows;
    return [];
  };

  return {
    raw: drizzleDb,

    async rpc<T = unknown>(
      functionName: string,
      args?: Record<string, unknown>
    ): Promise<{ data: T | null; error: Error | null }> {
      try {
        if (!args || Object.keys(args).length === 0) {
          const rows = await runSql(`SELECT * FROM ${schema}.${functionName}()`);
          return { data: (rows && rows.length > 0 ? rows : null) as T, error: null };
        }

        const keys = Object.keys(args);
        const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
        const values = Object.values(args).map((val) =>
          typeof val === 'object' && val !== null ? JSON.stringify(val) : val
        );

        const rows = await runSql(
          `SELECT * FROM ${schema}.${functionName}(${placeholders})`,
          values
        );
        return { data: (rows && rows.length > 0 ? rows : null) as T, error: null };
      } catch (err: any) {
        return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
      }
    },

    async enqueueJob(opts): Promise<{ id: string }> {
      const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : new Date().toISOString();
      const payloadJson = JSON.stringify(opts.payload ?? {});

      const rows = await runSql<{ id: string }>(
        `INSERT INTO ${schema}.queued_jobs (tenant_id, target_path, payload, run_at, max_attempts, status)
         VALUES ($1::uuid, $2, $3::jsonb, $4::timestamptz, $5, 'pending')
         RETURNING id`,
        [opts.tenantId, opts.targetPath, payloadJson, runAt, opts.maxAttempts ?? 3]
      );

      if (!rows || rows.length === 0) throw new Error('DrizzleAdapter: Failed to enqueue job.');
      return { id: rows[0].id };
    },

    async removeJob(jobId: string): Promise<void> {
      await runSql(`DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid`, [jobId]);
    },

    async rescheduleJob(opts): Promise<void> {
      await runSql(
        `UPDATE ${schema}.queued_jobs
         SET status = 'pending', attempts = $1, run_at = $2::timestamptz, updated_at = NOW()
         WHERE id = $3::uuid`,
        [opts.attempts, new Date(opts.runAt).toISOString(), opts.jobId]
      );
    },

    async recordJobLog(log): Promise<void> {
      await runSql(
        `INSERT INTO ${schema}.queued_job_logs 
         (job_id, tenant_id, target_path, status, started_at, finished_at, duration_ms, response_payload, error_message, attempts)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8::jsonb, $9, $10)`,
        [
          log.jobId,
          log.tenantId,
          log.targetPath,
          log.status,
          log.startedAt,
          log.finishedAt,
          log.durationMs,
          log.responsePayload ? JSON.stringify(log.responsePayload) : null,
          log.errorMessage ?? null,
          log.attempts
        ]
      );
    },

    async getJob(jobId: string): Promise<JobRecord | null> {
      const rows = await runSql<JobRecord>(
        `SELECT * FROM ${schema}.queued_jobs WHERE id = $1::uuid LIMIT 1`,
        [jobId]
      );
      return rows && rows.length > 0 ? rows[0] : null;
    },

    async cancelJob(jobId: string, tenantId?: string): Promise<boolean> {
      if (tenantId) {
        await runSql(
          `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid AND tenant_id = $2::uuid AND status = 'pending'`,
          [jobId, tenantId]
        );
        return true;
      }
      await runSql(
        `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid AND status = 'pending'`,
        [jobId]
      );
      return true;
    },

    async recordScheduledLog(log): Promise<{ id: string }> {
      const rows = await runSql<{ id: string }>(
        `INSERT INTO ${schema}.scheduled_job_logs
         (scheduled_job_id, tenant_id, target_path, status, triggered_by, started_at, finished_at, duration_ms, response_payload, error_message)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6::timestamptz, $7::timestamptz, $8, $9::jsonb, $10)
         RETURNING id`,
        [
          log.scheduledJobId,
          log.tenantId,
          log.targetPath,
          log.status,
          log.triggeredBy,
          log.startedAt,
          log.finishedAt ?? null,
          log.durationMs ?? null,
          log.responsePayload ? JSON.stringify(log.responsePayload) : null,
          log.errorMessage ?? null
        ]
      );
      return { id: rows[0].id };
    }
  };
}
