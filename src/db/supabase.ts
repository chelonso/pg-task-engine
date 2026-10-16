import type { DatabaseAdapter, JobRecord } from '../types.js';

export interface SupabaseClientLike {
  rpc(
    fn: string,
    args?: Record<string, unknown>
  ): Promise<{ data: unknown; error: unknown }>;
  from(table: string): any;
  schema?(schema: string): any;
}

export function createSupabaseAdapter(
  client: SupabaseClientLike,
  options?: { schema?: string }
): DatabaseAdapter<SupabaseClientLike> {
  const schema = options?.schema || 'common';

  const getTable = (table: string) => {
    return client.schema ? client.schema(schema).from(table) : client.from(table);
  };

  return {
    raw: client,

    async rpc<T = unknown>(
      functionName: string,
      args?: Record<string, unknown>
    ): Promise<{ data: T | null; error: Error | null }> {
      const res = await client.rpc(functionName, args);
      if (res.error) {
        const err = res.error instanceof Error ? res.error : new Error(String((res.error as any)?.message || res.error));
        return { data: null, error: err };
      }
      return { data: res.data as T, error: null };
    },

    async enqueueJob(opts): Promise<{ id: string }> {
      const { data, error } = await this.rpc<string>('fn_enqueue_job', {
        p_tenant_id: opts.tenantId,
        p_target_path: opts.targetPath,
        p_payload: opts.payload ?? {},
        p_run_at: opts.runAt ? new Date(opts.runAt).toISOString() : new Date().toISOString(),
        p_max_attempts: opts.maxAttempts ?? 3
      });

      if (error || !data) {
        // Fallback: direct table insert
        const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : new Date().toISOString();
        const res = await getTable('queued_jobs')
          .insert({
            tenant_id: opts.tenantId,
            target_path: opts.targetPath,
            payload: opts.payload ?? {},
            run_at: runAt,
            max_attempts: opts.maxAttempts ?? 3,
            status: 'pending'
          })
          .select('id')
          .single();

        if (res.error) throw new Error(`Failed to enqueue job: ${res.error.message}`);
        return { id: res.data.id };
      }

      return { id: data };
    },

    async removeJob(jobId: string): Promise<void> {
      const res = await getTable('queued_jobs').delete().eq('id', jobId);
      if (res.error) {
        throw new Error(`Failed to remove job ${jobId}: ${res.error.message}`);
      }
    },

    async rescheduleJob(opts): Promise<void> {
      const res = await getTable('queued_jobs')
        .update({
          status: 'pending',
          attempts: opts.attempts,
          run_at: new Date(opts.runAt).toISOString(),
          updated_at: new Date().toISOString()
        })
        .eq('id', opts.jobId);

      if (res.error) {
        throw new Error(`Failed to reschedule job ${opts.jobId}: ${res.error.message}`);
      }
    },

    async recordJobLog(log): Promise<void> {
      const res = await getTable('queued_job_logs').insert({
        job_id: log.jobId,
        tenant_id: log.tenantId,
        target_path: log.targetPath,
        status: log.status,
        started_at: log.startedAt,
        finished_at: log.finishedAt,
        duration_ms: log.durationMs,
        response_payload: log.responsePayload ?? null,
        error_message: log.errorMessage ?? null,
        attempts: log.attempts
      });

      if (res.error) {
        throw new Error(`Failed to record queue log: ${res.error.message}`);
      }
    },

    async getJob(jobId: string): Promise<JobRecord | null> {
      const res = await getTable('queued_jobs').select('*').eq('id', jobId).maybeSingle();
      if (res.error) throw new Error(`Failed to fetch job ${jobId}: ${res.error.message}`);
      return (res.data as JobRecord) || null;
    },

    async cancelJob(jobId: string, tenantId?: string): Promise<boolean> {
      let query = getTable('queued_jobs').delete().eq('id', jobId).eq('status', 'pending');
      if (tenantId) {
        query = query.eq('tenant_id', tenantId);
      }
      const res = await query.select('id');
      if (res.error) throw new Error(`Failed to cancel job: ${res.error.message}`);
      return (res.data && res.data.length > 0) || false;
    },

    async recordScheduledLog(log): Promise<{ id: string }> {
      const res = await getTable('scheduled_job_logs')
        .insert({
          scheduled_job_id: log.scheduledJobId,
          tenant_id: log.tenantId,
          target_path: log.targetPath,
          status: log.status,
          triggered_by: log.triggeredBy,
          started_at: log.startedAt,
          finished_at: log.finishedAt ?? null,
          duration_ms: log.durationMs ?? null,
          response_payload: log.responsePayload ?? null,
          error_message: log.errorMessage ?? null
        })
        .select('id')
        .single();

      if (res.error) throw new Error(`Failed to record scheduled log: ${res.error.message}`);
      return { id: res.data.id };
    }
  };
}
