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
      if (log.id) {
        const res = await getTable('scheduled_job_logs')
          .update({
            status: log.status,
            finished_at: log.finishedAt ?? new Date().toISOString(),
            duration_ms: log.durationMs ?? null,
            response_payload: log.responsePayload ?? null,
            error_message: log.errorMessage ?? null
          })
          .eq('id', log.id)
          .select('id')
          .single();

        if (res.error) throw new Error(`Failed to update scheduled log: ${res.error.message}`);

        // Update scheduled_jobs table status and last run
        await getTable('scheduled_jobs')
          .update({
            status: log.status,
            last_run_at: log.finishedAt ?? new Date().toISOString()
          })
          .eq('id', log.scheduledJobId);

        return { id: log.id };
      }

      const res = await getTable('scheduled_job_logs')
        .insert({
          job_id: log.scheduledJobId,
          tenant_id: log.tenantId,
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

export interface SupabaseRestAdapterOptions {
  url: string;
  serviceKey: string;
  schema?: string;
}

export interface SupabaseRestClient {
  url: string;
  serviceKey: string;
  query<T = any>(
    path: string,
    options?: {
      schema?: string;
      method?: string;
      headers?: Record<string, string>;
      body?: any;
    }
  ): Promise<T>;
  rpc<T = any>(
    fn: string,
    args?: Record<string, unknown>,
    options?: { schema?: string }
  ): Promise<{ data: T | null; error: Error | null }>;
}

/**
 * Agnostic Supabase REST Adapter using native Web Fetch API.
 * Requires zero external dependencies. Ideal for lightweight services and microservices.
 */
export function createSupabaseRestAdapter(
  options: SupabaseRestAdapterOptions
): DatabaseAdapter<SupabaseRestClient> {
  const { url, serviceKey, schema = 'common' } = options;
  const baseUrl = url.replace(/\/+$/, '');

  const defaultHeaders = {
    apikey: serviceKey,
    Authorization: `Bearer ${serviceKey}`,
    'Content-Type': 'application/json'
  };

  const rawClient: SupabaseRestClient = {
    url: baseUrl,
    serviceKey,
    async query<T = any>(path: string, opts: any = {}): Promise<T> {
      const targetSchema = opts.schema || 'gistar';
      const cleanPath = path.replace(/^\/+/, '');
      const res = await fetch(`${baseUrl}/rest/v1/${cleanPath}`, {
        ...opts,
        headers: {
          ...defaultHeaders,
          'Accept-Profile': targetSchema,
          'Content-Profile': targetSchema,
          ...opts.headers
        }
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`DB Query Error: ${res.status} - ${txt}`);
      }
      return res.json() as Promise<T>;
    },
    async rpc<T = any>(
      fn: string,
      args?: Record<string, unknown>,
      opts: any = {}
    ): Promise<{ data: T | null; error: Error | null }> {
      const targetSchema = opts.schema || schema;
      try {
        const res = await fetch(`${baseUrl}/rest/v1/rpc/${fn}`, {
          method: 'POST',
          headers: {
            ...defaultHeaders,
            'Accept-Profile': targetSchema,
            'Content-Profile': targetSchema
          },
          body: JSON.stringify(args ?? {})
        });
        if (!res.ok) {
          const txt = await res.text();
          return { data: null, error: new Error(`RPC Error (${res.status}): ${txt}`) };
        }
        const data = (await res.json()) as T;
        return { data, error: null };
      } catch (err: any) {
        return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
      }
    }
  };

  return {
    raw: rawClient,

    async rpc<T = unknown>(
      functionName: string,
      args?: Record<string, unknown>
    ): Promise<{ data: T | null; error: Error | null }> {
      return rawClient.rpc<T>(functionName, args, { schema });
    },

    async enqueueJob(opts): Promise<{ id: string }> {
      // 1. Try RPC fn_enqueue_job
      const { data, error } = await this.rpc<string>('fn_enqueue_job', {
        p_tenant_id: opts.tenantId,
        p_target_path: opts.targetPath,
        p_payload: opts.payload ?? {},
        p_run_at: opts.runAt ? new Date(opts.runAt).toISOString() : new Date().toISOString(),
        p_max_attempts: opts.maxAttempts ?? 3
      });

      if (!error && data) {
        return { id: typeof data === 'object' && (data as any)?.id ? (data as any).id : String(data) };
      }

      // 2. Direct table insert fallback
      const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : new Date().toISOString();
      const res = await fetch(`${baseUrl}/rest/v1/queued_jobs`, {
        method: 'POST',
        headers: {
          ...defaultHeaders,
          'Accept-Profile': schema,
          'Content-Profile': schema,
          Prefer: 'return=representation'
        },
        body: JSON.stringify({
          tenant_id: opts.tenantId,
          target_path: opts.targetPath,
          payload: opts.payload ?? {},
          run_at: runAt,
          max_attempts: opts.maxAttempts ?? 3,
          status: 'pending'
        })
      });

      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to enqueue job: ${res.status} - ${txt}`);
      }

      const rows = await res.json();
      return { id: rows[0].id };
    },

    async removeJob(jobId: string): Promise<void> {
      const res = await fetch(`${baseUrl}/rest/v1/queued_jobs?id=eq.${jobId}`, {
        method: 'DELETE',
        headers: {
          ...defaultHeaders,
          'Accept-Profile': schema,
          'Content-Profile': schema
        }
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to remove job ${jobId}: ${res.status} - ${txt}`);
      }
    },

    async rescheduleJob(opts): Promise<void> {
      const res = await fetch(`${baseUrl}/rest/v1/queued_jobs?id=eq.${opts.jobId}`, {
        method: 'PATCH',
        headers: {
          ...defaultHeaders,
          'Accept-Profile': schema,
          'Content-Profile': schema
        },
        body: JSON.stringify({
          status: 'pending',
          attempts: opts.attempts,
          run_at: new Date(opts.runAt).toISOString(),
          updated_at: new Date().toISOString()
        })
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to reschedule job ${opts.jobId}: ${res.status} - ${txt}`);
      }
    },

    async recordJobLog(log): Promise<void> {
      const res = await fetch(`${baseUrl}/rest/v1/queued_job_logs`, {
        method: 'POST',
        headers: {
          ...defaultHeaders,
          'Accept-Profile': schema,
          'Content-Profile': schema
        },
        body: JSON.stringify({
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
        })
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to record queue log: ${res.status} - ${txt}`);
      }
    },

    async getJob(jobId: string): Promise<JobRecord | null> {
      const res = await fetch(`${baseUrl}/rest/v1/queued_jobs?id=eq.${jobId}&limit=1`, {
        headers: {
          ...defaultHeaders,
          'Accept-Profile': schema
        }
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to get job ${jobId}: ${res.status} - ${txt}`);
      }
      const data = await res.json();
      return (data && data.length > 0 ? data[0] : null) as JobRecord | null;
    },

    async cancelJob(jobId: string, tenantId?: string): Promise<boolean> {
      let urlStr = `${baseUrl}/rest/v1/queued_jobs?id=eq.${jobId}&status=eq.pending`;
      if (tenantId) urlStr += `&tenant_id=eq.${tenantId}`;

      const res = await fetch(urlStr, {
        method: 'DELETE',
        headers: {
          ...defaultHeaders,
          'Accept-Profile': schema,
          Prefer: 'return=representation'
        }
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to cancel job: ${res.status} - ${txt}`);
      }
      const data = await res.json();
      return Array.isArray(data) && data.length > 0;
    },

    async recordScheduledLog(log): Promise<{ id: string }> {
      if (log.id) {
        const logRes = await fetch(`${baseUrl}/rest/v1/scheduled_job_logs?id=eq.${log.id}`, {
          method: 'PATCH',
          headers: {
            ...defaultHeaders,
            'Accept-Profile': schema,
            'Content-Profile': schema
          },
          body: JSON.stringify({
            status: log.status,
            finished_at: log.finishedAt ?? new Date().toISOString(),
            duration_ms: log.durationMs ?? null,
            response_payload: log.responsePayload ?? null,
            error_message: log.errorMessage ?? null
          })
        });

        if (!logRes.ok) {
          const txt = await logRes.text();
          throw new Error(`Failed to update scheduled log: ${logRes.status} - ${txt}`);
        }

        // Update scheduled_jobs table
        await fetch(`${baseUrl}/rest/v1/scheduled_jobs?id=eq.${log.scheduledJobId}`, {
          method: 'PATCH',
          headers: {
            ...defaultHeaders,
            'Accept-Profile': schema,
            'Content-Profile': schema
          },
          body: JSON.stringify({
            status: log.status,
            last_run_at: log.finishedAt ?? new Date().toISOString()
          })
        }).catch(() => {});

        return { id: log.id };
      }

      const res = await fetch(`${baseUrl}/rest/v1/scheduled_job_logs`, {
        method: 'POST',
        headers: {
          ...defaultHeaders,
          'Accept-Profile': schema,
          'Content-Profile': schema,
          Prefer: 'return=representation'
        },
        body: JSON.stringify({
          job_id: log.scheduledJobId,
          tenant_id: log.tenantId,
          status: log.status,
          triggered_by: log.triggeredBy,
          started_at: log.startedAt,
          finished_at: log.finishedAt ?? null,
          duration_ms: log.durationMs ?? null,
          response_payload: log.responsePayload ?? null,
          error_message: log.errorMessage ?? null
        })
      });

      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to record scheduled log: ${res.status} - ${txt}`);
      }

      const data = await res.json();
      return { id: data[0].id };
    }
  };
}
