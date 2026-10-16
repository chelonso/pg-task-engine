import { z } from 'zod';

export interface TaskLogger {
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, error?: unknown, meta?: Record<string, unknown>): void;
  debug?(message: string, meta?: Record<string, unknown>): void;
}

/**
 * Agnostic Database Adapter interface.
 * Implemented by SupabaseAdapter, PrismaAdapter, DrizzleAdapter, and PgAdapter.
 */
export interface DatabaseAdapter<TRawClient = unknown> {
  readonly raw: TRawClient;

  /**
   * Invokes a PostgreSQL stored procedure / function.
   * Required for fn_poll_next_job, fn_enqueue_job, fn_publish_domain_event, and rpc: tasks.
   */
  rpc<T = unknown>(
    functionName: string,
    args?: Record<string, unknown>
  ): Promise<{ data: T | null; error: Error | null }>;

  /**
   * Optional direct query helper for table operations.
   */
  query?<T = unknown>(sql: string, params?: unknown[]): Promise<T>;

  /**
   * Enqueues a job into common.queued_jobs.
   */
  enqueueJob?(options: {
    tenantId: string;
    targetPath: string;
    payload: unknown;
    runAt?: Date | string;
    maxAttempts?: number;
  }): Promise<{ id: string }>;

  /**
   * Deletes a processed or dead job from common.queued_jobs.
   */
  removeJob?(jobId: string): Promise<void>;

  /**
   * Reschedules a failed job with updated attempts and backoff run_at.
   */
  rescheduleJob?(options: {
    jobId: string;
    attempts: number;
    runAt: Date | string;
  }): Promise<void>;

  /**
   * Records execution audit trail in common.queued_job_logs.
   */
  recordJobLog?(log: {
    jobId: string;
    tenantId: string;
    targetPath: string;
    status: 'success' | 'failed';
    startedAt: string;
    finishedAt: string;
    durationMs: number;
    responsePayload?: unknown;
    errorMessage?: string | null;
    attempts: number;
  }): Promise<void>;

  /**
   * Retrieves active job or status.
   */
  getJob?(jobId: string): Promise<JobRecord | null>;

  /**
   * Cancels a pending job.
   */
  cancelJob?(jobId: string, tenantId?: string): Promise<boolean>;

  /**
   * Records execution audit for scheduled cron jobs.
   */
  recordScheduledLog?(log: {
    id?: string;
    scheduledJobId: string;
    tenantId: string;
    targetPath: string;
    status: 'running' | 'success' | 'failed';
    triggeredBy: 'cron' | 'manual' | 'api';
    startedAt: string;
    finishedAt?: string;
    durationMs?: number;
    responsePayload?: unknown;
    errorMessage?: string | null;
  }): Promise<{ id: string }>;
}

export interface TaskContext<TDb = unknown> {
  /** Strict tenant UUID for row-level isolation */
  tenantId: string;
  /** UUID of current running job */
  jobId: string;
  /** Attempt index (1, 2, 3...) */
  attempt: number;
  /** Configured maximum allowed attempts */
  maxAttempts: number;
  /** Injected Database client or ORM instance (Prisma, Drizzle, Supabase, etc.) */
  db: TDb;
  /** Cancellation token with automatic timeout */
  signal: AbortSignal;
  /** Scoped logger prefixing [jobId] and [tenantId] */
  logger: TaskLogger;
}

export type TaskHandler<TPayload = unknown, TResult = unknown, TDb = unknown> = (
  payload: TPayload,
  ctx: TaskContext<TDb>
) => Promise<TResult>;

export interface TaskDefinition<TPayload = unknown, TResult = unknown, TDb = unknown> {
  name: string;
  schema?: z.ZodType<TPayload>;
  timeoutMs?: number;
  handler: TaskHandler<TPayload, TResult, TDb>;
  run(payload: unknown, ctx: TaskContext<TDb>): Promise<TResult>;
}

export interface DefineTaskOptions<TPayload = unknown, TResult = unknown, TDb = unknown> {
  name: string;
  schema?: z.ZodType<TPayload>;
  timeoutMs?: number;
  handler: TaskHandler<TPayload, TResult, TDb>;
}

export interface JobRecord {
  id: string;
  tenant_id: string;
  target_path: string;
  payload: unknown;
  status: 'pending' | 'processing';
  run_at: string;
  attempts: number;
  max_attempts: number;
  created_at: string;
  updated_at: string;
}

export interface PolledJob {
  id: string;
  tenant_id: string;
  target_path: string;
  payload: unknown;
  attempts: number;
  max_attempts?: number;
}

export interface EnqueueJobOptions {
  tenantId: string;
  targetPath: string;
  payload?: unknown;
  runAt?: Date | string;
  maxAttempts?: number;
}

export interface JobStatusResponse {
  id: string;
  status: 'pending' | 'processing' | 'completed' | 'failed' | 'not_found';
  attempts: number;
  maxAttempts?: number;
  targetPath?: string;
  runAt?: string;
  durationMs?: number;
  responsePayload?: unknown;
  errorMessage?: string | null;
  finishedAt?: string;
}

export interface DomainEventOptions {
  tenantId: string;
  eventType: string;
  payload?: unknown;
  source?: string;
}

export interface TaskEngineConfig<TDb = unknown> {
  /** Database Adapter or native client instance */
  db: DatabaseAdapter | unknown;
  /** Idle polling interval in milliseconds when queue is empty (default: 5000ms) */
  pollIntervalMs?: number;
  /** Number of concurrent jobs to process in parallel (default: 1) */
  concurrency?: number;
  /** Default execution timeout per task in milliseconds (default: 30000ms) */
  defaultTimeoutMs?: number;
  /** Custom base logger implementation */
  logger?: TaskLogger;
  /** Initial task definitions to register */
  tasks?: TaskDefinition[];
  /** PostgreSQL schema where queue tables reside (default: 'common') */
  schema?: string;
}
