import type { DatabaseAdapter, EnqueueJobOptions, JobStatusResponse } from '../types.js';

export class TaskQueue {
  constructor(private db: DatabaseAdapter) {}

  /**
   * Enqueues a job into common.queued_jobs.
   */
  async enqueue(options: EnqueueJobOptions): Promise<{ id: string; status: 'pending' }> {
    if (!options.tenantId) {
      throw new Error('TaskQueue: tenantId is required for multi-tenant isolation.');
    }
    if (!options.targetPath) {
      throw new Error('TaskQueue: targetPath is required.');
    }

    if (this.db.enqueueJob) {
      const res = await this.db.enqueueJob({
        tenantId: options.tenantId,
        targetPath: options.targetPath,
        payload: options.payload ?? {},
        runAt: options.runAt,
        maxAttempts: options.maxAttempts ?? 3
      });
      return { id: res.id, status: 'pending' };
    }

    // Direct RPC fallback
    const { data, error } = await this.db.rpc<string>('fn_enqueue_job', {
      p_tenant_id: options.tenantId,
      p_target_path: options.targetPath,
      p_payload: options.payload ?? {},
      p_run_at: options.runAt ? new Date(options.runAt).toISOString() : new Date().toISOString(),
      p_max_attempts: options.maxAttempts ?? 3
    });

    if (error || !data) {
      throw new Error(`TaskQueue: Failed to enqueue job: ${error?.message || 'Unknown error'}`);
    }

    return { id: data, status: 'pending' };
  }

  /**
   * Retrieves the current execution status of a job (active or logged).
   */
  async getStatus(jobId: string): Promise<JobStatusResponse> {
    if (!jobId) {
      throw new Error('TaskQueue: jobId is required.');
    }

    // 1. Check if job is still in active queue
    if (this.db.getJob) {
      const activeJob = await this.db.getJob(jobId);
      if (activeJob) {
        return {
          id: activeJob.id,
          status: activeJob.status,
          attempts: activeJob.attempts,
          maxAttempts: activeJob.max_attempts,
          targetPath: activeJob.target_path,
          runAt: activeJob.run_at
        };
      }
    }

    // 2. Query historical log from queued_job_logs via query or RPC
    if (this.db.query) {
      const logs = await this.db.query<any[]>(
        `SELECT * FROM common.queued_job_logs WHERE job_id = $1::uuid ORDER BY created_at DESC LIMIT 1`,
        [jobId]
      );
      if (logs && logs.length > 0) {
        const log = logs[0];
        return {
          id: log.job_id,
          status: log.status === 'success' ? 'completed' : 'failed',
          attempts: log.attempts,
          targetPath: log.target_path,
          durationMs: log.duration_ms,
          responsePayload: log.response_payload,
          errorMessage: log.error_message,
          finishedAt: log.finished_at
        };
      }
    }

    return {
      id: jobId,
      status: 'not_found',
      attempts: 0
    };
  }

  /**
   * Cancels a pending job from the active queue.
   */
  async cancel(jobId: string, tenantId?: string): Promise<{ success: boolean; message: string }> {
    if (!jobId) {
      throw new Error('TaskQueue: jobId is required.');
    }

    if (this.db.cancelJob) {
      const cancelled = await this.db.cancelJob(jobId, tenantId);
      return {
        success: cancelled,
        message: cancelled ? 'Job cancelled successfully' : 'Job not found or already running/finished'
      };
    }

    return {
      success: false,
      message: 'Database adapter does not support direct cancellation'
    };
  }
}
