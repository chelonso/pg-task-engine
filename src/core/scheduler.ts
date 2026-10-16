import type { DatabaseAdapter, TaskDefinition, TaskLogger } from '../types.js';

export interface ExecuteScheduledTaskOptions {
  jobId: string;
  tenantId: string;
  targetPath: string;
  payload?: unknown;
  triggeredBy?: 'cron' | 'manual' | 'api';
}

export interface ScheduledTaskResult {
  logId: string;
  status: 'success' | 'failed';
  durationMs: number;
  result?: unknown;
  error?: string;
}

export class TaskScheduler {
  constructor(
    private db: DatabaseAdapter,
    private taskRegistry: Map<string, TaskDefinition>,
    private logger: TaskLogger,
    private defaultTimeoutMs: number = 60000
  ) {}

  /**
   * Executes a scheduled task directly (triggered by pg_cron or manual HTTP invocation).
   */
  async execute(options: ExecuteScheduledTaskOptions): Promise<ScheduledTaskResult> {
    const { jobId, tenantId, targetPath, payload = {}, triggeredBy = 'cron' } = options;
    const startedAt = new Date().toISOString();
    const startTime = Date.now();

    this.logger.info(`Scheduler: Starting scheduled task [${targetPath}] for tenant [${tenantId}]`, {
      jobId,
      triggeredBy
    });

    // 1. Initial audit record (running)
    let logId = '';
    if (this.db.recordScheduledLog) {
      try {
        const res = await this.db.recordScheduledLog({
          scheduledJobId: jobId,
          tenantId,
          targetPath,
          status: 'running',
          triggeredBy,
          startedAt
        });
        logId = res.id;
      } catch (err) {
        this.logger.warn('Scheduler: Failed to record initial running log', { error: String(err) });
      }
    }

    const abortController = new AbortController();
    const timerId = setTimeout(() => {
      abortController.abort(new Error(`Scheduled task [${targetPath}] timed out after ${this.defaultTimeoutMs}ms`));
    }, this.defaultTimeoutMs);

    try {
      let result: unknown;

      // 2. Direct RPC execution if prefixed with 'rpc:'
      if (targetPath.startsWith('rpc:')) {
        const rpcName = targetPath.replace('rpc:', '');
        const { data, error } = await this.db.rpc(rpcName, {
          p_tenant_id: tenantId,
          p_payload: payload
        });
        if (error) throw new Error(`RPC Error in ${rpcName}: ${error.message}`);
        result = data;
      } else {
        // 3. Registered JavaScript/TypeScript task
        const taskDef = this.taskRegistry.get(targetPath);
        if (!taskDef) {
          throw new Error(`Scheduler: Task handler not registered: ${targetPath}`);
        }

        const taskContext = {
          tenantId,
          jobId,
          attempt: 1,
          maxAttempts: 1,
          db: this.db.raw,
          signal: abortController.signal,
          logger: this.logger
        };

        result = await taskDef.run(payload, taskContext);
      }

      const durationMs = Date.now() - startTime;
      const finishedAt = new Date().toISOString();

      // 4. Update audit log (success)
      if (this.db.recordScheduledLog) {
        await this.db.recordScheduledLog({
          id: logId || undefined,
          scheduledJobId: jobId,
          tenantId,
          targetPath,
          status: 'success',
          triggeredBy,
          startedAt,
          finishedAt,
          durationMs,
          responsePayload: result
        });
      }

      this.logger.info(`Scheduler: Task [${targetPath}] succeeded in ${durationMs}ms`, { jobId });

      return {
        logId,
        status: 'success',
        durationMs,
        result
      };
    } catch (err: any) {
      const durationMs = Date.now() - startTime;
      const finishedAt = new Date().toISOString();
      const errorMsg = err?.stack || err?.message || String(err);

      this.logger.error(`Scheduler: Task [${targetPath}] failed: ${errorMsg}`, err, { jobId });

      if (this.db.recordScheduledLog) {
        try {
          await this.db.recordScheduledLog({
            id: logId || undefined,
            scheduledJobId: jobId,
            tenantId,
            targetPath,
            status: 'failed',
            triggeredBy,
            startedAt,
            finishedAt,
            durationMs,
            errorMessage: errorMsg
          });
        } catch (logErr) {
          this.logger.error('Scheduler: Failed to write failure log', logErr);
        }
      }

      return {
        logId,
        status: 'failed',
        durationMs,
        error: errorMsg
      };
    } finally {
      clearTimeout(timerId);
    }
  }
}
