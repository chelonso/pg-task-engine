import type {
  DatabaseAdapter,
  PolledJob,
  TaskContext,
  TaskDefinition,
  TaskLogger
} from '../types.js';

export interface TaskWorkerOptions {
  db: DatabaseAdapter;
  taskRegistry: Map<string, TaskDefinition>;
  logger: TaskLogger;
  pollIntervalMs?: number;
  concurrency?: number;
  defaultTimeoutMs?: number;
}

export class TaskWorker {
  private db: DatabaseAdapter;
  private taskRegistry: Map<string, TaskDefinition>;
  private logger: TaskLogger;
  private pollIntervalMs: number;
  private concurrency: number;
  private defaultTimeoutMs: number;

  private isRunning: boolean = false;
  private isDraining: boolean = false;
  private timerId: NodeJS.Timeout | null = null;
  private activeJobsCount: number = 0;
  private drainResolvers: Array<() => void> = [];

  constructor(options: TaskWorkerOptions) {
    this.db = options.db;
    this.taskRegistry = options.taskRegistry;
    this.logger = options.logger;
    this.pollIntervalMs = options.pollIntervalMs ?? 5000;
    this.concurrency = Math.max(1, options.concurrency ?? 1);
    this.defaultTimeoutMs = options.defaultTimeoutMs ?? 30000;
  }

  /**
   * Starts the polling loop and registers process shutdown signals.
   */
  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.isDraining = false;

    this.logger.info('TaskWorker: Started worker loop', {
      pollIntervalMs: this.pollIntervalMs,
      concurrency: this.concurrency
    });

    this.registerSignalHandlers();
    this.scheduleNextPoll(0);
  }

  /**
   * Gracefully stops the worker, waiting for all active jobs to finish.
   */
  async stop(drainTimeoutMs: number = 15000): Promise<void> {
    if (!this.isRunning) return;

    this.logger.info('TaskWorker: Graceful shutdown initiated. Waiting for active jobs to finish...');
    this.isRunning = false;
    this.isDraining = true;

    if (this.timerId) {
      clearTimeout(this.timerId);
      this.timerId = null;
    }

    if (this.activeJobsCount === 0) {
      this.logger.info('TaskWorker: No active jobs. Worker stopped cleanly.');
      return;
    }

    // Wait for in-flight jobs to complete or timeout
    await Promise.race([
      new Promise<void>((resolve) => {
        this.drainResolvers.push(resolve);
      }),
      new Promise<void>((_, reject) => {
        setTimeout(() => {
          reject(new Error(`TaskWorker: Drain timeout reached (${drainTimeoutMs}ms). Forcing stop.`));
        }, drainTimeoutMs);
      })
    ]).catch((err) => {
      this.logger.warn(err.message);
    });

    this.logger.info('TaskWorker: Worker successfully stopped.');
  }

  private registerSignalHandlers(): void {
    const onSignal = async (signal: string) => {
      this.logger.info(`TaskWorker: Received ${signal}, starting graceful shutdown...`);
      await this.stop().catch((err) => this.logger.error('Shutdown error', err));
    };

    process.once('SIGTERM', () => onSignal('SIGTERM'));
    process.once('SIGINT', () => onSignal('SIGINT'));
  }

  private scheduleNextPoll(delayMs: number): void {
    if (!this.isRunning || this.isDraining) return;
    if (this.timerId) clearTimeout(this.timerId);

    this.timerId = setTimeout(() => {
      this.pollAndDispatch().catch((err) => {
        this.logger.error('TaskWorker: Unhandled polling error', err);
        this.scheduleNextPoll(this.pollIntervalMs);
      });
    }, delayMs);
  }

  private async pollAndDispatch(): Promise<void> {
    if (!this.isRunning || this.isDraining) return;

    // Check concurrency capacity
    const availableSlots = this.concurrency - this.activeJobsCount;
    if (availableSlots <= 0) {
      // Saturated: wait and check again shortly
      this.scheduleNextPoll(200);
      return;
    }

    try {
      // 1. Atomically poll next job via fn_poll_next_job (FOR UPDATE SKIP LOCKED)
      const { data, error } = await this.db.rpc<PolledJob[]>('fn_poll_next_job');

      if (error) {
        this.logger.error(`TaskWorker: Poller RPC failed: ${error.message}`, error);
        this.scheduleNextPoll(this.pollIntervalMs);
        return;
      }

      const jobs = Array.isArray(data) ? data : data ? [data] : [];
      const job = jobs[0];

      // No pending jobs available in the queue
      if (!job || !job.id) {
        this.scheduleNextPoll(this.pollIntervalMs);
        return;
      }

      // Found a job: process it concurrently
      this.activeJobsCount++;
      this.processJob(job).finally(() => {
        this.activeJobsCount--;
        if (this.isDraining && this.activeJobsCount === 0) {
          this.drainResolvers.forEach((fn) => fn());
          this.drainResolvers = [];
        }
      });

      // Immediate check for more jobs if concurrency permits
      this.scheduleNextPoll(0);
    } catch (err: any) {
      this.logger.error('TaskWorker: Critical error in poller loop', err);
      this.scheduleNextPoll(this.pollIntervalMs);
    }
  }

  private async processJob(job: PolledJob): Promise<void> {
    const startedAt = new Date().toISOString();
    const startTime = Date.now();
    const scopedLogger = this.createScopedLogger(job.id, job.tenant_id);

    scopedLogger.info(`Worker: Processing task [${job.target_path}] (Attempt ${job.attempts + 1})`);

    const abortController = new AbortController();
    const timeoutMs = this.defaultTimeoutMs;
    const timer = setTimeout(() => {
      abortController.abort(new Error(`Task [${job.target_path}] exceeded timeout of ${timeoutMs}ms`));
    }, timeoutMs);

    const taskContext: TaskContext = {
      tenantId: job.tenant_id,
      jobId: job.id,
      attempt: job.attempts + 1,
      maxAttempts: job.max_attempts ?? 3,
      db: this.db.raw,
      signal: abortController.signal,
      logger: scopedLogger
    };

    try {
      let result: unknown;

      // 1. Hybrid Check: Direct SQL Stored Procedure (rpc:)
      if (job.target_path.startsWith('rpc:')) {
        const rpcName = job.target_path.replace('rpc:', '');
        const { data, error } = await this.db.rpc(rpcName, {
          p_tenant_id: job.tenant_id,
          p_payload: job.payload ?? {}
        });
        if (error) throw new Error(`RPC Error in ${rpcName}: ${error.message}`);
        result = data;
      } else {
        // 2. TypeScript / JavaScript Registered Task
        const taskDef = this.taskRegistry.get(job.target_path);
        if (!taskDef) {
          const errorMsg = `Worker: Handler not registered for task: ${job.target_path}`;
          scopedLogger.error(errorMsg);

          // Unregistered task cannot be retried -> record failure and remove
          await this.removeJob(job.id);
          await this.recordLog(job, 'failed', startedAt, startTime, null, errorMsg);
          return;
        }

        result = await taskDef.run(job.payload, taskContext);
      }

      // 3. Success: remove from queue and audit log
      await this.removeJob(job.id);
      await this.recordLog(job, 'success', startedAt, startTime, result, null);

      scopedLogger.info(`Worker: Task [${job.target_path}] completed successfully in ${Date.now() - startTime}ms`);
    } catch (err: any) {
      const errorMsg = err?.stack || err?.message || String(err);
      scopedLogger.error(`Worker: Error executing task [${job.target_path}]: ${errorMsg}`, err);

      const nextAttempt = job.attempts + 1;
      const maxAttempts = job.max_attempts ?? 3;

      // Log current failed attempt
      await this.recordLog(job, 'failed', startedAt, startTime, null, errorMsg);

      if (nextAttempt < maxAttempts) {
        // Exponential backoff: 2^attempt * 5 seconds (5s, 10s, 20s...)
        const delaySeconds = Math.pow(2, nextAttempt) * 5;
        const nextRunAt = new Date(Date.now() + delaySeconds * 1000);

        scopedLogger.warn(
          `Worker: Rescheduling task for retry in ${delaySeconds}s (Attempt ${nextAttempt + 1}/${maxAttempts})`,
          { nextRunAt: nextRunAt.toISOString() }
        );

        await this.rescheduleJob(job.id, nextAttempt, nextRunAt);
      } else {
        scopedLogger.error(`Worker: Task exhausted all ${maxAttempts} attempts. Removed from active queue.`);
        await this.removeJob(job.id);
      }
    } finally {
      clearTimeout(timer);
    }
  }

  private async removeJob(jobId: string): Promise<void> {
    try {
      if (this.db.removeJob) {
        await this.db.removeJob(jobId);
      } else {
        await this.db.query?.(`DELETE FROM common.queued_jobs WHERE id = $1::uuid`, [jobId]);
      }
    } catch (err) {
      this.logger.error(`Worker: Failed to delete job ${jobId} from queue`, err);
    }
  }

  private async rescheduleJob(jobId: string, attempts: number, runAt: Date): Promise<void> {
    try {
      if (this.db.rescheduleJob) {
        await this.db.rescheduleJob({ jobId, attempts, runAt });
      } else {
        await this.db.query?.(
          `UPDATE common.queued_jobs SET status = 'pending', attempts = $1, run_at = $2::timestamptz, updated_at = NOW() WHERE id = $3::uuid`,
          [attempts, runAt.toISOString(), jobId]
        );
      }
    } catch (err) {
      this.logger.error(`Worker: Failed to reschedule job ${jobId}`, err);
    }
  }

  private async recordLog(
    job: PolledJob,
    status: 'success' | 'failed',
    startedAt: string,
    startTime: number,
    responsePayload?: unknown,
    errorMessage?: string | null
  ): Promise<void> {
    const durationMs = Date.now() - startTime;
    const finishedAt = new Date().toISOString();

    try {
      if (this.db.recordJobLog) {
        await this.db.recordJobLog({
          jobId: job.id,
          tenantId: job.tenant_id,
          targetPath: job.target_path,
          status,
          startedAt,
          finishedAt,
          durationMs,
          responsePayload,
          errorMessage,
          attempts: job.attempts + 1
        });
      }
    } catch (err) {
      this.logger.error('Worker: Failed to write audit log to database', err);
    }
  }

  private createScopedLogger(jobId: string, tenantId: string): TaskLogger {
    const prefix = `[${jobId.substring(0, 8)}][${tenantId.substring(0, 8)}]`;
    return {
      info: (msg, meta) => this.logger.info(`${prefix} ${msg}`, meta),
      warn: (msg, meta) => this.logger.warn(`${prefix} ${msg}`, meta),
      error: (msg, err, meta) => this.logger.error(`${prefix} ${msg}`, err, meta),
      debug: (msg, meta) => this.logger.debug?.(`${prefix} ${msg}`, meta)
    };
  }
}
