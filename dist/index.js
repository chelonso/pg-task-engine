'use strict';

// src/db/adapter.ts
function resolveDatabaseAdapter(db) {
  if (!db || typeof db !== "object") {
    throw new Error("TaskEngine: Invalid database client provided.");
  }
  if ("rpc" in db && typeof db.rpc === "function") {
    return db;
  }
  throw new Error(
    "TaskEngine: Provided db instance does not implement DatabaseAdapter. Use createSupabaseAdapter, createPrismaAdapter, createDrizzleAdapter, or createPgAdapter."
  );
}

// src/core/events.ts
var DomainEventsManager = class {
  constructor(db) {
    this.db = db;
  }
  db;
  /**
   * Publishes a domain event into the common.domain_events outbox table.
   */
  async publish(options) {
    const eventName = options.eventName || options.event_name || options.eventType;
    if (!options.tenantId) {
      throw new Error("DomainEvents: tenantId is required for multi-tenant isolation.");
    }
    if (!eventName) {
      throw new Error("DomainEvents: eventName is required.");
    }
    const { data, error } = await this.db.rpc("fn_publish_domain_event", {
      p_tenant_id: options.tenantId,
      p_event_name: eventName,
      p_payload: options.payload ?? {}
    });
    if (!error && data) {
      return { id: typeof data === "object" && data?.id ? data.id : String(data), status: "pending" };
    }
    if (this.db.raw && typeof this.db.raw.query === "function") {
      try {
        const rows = await this.db.raw.query("domain_events", {
          method: "POST",
          schema: "common",
          headers: { Prefer: "return=representation" },
          body: JSON.stringify({
            tenant_id: options.tenantId,
            event_name: eventName,
            payload: options.payload ?? {}
          })
        });
        const id = Array.isArray(rows) && rows[0]?.id ? rows[0].id : "event-" + Date.now();
        return { id, status: "pending" };
      } catch (insertErr) {
        throw new Error(`DomainEvents: Failed to publish event: ${insertErr?.message || error?.message}`);
      }
    }
    if (error || !data) {
      throw new Error(`DomainEvents: Failed to publish event: ${error?.message || "No ID returned"}`);
    }
    return { id: String(data), status: "pending" };
  }
  /**
   * Processes an outbox domain event, evaluating subscriptions and enqueuing jobs.
   */
  async processEvent(eventId) {
    if (!eventId) {
      throw new Error("DomainEvents: eventId is required.");
    }
    const { data, error } = await this.db.rpc("fn_process_domain_event", {
      p_event_id: eventId
    });
    if (error) {
      throw new Error(`DomainEvents: Failed to process event ${eventId}: ${error.message}`);
    }
    return { matchedSubscriptions: data ?? 0 };
  }
};

// src/core/http-handlers.ts
var TaskHttpController = class {
  constructor(engine) {
    this.engine = engine;
  }
  engine;
  /**
   * POST /add (Enqueue a job)
   */
  async handleEnqueue(request) {
    try {
      const body = await request.json().catch(() => ({}));
      const tenantId = body.tenantId || body.tenant_id;
      const targetPath = body.targetPath || body.target_path;
      const payload = body.payload ?? {};
      const runAt = body.runAt || body.run_at;
      const maxAttempts = body.maxAttempts || body.max_attempts;
      if (!tenantId) {
        return new Response(
          JSON.stringify({ error: "Missing required field: tenantId (required for multi-tenant isolation)" }),
          { status: 400, headers: { "Content-Type": "application/json" } }
        );
      }
      if (!targetPath) {
        return new Response(
          JSON.stringify({ error: "Missing required field: targetPath" }),
          { status: 400, headers: { "Content-Type": "application/json" } }
        );
      }
      const result = await this.engine.queue.enqueue({
        tenantId,
        targetPath,
        payload,
        runAt,
        maxAttempts
      });
      return new Response(
        JSON.stringify({
          success: true,
          jobId: result.id,
          job_id: result.id,
          status: result.status,
          message: "Task successfully enqueued"
        }),
        {
          status: 202,
          headers: { "Content-Type": "application/json" }
        }
      );
    } catch (err) {
      return new Response(
        JSON.stringify({ error: err?.message || "Internal server error while enqueuing task" }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }
  }
  /**
   * GET /status/:id (Get job status)
   */
  async handleGetStatus(jobId) {
    try {
      if (!jobId) {
        return new Response(
          JSON.stringify({ error: "Missing jobId parameter" }),
          { status: 400, headers: { "Content-Type": "application/json" } }
        );
      }
      const status = await this.engine.queue.getStatus(jobId);
      return new Response(JSON.stringify(status), {
        status: status.status === "not_found" ? 404 : 200,
        headers: { "Content-Type": "application/json" }
      });
    } catch (err) {
      return new Response(
        JSON.stringify({ error: err?.message || "Internal server error while retrieving task status" }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }
  }
  /**
   * DELETE /cancel/:id (Cancel a pending job)
   */
  async handleCancel(jobId, tenantId) {
    try {
      if (!jobId) {
        return new Response(
          JSON.stringify({ error: "Missing jobId parameter" }),
          { status: 400, headers: { "Content-Type": "application/json" } }
        );
      }
      const result = await this.engine.queue.cancel(jobId, tenantId);
      return new Response(JSON.stringify(result), {
        status: result.success ? 200 : 404,
        headers: { "Content-Type": "application/json" }
      });
    } catch (err) {
      return new Response(
        JSON.stringify({ error: err?.message || "Internal server error while cancelling task" }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }
  }
  /**
   * POST /cron/execute (Trigger scheduled cron execution)
   */
  async handleCronExecute(request) {
    try {
      const body = await request.json().catch(() => ({}));
      const jobId = body.jobId || body.job_id;
      const tenantId = body.tenantId || body.tenant_id;
      const targetPath = body.targetPath || body.target_path;
      const payload = body.payload ?? {};
      const triggeredBy = body.triggeredBy || body.triggered_by || "api";
      if (!jobId || !tenantId || !targetPath) {
        return new Response(
          JSON.stringify({ error: "Missing required parameters: jobId, tenantId, targetPath" }),
          { status: 400, headers: { "Content-Type": "application/json" } }
        );
      }
      const result = await this.engine.scheduler.execute({
        jobId,
        tenantId,
        targetPath,
        payload,
        triggeredBy
      });
      return new Response(JSON.stringify(result), {
        status: result.status === "success" ? 200 : 500,
        headers: { "Content-Type": "application/json" }
      });
    } catch (err) {
      return new Response(
        JSON.stringify({ error: err?.message || "Internal server error executing scheduled task" }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }
  }
  /**
   * POST /events/publish (Publish a domain event)
   */
  async handlePublishEvent(request) {
    try {
      const body = await request.json().catch(() => ({}));
      const tenantId = body.tenantId || body.tenant_id;
      const eventName = body.eventName || body.event_name || body.eventType || body.event_type;
      const payload = body.payload ?? {};
      const source = body.source || "api";
      if (!tenantId || !eventName) {
        return new Response(
          JSON.stringify({ error: "Missing required parameters: tenantId, event_name" }),
          { status: 400, headers: { "Content-Type": "application/json" } }
        );
      }
      const result = await this.engine.events.publish({
        tenantId,
        eventName,
        payload,
        source
      });
      return new Response(JSON.stringify(result), {
        status: 202,
        headers: { "Content-Type": "application/json" }
      });
    } catch (err) {
      return new Response(
        JSON.stringify({ error: err?.message || "Internal server error publishing event" }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }
  }
};

// src/core/queue.ts
var TaskQueue = class {
  constructor(db) {
    this.db = db;
  }
  db;
  /**
   * Enqueues a job into common.queued_jobs.
   */
  async enqueue(options) {
    if (!options.tenantId) {
      throw new Error("TaskQueue: tenantId is required for multi-tenant isolation.");
    }
    if (!options.targetPath) {
      throw new Error("TaskQueue: targetPath is required.");
    }
    if (this.db.enqueueJob) {
      const res = await this.db.enqueueJob({
        tenantId: options.tenantId,
        targetPath: options.targetPath,
        payload: options.payload ?? {},
        runAt: options.runAt,
        maxAttempts: options.maxAttempts ?? 3
      });
      return { id: res.id, status: "pending" };
    }
    const { data, error } = await this.db.rpc("fn_enqueue_job", {
      p_tenant_id: options.tenantId,
      p_target_path: options.targetPath,
      p_payload: options.payload ?? {},
      p_run_at: options.runAt ? new Date(options.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString(),
      p_max_attempts: options.maxAttempts ?? 3
    });
    if (error || !data) {
      throw new Error(`TaskQueue: Failed to enqueue job: ${error?.message || "Unknown error"}`);
    }
    return { id: data, status: "pending" };
  }
  /**
   * Retrieves the current execution status of a job (active or logged).
   */
  async getStatus(jobId) {
    if (!jobId) {
      throw new Error("TaskQueue: jobId is required.");
    }
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
    if (this.db.query) {
      const logs = await this.db.query(
        `SELECT * FROM common.queued_job_logs WHERE job_id = $1::uuid ORDER BY created_at DESC LIMIT 1`,
        [jobId]
      );
      if (logs && logs.length > 0) {
        const log = logs[0];
        return {
          id: log.job_id,
          status: log.status === "success" ? "completed" : "failed",
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
      status: "not_found",
      attempts: 0
    };
  }
  /**
   * Cancels a pending job from the active queue.
   */
  async cancel(jobId, tenantId) {
    if (!jobId) {
      throw new Error("TaskQueue: jobId is required.");
    }
    if (this.db.cancelJob) {
      const cancelled = await this.db.cancelJob(jobId, tenantId);
      return {
        success: cancelled,
        message: cancelled ? "Job cancelled successfully" : "Job not found or already running/finished"
      };
    }
    return {
      success: false,
      message: "Database adapter does not support direct cancellation"
    };
  }
};

// src/core/scheduler.ts
var TaskScheduler = class {
  constructor(db, taskRegistry, logger, defaultTimeoutMs = 6e4) {
    this.db = db;
    this.taskRegistry = taskRegistry;
    this.logger = logger;
    this.defaultTimeoutMs = defaultTimeoutMs;
  }
  db;
  taskRegistry;
  logger;
  defaultTimeoutMs;
  /**
   * Executes a scheduled task directly (triggered by pg_cron or manual HTTP invocation).
   */
  async execute(options) {
    const { jobId, tenantId, targetPath, payload = {}, triggeredBy = "cron" } = options;
    const startedAt = (/* @__PURE__ */ new Date()).toISOString();
    const startTime = Date.now();
    this.logger.info(`Scheduler: Starting scheduled task [${targetPath}] for tenant [${tenantId}]`, {
      jobId,
      triggeredBy
    });
    let logId = "";
    if (this.db.recordScheduledLog) {
      try {
        const res = await this.db.recordScheduledLog({
          scheduledJobId: jobId,
          tenantId,
          targetPath,
          status: "running",
          triggeredBy,
          startedAt
        });
        logId = res.id;
      } catch (err) {
        this.logger.warn("Scheduler: Failed to record initial running log", { error: String(err) });
      }
    }
    const abortController = new AbortController();
    const timerId = setTimeout(() => {
      abortController.abort(new Error(`Scheduled task [${targetPath}] timed out after ${this.defaultTimeoutMs}ms`));
    }, this.defaultTimeoutMs);
    try {
      let result;
      if (targetPath.startsWith("rpc:")) {
        const rpcName = targetPath.replace("rpc:", "");
        const { data, error } = await this.db.rpc(rpcName, {
          p_tenant_id: tenantId,
          p_payload: payload
        });
        if (error) throw new Error(`RPC Error in ${rpcName}: ${error.message}`);
        result = data;
      } else {
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
      const finishedAt = (/* @__PURE__ */ new Date()).toISOString();
      if (this.db.recordScheduledLog) {
        await this.db.recordScheduledLog({
          id: logId || void 0,
          scheduledJobId: jobId,
          tenantId,
          targetPath,
          status: "success",
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
        status: "success",
        durationMs,
        result
      };
    } catch (err) {
      const durationMs = Date.now() - startTime;
      const finishedAt = (/* @__PURE__ */ new Date()).toISOString();
      const errorMsg = err?.stack || err?.message || String(err);
      this.logger.error(`Scheduler: Task [${targetPath}] failed: ${errorMsg}`, err, { jobId });
      if (this.db.recordScheduledLog) {
        try {
          await this.db.recordScheduledLog({
            id: logId || void 0,
            scheduledJobId: jobId,
            tenantId,
            targetPath,
            status: "failed",
            triggeredBy,
            startedAt,
            finishedAt,
            durationMs,
            errorMessage: errorMsg
          });
        } catch (logErr) {
          this.logger.error("Scheduler: Failed to write failure log", logErr);
        }
      }
      return {
        logId,
        status: "failed",
        durationMs,
        error: errorMsg
      };
    } finally {
      clearTimeout(timerId);
    }
  }
};

// src/core/worker.ts
var TaskWorker = class {
  db;
  taskRegistry;
  logger;
  pollIntervalMs;
  concurrency;
  defaultTimeoutMs;
  isRunning = false;
  isDraining = false;
  timerId = null;
  activeJobsCount = 0;
  drainResolvers = [];
  constructor(options) {
    this.db = options.db;
    this.taskRegistry = options.taskRegistry;
    this.logger = options.logger;
    this.pollIntervalMs = options.pollIntervalMs ?? 5e3;
    this.concurrency = Math.max(1, options.concurrency ?? 1);
    this.defaultTimeoutMs = options.defaultTimeoutMs ?? 3e4;
  }
  /**
   * Starts the polling loop and registers process shutdown signals.
   */
  start() {
    if (this.isRunning) return;
    this.isRunning = true;
    this.isDraining = false;
    this.logger.info("TaskWorker: Started worker loop", {
      pollIntervalMs: this.pollIntervalMs,
      concurrency: this.concurrency
    });
    this.registerSignalHandlers();
    this.scheduleNextPoll(0);
  }
  /**
   * Gracefully stops the worker, waiting for all active jobs to finish.
   */
  async stop(drainTimeoutMs = 15e3) {
    if (!this.isRunning) return;
    this.logger.info("TaskWorker: Graceful shutdown initiated. Waiting for active jobs to finish...");
    this.isRunning = false;
    this.isDraining = true;
    if (this.timerId) {
      clearTimeout(this.timerId);
      this.timerId = null;
    }
    if (this.activeJobsCount === 0) {
      this.logger.info("TaskWorker: No active jobs. Worker stopped cleanly.");
      return;
    }
    await Promise.race([
      new Promise((resolve) => {
        this.drainResolvers.push(resolve);
      }),
      new Promise((_, reject) => {
        setTimeout(() => {
          reject(new Error(`TaskWorker: Drain timeout reached (${drainTimeoutMs}ms). Forcing stop.`));
        }, drainTimeoutMs);
      })
    ]).catch((err) => {
      this.logger.warn(err.message);
    });
    this.logger.info("TaskWorker: Worker successfully stopped.");
  }
  registerSignalHandlers() {
    const onSignal = async (signal) => {
      this.logger.info(`TaskWorker: Received ${signal}, starting graceful shutdown...`);
      await this.stop().catch((err) => this.logger.error("Shutdown error", err));
    };
    process.once("SIGTERM", () => onSignal("SIGTERM"));
    process.once("SIGINT", () => onSignal("SIGINT"));
  }
  scheduleNextPoll(delayMs) {
    if (!this.isRunning || this.isDraining) return;
    if (this.timerId) clearTimeout(this.timerId);
    this.timerId = setTimeout(() => {
      this.pollAndDispatch().catch((err) => {
        this.logger.error("TaskWorker: Unhandled polling error", err);
        this.scheduleNextPoll(this.pollIntervalMs);
      });
    }, delayMs);
  }
  async pollAndDispatch() {
    if (!this.isRunning || this.isDraining) return;
    const availableSlots = this.concurrency - this.activeJobsCount;
    if (availableSlots <= 0) {
      this.scheduleNextPoll(200);
      return;
    }
    try {
      const { data, error } = await this.db.rpc("fn_poll_next_job");
      if (error) {
        this.logger.error(`TaskWorker: Poller RPC failed: ${error.message}`, error);
        this.scheduleNextPoll(this.pollIntervalMs);
        return;
      }
      const jobs = Array.isArray(data) ? data : data ? [data] : [];
      const job = jobs[0];
      if (!job || !job.id) {
        this.scheduleNextPoll(this.pollIntervalMs);
        return;
      }
      this.activeJobsCount++;
      this.processJob(job).finally(() => {
        this.activeJobsCount--;
        if (this.isDraining && this.activeJobsCount === 0) {
          this.drainResolvers.forEach((fn) => fn());
          this.drainResolvers = [];
        }
      });
      this.scheduleNextPoll(0);
    } catch (err) {
      this.logger.error("TaskWorker: Critical error in poller loop", err);
      this.scheduleNextPoll(this.pollIntervalMs);
    }
  }
  async processJob(job) {
    const startedAt = (/* @__PURE__ */ new Date()).toISOString();
    const startTime = Date.now();
    const scopedLogger = this.createScopedLogger(job.id, job.tenant_id);
    scopedLogger.info(`Worker: Processing task [${job.target_path}] (Attempt ${job.attempts + 1})`);
    const abortController = new AbortController();
    const timeoutMs = this.defaultTimeoutMs;
    const timer = setTimeout(() => {
      abortController.abort(new Error(`Task [${job.target_path}] exceeded timeout of ${timeoutMs}ms`));
    }, timeoutMs);
    const taskContext = {
      tenantId: job.tenant_id,
      jobId: job.id,
      attempt: job.attempts + 1,
      maxAttempts: job.max_attempts ?? 3,
      db: this.db.raw,
      signal: abortController.signal,
      logger: scopedLogger
    };
    try {
      let result;
      if (job.target_path.startsWith("rpc:")) {
        const rpcName = job.target_path.replace("rpc:", "");
        const { data, error } = await this.db.rpc(rpcName, {
          p_tenant_id: job.tenant_id,
          p_payload: job.payload ?? {}
        });
        if (error) throw new Error(`RPC Error in ${rpcName}: ${error.message}`);
        result = data;
      } else {
        const taskDef = this.taskRegistry.get(job.target_path);
        if (!taskDef) {
          const errorMsg = `Worker: Handler not registered for task: ${job.target_path}`;
          scopedLogger.error(errorMsg);
          await this.removeJob(job.id);
          await this.recordLog(job, "failed", startedAt, startTime, null, errorMsg);
          return;
        }
        result = await taskDef.run(job.payload, taskContext);
      }
      await this.removeJob(job.id);
      await this.recordLog(job, "success", startedAt, startTime, result, null);
      scopedLogger.info(`Worker: Task [${job.target_path}] completed successfully in ${Date.now() - startTime}ms`);
    } catch (err) {
      const errorMsg = err?.stack || err?.message || String(err);
      scopedLogger.error(`Worker: Error executing task [${job.target_path}]: ${errorMsg}`, err);
      const nextAttempt = job.attempts + 1;
      const maxAttempts = job.max_attempts ?? 3;
      await this.recordLog(job, "failed", startedAt, startTime, null, errorMsg);
      if (nextAttempt < maxAttempts) {
        const delaySeconds = Math.pow(2, nextAttempt) * 5;
        const nextRunAt = new Date(Date.now() + delaySeconds * 1e3);
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
  async removeJob(jobId) {
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
  async rescheduleJob(jobId, attempts, runAt) {
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
  async recordLog(job, status, startedAt, startTime, responsePayload, errorMessage) {
    const durationMs = Date.now() - startTime;
    const finishedAt = (/* @__PURE__ */ new Date()).toISOString();
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
      this.logger.error("Worker: Failed to write audit log to database", err);
    }
  }
  createScopedLogger(jobId, tenantId) {
    const prefix = `[${jobId.substring(0, 8)}][${tenantId.substring(0, 8)}]`;
    return {
      info: (msg, meta) => this.logger.info(`${prefix} ${msg}`, meta),
      warn: (msg, meta) => this.logger.warn(`${prefix} ${msg}`, meta),
      error: (msg, err, meta) => this.logger.error(`${prefix} ${msg}`, err, meta),
      debug: (msg, meta) => this.logger.debug?.(`${prefix} ${msg}`, meta)
    };
  }
};

// src/core/engine.ts
var TaskEngine = class {
  db;
  queue;
  scheduler;
  events;
  worker;
  http;
  logger;
  taskRegistry = /* @__PURE__ */ new Map();
  constructor(config) {
    this.db = resolveDatabaseAdapter(config.db);
    this.logger = config.logger ?? {
      info: (msg, meta) => console.log(`[INFO] ${msg}`, meta ?? ""),
      warn: (msg, meta) => console.warn(`[WARN] ${msg}`, meta ?? ""),
      error: (msg, err, meta) => console.error(`[ERROR] ${msg}`, err ?? "", meta ?? ""),
      debug: (msg, meta) => console.debug?.(`[DEBUG] ${msg}`, meta ?? "")
    };
    if (config.tasks) {
      config.tasks.forEach((t) => this.taskRegistry.set(t.name, t));
    }
    this.queue = new TaskQueue(this.db);
    this.events = new DomainEventsManager(this.db);
    this.scheduler = new TaskScheduler(
      this.db,
      this.taskRegistry,
      this.logger,
      config.defaultTimeoutMs ?? 6e4
    );
    this.worker = new TaskWorker({
      db: this.db,
      taskRegistry: this.taskRegistry,
      logger: this.logger,
      pollIntervalMs: config.pollIntervalMs ?? 5e3,
      concurrency: config.concurrency ?? 1,
      defaultTimeoutMs: config.defaultTimeoutMs ?? 3e4
    });
    this.http = new TaskHttpController(this);
  }
  /**
   * Registers a typed task definition in the engine.
   */
  registerTask(task) {
    this.taskRegistry.set(task.name, task);
    return this;
  }
  /**
   * Registers multiple task definitions in bulk.
   */
  registerTasks(tasks) {
    tasks.forEach((t) => this.taskRegistry.set(t.name, t));
    return this;
  }
  /**
   * Starts the background queue worker.
   */
  start() {
    this.worker.start();
    return this;
  }
  /**
   * Gracefully stops the background queue worker.
   */
  async stop(drainTimeoutMs) {
    await this.worker.stop(drainTimeoutMs);
  }
};

// src/core/task.ts
var TaskValidationError = class extends Error {
  issues;
  constructor(message, issues) {
    super(message);
    this.name = "TaskValidationError";
    this.issues = issues;
  }
};
function defineTask(options) {
  const { name, schema, timeoutMs, handler } = options;
  return {
    name,
    schema,
    timeoutMs,
    handler,
    async run(rawPayload, ctx) {
      let validatedPayload = rawPayload;
      if (schema) {
        const parseResult = schema.safeParse(rawPayload);
        if (!parseResult.success) {
          const formatted = parseResult.error.issues.map((i) => `${i.path.join(".") || "root"}: ${i.message}`).join("; ");
          const err = new TaskValidationError(
            `Task [${name}] payload validation failed: ${formatted}`,
            parseResult.error.issues
          );
          ctx.logger.error(err.message, err, { issues: parseResult.error.issues });
          throw err;
        }
        validatedPayload = parseResult.data;
      }
      const taskTimeout = timeoutMs ?? 3e4;
      const timeoutController = new AbortController();
      const onParentAbort = () => timeoutController.abort(ctx.signal.reason);
      if (ctx.signal.aborted) {
        timeoutController.abort(ctx.signal.reason);
      } else {
        ctx.signal.addEventListener("abort", onParentAbort, { once: true });
      }
      const timerId = setTimeout(() => {
        timeoutController.abort(
          new Error(`Task [${name}] timed out after ${taskTimeout}ms`)
        );
      }, taskTimeout);
      const scopedContext = {
        ...ctx,
        signal: timeoutController.signal
      };
      try {
        return await handler(validatedPayload, scopedContext);
      } finally {
        clearTimeout(timerId);
        ctx.signal.removeEventListener("abort", onParentAbort);
      }
    }
  };
}

// src/db/supabase.ts
function createSupabaseAdapter(client, options) {
  const schema = options?.schema || "common";
  const getTable = (table) => {
    return client.schema ? client.schema(schema).from(table) : client.from(table);
  };
  return {
    raw: client,
    async rpc(functionName, args) {
      const res = await client.rpc(functionName, args);
      if (res.error) {
        const err = res.error instanceof Error ? res.error : new Error(String(res.error?.message || res.error));
        return { data: null, error: err };
      }
      return { data: res.data, error: null };
    },
    async enqueueJob(opts) {
      const { data, error } = await this.rpc("fn_enqueue_job", {
        p_tenant_id: opts.tenantId,
        p_target_path: opts.targetPath,
        p_payload: opts.payload ?? {},
        p_run_at: opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString(),
        p_max_attempts: opts.maxAttempts ?? 3
      });
      if (error || !data) {
        const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString();
        const res = await getTable("queued_jobs").insert({
          tenant_id: opts.tenantId,
          target_path: opts.targetPath,
          payload: opts.payload ?? {},
          run_at: runAt,
          max_attempts: opts.maxAttempts ?? 3,
          status: "pending"
        }).select("id").single();
        if (res.error) throw new Error(`Failed to enqueue job: ${res.error.message}`);
        return { id: res.data.id };
      }
      return { id: data };
    },
    async removeJob(jobId) {
      const res = await getTable("queued_jobs").delete().eq("id", jobId);
      if (res.error) {
        throw new Error(`Failed to remove job ${jobId}: ${res.error.message}`);
      }
    },
    async rescheduleJob(opts) {
      const res = await getTable("queued_jobs").update({
        status: "pending",
        attempts: opts.attempts,
        run_at: new Date(opts.runAt).toISOString(),
        updated_at: (/* @__PURE__ */ new Date()).toISOString()
      }).eq("id", opts.jobId);
      if (res.error) {
        throw new Error(`Failed to reschedule job ${opts.jobId}: ${res.error.message}`);
      }
    },
    async recordJobLog(log) {
      const res = await getTable("queued_job_logs").insert({
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
    async getJob(jobId) {
      const res = await getTable("queued_jobs").select("*").eq("id", jobId).maybeSingle();
      if (res.error) throw new Error(`Failed to fetch job ${jobId}: ${res.error.message}`);
      return res.data || null;
    },
    async cancelJob(jobId, tenantId) {
      let query = getTable("queued_jobs").delete().eq("id", jobId).eq("status", "pending");
      if (tenantId) {
        query = query.eq("tenant_id", tenantId);
      }
      const res = await query.select("id");
      if (res.error) throw new Error(`Failed to cancel job: ${res.error.message}`);
      return res.data && res.data.length > 0 || false;
    },
    async recordScheduledLog(log) {
      if (log.id) {
        const res2 = await getTable("scheduled_job_logs").update({
          status: log.status,
          finished_at: log.finishedAt ?? (/* @__PURE__ */ new Date()).toISOString(),
          duration_ms: log.durationMs ?? null,
          response_payload: log.responsePayload ?? null,
          error_message: log.errorMessage ?? null
        }).eq("id", log.id).select("id").single();
        if (res2.error) throw new Error(`Failed to update scheduled log: ${res2.error.message}`);
        await getTable("scheduled_jobs").update({
          status: log.status,
          last_run_at: log.finishedAt ?? (/* @__PURE__ */ new Date()).toISOString()
        }).eq("id", log.scheduledJobId);
        return { id: log.id };
      }
      const res = await getTable("scheduled_job_logs").insert({
        job_id: log.scheduledJobId,
        tenant_id: log.tenantId,
        status: log.status,
        triggered_by: log.triggeredBy,
        started_at: log.startedAt,
        finished_at: log.finishedAt ?? null,
        duration_ms: log.durationMs ?? null,
        response_payload: log.responsePayload ?? null,
        error_message: log.errorMessage ?? null
      }).select("id").single();
      if (res.error) throw new Error(`Failed to record scheduled log: ${res.error.message}`);
      return { id: res.data.id };
    }
  };
}
function createSupabaseRestAdapter(options) {
  const { url, serviceKey, schema = "common" } = options;
  const baseUrl = url.replace(/\/+$/, "");
  const defaultHeaders = {
    apikey: serviceKey,
    Authorization: `Bearer ${serviceKey}`,
    "Content-Type": "application/json"
  };
  const rawClient = {
    url: baseUrl,
    serviceKey,
    async query(path, opts = {}) {
      const targetSchema = opts.schema || "gistar";
      const cleanPath = path.replace(/^\/+/, "");
      const res = await fetch(`${baseUrl}/rest/v1/${cleanPath}`, {
        ...opts,
        headers: {
          ...defaultHeaders,
          "Accept-Profile": targetSchema,
          "Content-Profile": targetSchema,
          ...opts.headers
        }
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`DB Query Error: ${res.status} - ${txt}`);
      }
      return res.json();
    },
    async rpc(fn, args, opts = {}) {
      const targetSchema = opts.schema || schema;
      try {
        const res = await fetch(`${baseUrl}/rest/v1/rpc/${fn}`, {
          method: "POST",
          headers: {
            ...defaultHeaders,
            "Accept-Profile": targetSchema,
            "Content-Profile": targetSchema
          },
          body: JSON.stringify(args ?? {})
        });
        if (!res.ok) {
          const txt = await res.text();
          return { data: null, error: new Error(`RPC Error (${res.status}): ${txt}`) };
        }
        const data = await res.json();
        return { data, error: null };
      } catch (err) {
        return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
      }
    }
  };
  return {
    raw: rawClient,
    async rpc(functionName, args) {
      return rawClient.rpc(functionName, args, { schema });
    },
    async enqueueJob(opts) {
      const { data, error } = await this.rpc("fn_enqueue_job", {
        p_tenant_id: opts.tenantId,
        p_target_path: opts.targetPath,
        p_payload: opts.payload ?? {},
        p_run_at: opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString(),
        p_max_attempts: opts.maxAttempts ?? 3
      });
      if (!error && data) {
        return { id: typeof data === "object" && data?.id ? data.id : String(data) };
      }
      const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString();
      const res = await fetch(`${baseUrl}/rest/v1/queued_jobs`, {
        method: "POST",
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema,
          "Content-Profile": schema,
          Prefer: "return=representation"
        },
        body: JSON.stringify({
          tenant_id: opts.tenantId,
          target_path: opts.targetPath,
          payload: opts.payload ?? {},
          run_at: runAt,
          max_attempts: opts.maxAttempts ?? 3,
          status: "pending"
        })
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to enqueue job: ${res.status} - ${txt}`);
      }
      const rows = await res.json();
      return { id: rows[0].id };
    },
    async removeJob(jobId) {
      const res = await fetch(`${baseUrl}/rest/v1/queued_jobs?id=eq.${jobId}`, {
        method: "DELETE",
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema,
          "Content-Profile": schema
        }
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to remove job ${jobId}: ${res.status} - ${txt}`);
      }
    },
    async rescheduleJob(opts) {
      const res = await fetch(`${baseUrl}/rest/v1/queued_jobs?id=eq.${opts.jobId}`, {
        method: "PATCH",
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema,
          "Content-Profile": schema
        },
        body: JSON.stringify({
          status: "pending",
          attempts: opts.attempts,
          run_at: new Date(opts.runAt).toISOString(),
          updated_at: (/* @__PURE__ */ new Date()).toISOString()
        })
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to reschedule job ${opts.jobId}: ${res.status} - ${txt}`);
      }
    },
    async recordJobLog(log) {
      const res = await fetch(`${baseUrl}/rest/v1/queued_job_logs`, {
        method: "POST",
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema,
          "Content-Profile": schema
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
    async getJob(jobId) {
      const res = await fetch(`${baseUrl}/rest/v1/queued_jobs?id=eq.${jobId}&limit=1`, {
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema
        }
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to get job ${jobId}: ${res.status} - ${txt}`);
      }
      const data = await res.json();
      return data && data.length > 0 ? data[0] : null;
    },
    async cancelJob(jobId, tenantId) {
      let urlStr = `${baseUrl}/rest/v1/queued_jobs?id=eq.${jobId}&status=eq.pending`;
      if (tenantId) urlStr += `&tenant_id=eq.${tenantId}`;
      const res = await fetch(urlStr, {
        method: "DELETE",
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema,
          Prefer: "return=representation"
        }
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to cancel job: ${res.status} - ${txt}`);
      }
      const data = await res.json();
      return Array.isArray(data) && data.length > 0;
    },
    async recordScheduledLog(log) {
      if (log.id) {
        const logRes = await fetch(`${baseUrl}/rest/v1/scheduled_job_logs?id=eq.${log.id}`, {
          method: "PATCH",
          headers: {
            ...defaultHeaders,
            "Accept-Profile": schema,
            "Content-Profile": schema
          },
          body: JSON.stringify({
            status: log.status,
            finished_at: log.finishedAt ?? (/* @__PURE__ */ new Date()).toISOString(),
            duration_ms: log.durationMs ?? null,
            response_payload: log.responsePayload ?? null,
            error_message: log.errorMessage ?? null
          })
        });
        if (!logRes.ok) {
          const txt = await logRes.text();
          throw new Error(`Failed to update scheduled log: ${logRes.status} - ${txt}`);
        }
        await fetch(`${baseUrl}/rest/v1/scheduled_jobs?id=eq.${log.scheduledJobId}`, {
          method: "PATCH",
          headers: {
            ...defaultHeaders,
            "Accept-Profile": schema,
            "Content-Profile": schema
          },
          body: JSON.stringify({
            status: log.status,
            last_run_at: log.finishedAt ?? (/* @__PURE__ */ new Date()).toISOString()
          })
        }).catch(() => {
        });
        return { id: log.id };
      }
      const res = await fetch(`${baseUrl}/rest/v1/scheduled_job_logs`, {
        method: "POST",
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema,
          "Content-Profile": schema,
          Prefer: "return=representation"
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

// src/db/prisma.ts
function createPrismaAdapter(prisma, options) {
  const schema = options?.schema || "common";
  return {
    raw: prisma,
    async rpc(functionName, args) {
      try {
        if (!args || Object.keys(args).length === 0) {
          const rows2 = await prisma.$queryRawUnsafe(
            `SELECT * FROM ${schema}.${functionName}()`
          );
          return { data: rows2 && rows2.length > 0 ? rows2 : null, error: null };
        }
        const keys = Object.keys(args);
        const placeholders = keys.map((_, i) => `$${i + 1}`).join(", ");
        const values = Object.values(args).map(
          (val) => typeof val === "object" && val !== null ? JSON.stringify(val) : val
        );
        const rows = await prisma.$queryRawUnsafe(
          `SELECT * FROM ${schema}.${functionName}(${placeholders})`,
          ...values
        );
        return { data: rows && rows.length > 0 ? rows : null, error: null };
      } catch (err) {
        return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
      }
    },
    async enqueueJob(opts) {
      const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString();
      const payloadJson = JSON.stringify(opts.payload ?? {});
      const rows = await prisma.$queryRawUnsafe(
        `INSERT INTO ${schema}.queued_jobs (tenant_id, target_path, payload, run_at, max_attempts, status)
         VALUES ($1::uuid, $2, $3::jsonb, $4::timestamptz, $5, 'pending')
         RETURNING id`,
        opts.tenantId,
        opts.targetPath,
        payloadJson,
        runAt,
        opts.maxAttempts ?? 3
      );
      if (!rows || rows.length === 0) {
        throw new Error("PrismaAdapter: Failed to enqueue job, no ID returned.");
      }
      return { id: rows[0].id };
    },
    async removeJob(jobId) {
      await prisma.$executeRawUnsafe(
        `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid`,
        jobId
      );
    },
    async rescheduleJob(opts) {
      await prisma.$executeRawUnsafe(
        `UPDATE ${schema}.queued_jobs
         SET status = 'pending', attempts = $1, run_at = $2::timestamptz, updated_at = NOW()
         WHERE id = $3::uuid`,
        opts.attempts,
        new Date(opts.runAt).toISOString(),
        opts.jobId
      );
    },
    async recordJobLog(log) {
      await prisma.$executeRawUnsafe(
        `INSERT INTO ${schema}.queued_job_logs 
         (job_id, tenant_id, target_path, status, started_at, finished_at, duration_ms, response_payload, error_message, attempts)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8::jsonb, $9, $10)`,
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
      );
    },
    async getJob(jobId) {
      const rows = await prisma.$queryRawUnsafe(
        `SELECT * FROM ${schema}.queued_jobs WHERE id = $1::uuid LIMIT 1`,
        jobId
      );
      return rows && rows.length > 0 ? rows[0] : null;
    },
    async cancelJob(jobId, tenantId) {
      if (tenantId) {
        const count2 = await prisma.$executeRawUnsafe(
          `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid AND tenant_id = $2::uuid AND status = 'pending'`,
          jobId,
          tenantId
        );
        return count2 > 0;
      }
      const count = await prisma.$executeRawUnsafe(
        `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid AND status = 'pending'`,
        jobId
      );
      return count > 0;
    },
    async recordScheduledLog(log) {
      const rows = await prisma.$queryRawUnsafe(
        `INSERT INTO ${schema}.scheduled_job_logs
         (job_id, tenant_id, status, triggered_by, started_at, finished_at, duration_ms, response_payload, error_message)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8::jsonb, $9)
         RETURNING id`,
        log.scheduledJobId,
        log.tenantId,
        log.status,
        log.triggeredBy,
        log.startedAt,
        log.finishedAt ?? null,
        log.durationMs ?? null,
        log.responsePayload ? JSON.stringify(log.responsePayload) : null,
        log.errorMessage ?? null
      );
      return { id: rows[0].id };
    }
  };
}

// src/db/drizzle.ts
function createDrizzleAdapter(drizzleDb, options) {
  const schema = options?.schema || "common";
  const runSql = async (sqlQuery, params = []) => {
    const result = await drizzleDb.execute({
      sql: sqlQuery,
      params
    });
    if (Array.isArray(result)) return result;
    if (result && Array.isArray(result.rows)) return result.rows;
    return [];
  };
  return {
    raw: drizzleDb,
    async rpc(functionName, args) {
      try {
        if (!args || Object.keys(args).length === 0) {
          const rows2 = await runSql(`SELECT * FROM ${schema}.${functionName}()`);
          return { data: rows2 && rows2.length > 0 ? rows2 : null, error: null };
        }
        const keys = Object.keys(args);
        const placeholders = keys.map((_, i) => `$${i + 1}`).join(", ");
        const values = Object.values(args).map(
          (val) => typeof val === "object" && val !== null ? JSON.stringify(val) : val
        );
        const rows = await runSql(
          `SELECT * FROM ${schema}.${functionName}(${placeholders})`,
          values
        );
        return { data: rows && rows.length > 0 ? rows : null, error: null };
      } catch (err) {
        return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
      }
    },
    async enqueueJob(opts) {
      const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString();
      const payloadJson = JSON.stringify(opts.payload ?? {});
      const rows = await runSql(
        `INSERT INTO ${schema}.queued_jobs (tenant_id, target_path, payload, run_at, max_attempts, status)
         VALUES ($1::uuid, $2, $3::jsonb, $4::timestamptz, $5, 'pending')
         RETURNING id`,
        [opts.tenantId, opts.targetPath, payloadJson, runAt, opts.maxAttempts ?? 3]
      );
      if (!rows || rows.length === 0) throw new Error("DrizzleAdapter: Failed to enqueue job.");
      return { id: rows[0].id };
    },
    async removeJob(jobId) {
      await runSql(`DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid`, [jobId]);
    },
    async rescheduleJob(opts) {
      await runSql(
        `UPDATE ${schema}.queued_jobs
         SET status = 'pending', attempts = $1, run_at = $2::timestamptz, updated_at = NOW()
         WHERE id = $3::uuid`,
        [opts.attempts, new Date(opts.runAt).toISOString(), opts.jobId]
      );
    },
    async recordJobLog(log) {
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
    async getJob(jobId) {
      const rows = await runSql(
        `SELECT * FROM ${schema}.queued_jobs WHERE id = $1::uuid LIMIT 1`,
        [jobId]
      );
      return rows && rows.length > 0 ? rows[0] : null;
    },
    async cancelJob(jobId, tenantId) {
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
    async recordScheduledLog(log) {
      const rows = await runSql(
        `INSERT INTO ${schema}.scheduled_job_logs
         (job_id, tenant_id, status, triggered_by, started_at, finished_at, duration_ms, response_payload, error_message)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8::jsonb, $9)
         RETURNING id`,
        [
          log.scheduledJobId,
          log.tenantId,
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

// src/db/pg.ts
function createPgAdapter(clientOrPool, options) {
  const schema = options?.schema || "common";
  return {
    raw: clientOrPool,
    async rpc(functionName, args) {
      try {
        if (!args || Object.keys(args).length === 0) {
          const res2 = await clientOrPool.query(`SELECT * FROM ${schema}.${functionName}()`);
          return { data: res2.rows && res2.rows.length > 0 ? res2.rows : null, error: null };
        }
        const keys = Object.keys(args);
        const placeholders = keys.map((_, i) => `$${i + 1}`).join(", ");
        const values = Object.values(args).map(
          (val) => typeof val === "object" && val !== null ? JSON.stringify(val) : val
        );
        const res = await clientOrPool.query(
          `SELECT * FROM ${schema}.${functionName}(${placeholders})`,
          values
        );
        return { data: res.rows && res.rows.length > 0 ? res.rows : null, error: null };
      } catch (err) {
        return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
      }
    },
    async enqueueJob(opts) {
      const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString();
      const payloadJson = JSON.stringify(opts.payload ?? {});
      const res = await clientOrPool.query(
        `INSERT INTO ${schema}.queued_jobs (tenant_id, target_path, payload, run_at, max_attempts, status)
         VALUES ($1::uuid, $2, $3::jsonb, $4::timestamptz, $5, 'pending')
         RETURNING id`,
        [opts.tenantId, opts.targetPath, payloadJson, runAt, opts.maxAttempts ?? 3]
      );
      if (!res.rows || res.rows.length === 0) {
        throw new Error("PgAdapter: Failed to enqueue job.");
      }
      return { id: res.rows[0].id };
    },
    async removeJob(jobId) {
      await clientOrPool.query(`DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid`, [jobId]);
    },
    async rescheduleJob(opts) {
      await clientOrPool.query(
        `UPDATE ${schema}.queued_jobs
         SET status = 'pending', attempts = $1, run_at = $2::timestamptz, updated_at = NOW()
         WHERE id = $3::uuid`,
        [opts.attempts, new Date(opts.runAt).toISOString(), opts.jobId]
      );
    },
    async recordJobLog(log) {
      await clientOrPool.query(
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
    async getJob(jobId) {
      const res = await clientOrPool.query(
        `SELECT * FROM ${schema}.queued_jobs WHERE id = $1::uuid LIMIT 1`,
        [jobId]
      );
      return res.rows && res.rows.length > 0 ? res.rows[0] : null;
    },
    async cancelJob(jobId, tenantId) {
      if (tenantId) {
        const res2 = await clientOrPool.query(
          `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid AND tenant_id = $2::uuid AND status = 'pending'`,
          [jobId, tenantId]
        );
        return (res2.rowCount ?? 0) > 0;
      }
      const res = await clientOrPool.query(
        `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid AND status = 'pending'`,
        [jobId]
      );
      return (res.rowCount ?? 0) > 0;
    },
    async recordScheduledLog(log) {
      const res = await clientOrPool.query(
        `INSERT INTO ${schema}.scheduled_job_logs
         (job_id, tenant_id, status, triggered_by, started_at, finished_at, duration_ms, response_payload, error_message)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8::jsonb, $9)
         RETURNING id`,
        [
          log.scheduledJobId,
          log.tenantId,
          log.status,
          log.triggeredBy,
          log.startedAt,
          log.finishedAt ?? null,
          log.durationMs ?? null,
          log.responsePayload ? JSON.stringify(log.responsePayload) : null,
          log.errorMessage ?? null
        ]
      );
      return { id: res.rows[0].id };
    }
  };
}

exports.DomainEventsManager = DomainEventsManager;
exports.TaskEngine = TaskEngine;
exports.TaskHttpController = TaskHttpController;
exports.TaskQueue = TaskQueue;
exports.TaskScheduler = TaskScheduler;
exports.TaskValidationError = TaskValidationError;
exports.TaskWorker = TaskWorker;
exports.createDrizzleAdapter = createDrizzleAdapter;
exports.createPgAdapter = createPgAdapter;
exports.createPrismaAdapter = createPrismaAdapter;
exports.createSupabaseAdapter = createSupabaseAdapter;
exports.createSupabaseRestAdapter = createSupabaseRestAdapter;
exports.defineTask = defineTask;
exports.resolveDatabaseAdapter = resolveDatabaseAdapter;
//# sourceMappingURL=index.js.map
//# sourceMappingURL=index.js.map