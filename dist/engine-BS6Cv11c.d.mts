import { a as DatabaseAdapter, b as DomainEventOptions, E as EnqueueJobOptions, c as JobStatusResponse, T as TaskDefinition, g as TaskLogger, e as TaskEngineConfig } from './types-BHtRB4sA.mjs';

declare class DomainEventsManager {
    private db;
    constructor(db: DatabaseAdapter);
    /**
     * Publishes a domain event into the common.domain_events outbox table.
     */
    publish(options: DomainEventOptions): Promise<{
        id: string;
        status: 'pending';
    }>;
    /**
     * Processes an outbox domain event, evaluating subscriptions and enqueuing jobs.
     */
    processEvent(eventId: string): Promise<{
        matchedSubscriptions: number;
    }>;
}

declare class TaskHttpController {
    private engine;
    constructor(engine: TaskEngine);
    /**
     * POST /add (Enqueue a job)
     */
    handleEnqueue(request: Request): Promise<Response>;
    /**
     * GET /status/:id (Get job status)
     */
    handleGetStatus(jobId: string): Promise<Response>;
    /**
     * DELETE /cancel/:id (Cancel a pending job)
     */
    handleCancel(jobId: string, tenantId?: string): Promise<Response>;
    /**
     * POST /cron/execute (Trigger scheduled cron execution)
     */
    handleCronExecute(request: Request): Promise<Response>;
    /**
     * POST /events/publish (Publish a domain event)
     */
    handlePublishEvent(request: Request): Promise<Response>;
}

declare class TaskQueue {
    private db;
    constructor(db: DatabaseAdapter);
    /**
     * Enqueues a job into common.queued_jobs.
     */
    enqueue(options: EnqueueJobOptions): Promise<{
        id: string;
        status: 'pending';
    }>;
    /**
     * Retrieves the current execution status of a job (active or logged).
     */
    getStatus(jobId: string): Promise<JobStatusResponse>;
    /**
     * Cancels a pending job from the active queue.
     */
    cancel(jobId: string, tenantId?: string): Promise<{
        success: boolean;
        message: string;
    }>;
}

interface ExecuteScheduledTaskOptions {
    jobId: string;
    tenantId: string;
    targetPath: string;
    payload?: unknown;
    triggeredBy?: 'cron' | 'manual' | 'api';
}
interface ScheduledTaskResult {
    logId: string;
    status: 'success' | 'failed';
    durationMs: number;
    result?: unknown;
    error?: string;
}
declare class TaskScheduler {
    private db;
    private taskRegistry;
    private logger;
    private defaultTimeoutMs;
    constructor(db: DatabaseAdapter, taskRegistry: Map<string, TaskDefinition>, logger: TaskLogger, defaultTimeoutMs?: number);
    /**
     * Executes a scheduled task directly (triggered by pg_cron or manual HTTP invocation).
     */
    execute(options: ExecuteScheduledTaskOptions): Promise<ScheduledTaskResult>;
}

interface TaskWorkerOptions {
    db: DatabaseAdapter;
    taskRegistry: Map<string, TaskDefinition>;
    logger: TaskLogger;
    pollIntervalMs?: number;
    concurrency?: number;
    defaultTimeoutMs?: number;
}
declare class TaskWorker {
    private db;
    private taskRegistry;
    private logger;
    private pollIntervalMs;
    private concurrency;
    private defaultTimeoutMs;
    private isRunning;
    private isDraining;
    private timerId;
    private activeJobsCount;
    private drainResolvers;
    constructor(options: TaskWorkerOptions);
    /**
     * Starts the polling loop and registers process shutdown signals.
     */
    start(): void;
    /**
     * Gracefully stops the worker, waiting for all active jobs to finish.
     */
    stop(drainTimeoutMs?: number): Promise<void>;
    private registerSignalHandlers;
    private scheduleNextPoll;
    private pollAndDispatch;
    private processJob;
    private removeJob;
    private rescheduleJob;
    private recordLog;
    private createScopedLogger;
}

declare class TaskEngine<TDb = unknown> {
    readonly db: DatabaseAdapter;
    readonly queue: TaskQueue;
    readonly scheduler: TaskScheduler;
    readonly events: DomainEventsManager;
    readonly worker: TaskWorker;
    readonly http: TaskHttpController;
    readonly logger: TaskLogger;
    private taskRegistry;
    constructor(config: TaskEngineConfig<TDb>);
    /**
     * Registers a typed task definition in the engine.
     */
    registerTask(task: TaskDefinition): this;
    /**
     * Registers multiple task definitions in bulk.
     */
    registerTasks(tasks: TaskDefinition[]): this;
    /**
     * Starts the background queue worker.
     */
    start(): this;
    /**
     * Gracefully stops the background queue worker.
     */
    stop(drainTimeoutMs?: number): Promise<void>;
}

export { DomainEventsManager as D, type ExecuteScheduledTaskOptions as E, type ScheduledTaskResult as S, TaskEngine as T, TaskHttpController as a, TaskQueue as b, TaskScheduler as c, TaskWorker as d, type TaskWorkerOptions as e };
