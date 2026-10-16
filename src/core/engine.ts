import { resolveDatabaseAdapter } from '../db/adapter.js';
import type { DatabaseAdapter, TaskDefinition, TaskEngineConfig, TaskLogger } from '../types.js';
import { DomainEventsManager } from './events.js';
import { TaskHttpController } from './http-handlers.js';
import { TaskQueue } from './queue.js';
import { TaskScheduler } from './scheduler.js';
import { TaskWorker } from './worker.js';

export class TaskEngine<TDb = unknown> {
  public readonly db: DatabaseAdapter;
  public readonly queue: TaskQueue;
  public readonly scheduler: TaskScheduler;
  public readonly events: DomainEventsManager;
  public readonly worker: TaskWorker;
  public readonly http: TaskHttpController;
  public readonly logger: TaskLogger;

  private taskRegistry: Map<string, TaskDefinition> = new Map();

  constructor(config: TaskEngineConfig<TDb>) {
    this.db = resolveDatabaseAdapter(config.db);

    this.logger = config.logger ?? {
      info: (msg, meta) => console.log(`[INFO] ${msg}`, meta ?? ''),
      warn: (msg, meta) => console.warn(`[WARN] ${msg}`, meta ?? ''),
      error: (msg, err, meta) => console.error(`[ERROR] ${msg}`, err ?? '', meta ?? ''),
      debug: (msg, meta) => console.debug?.(`[DEBUG] ${msg}`, meta ?? '')
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
      config.defaultTimeoutMs ?? 60000
    );

    this.worker = new TaskWorker({
      db: this.db,
      taskRegistry: this.taskRegistry,
      logger: this.logger,
      pollIntervalMs: config.pollIntervalMs ?? 5000,
      concurrency: config.concurrency ?? 1,
      defaultTimeoutMs: config.defaultTimeoutMs ?? 30000
    });

    this.http = new TaskHttpController(this);
  }

  /**
   * Registers a typed task definition in the engine.
   */
  registerTask(task: TaskDefinition): this {
    this.taskRegistry.set(task.name, task);
    return this;
  }

  /**
   * Registers multiple task definitions in bulk.
   */
  registerTasks(tasks: TaskDefinition[]): this {
    tasks.forEach((t) => this.taskRegistry.set(t.name, t));
    return this;
  }

  /**
   * Starts the background queue worker.
   */
  start(): this {
    this.worker.start();
    return this;
  }

  /**
   * Gracefully stops the background queue worker.
   */
  async stop(drainTimeoutMs?: number): Promise<void> {
    await this.worker.stop(drainTimeoutMs);
  }
}
