// Core Engine
export { TaskEngine } from './core/engine.js';
export { TaskWorker, type TaskWorkerOptions } from './core/worker.js';
export { TaskQueue } from './core/queue.js';
export { TaskScheduler, type ExecuteScheduledTaskOptions, type ScheduledTaskResult } from './core/scheduler.js';
export { DomainEventsManager } from './core/events.js';
export { defineTask, TaskValidationError } from './core/task.js';
export { TaskHttpController } from './core/http-handlers.js';

// Database Adapters
export {
  resolveDatabaseAdapter,
  createSupabaseAdapter,
  createPrismaAdapter,
  createDrizzleAdapter,
  createPgAdapter
} from './db/index.js';

// Types
export type {
  DatabaseAdapter,
  TaskContext,
  TaskHandler,
  TaskDefinition,
  DefineTaskOptions,
  TaskLogger,
  JobRecord,
  PolledJob,
  EnqueueJobOptions,
  JobStatusResponse,
  DomainEventOptions,
  TaskEngineConfig
} from './types.js';
