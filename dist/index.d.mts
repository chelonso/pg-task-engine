export { D as DomainEventsManager, E as ExecuteScheduledTaskOptions, S as ScheduledTaskResult, T as TaskEngine, a as TaskHttpController, b as TaskQueue, c as TaskScheduler, d as TaskWorker, e as TaskWorkerOptions } from './engine-Dj6GarSH.mjs';
import { z } from 'zod';
import { D as DefineTaskOptions, T as TaskDefinition } from './types-CfVXbtgI.mjs';
export { a as DatabaseAdapter, b as DomainEventOptions, E as EnqueueJobOptions, J as JobRecord, c as JobStatusResponse, P as PolledJob, d as TaskContext, e as TaskEngineConfig, f as TaskHandler, g as TaskLogger } from './types-CfVXbtgI.mjs';
export { createDrizzleAdapter, createPgAdapter, createPrismaAdapter, createSupabaseAdapter, createSupabaseRestAdapter, resolveDatabaseAdapter } from './db/index.mjs';

declare class TaskValidationError extends Error {
    readonly issues: z.ZodIssue[];
    constructor(message: string, issues: z.ZodIssue[]);
}
/**
 * Creates a type-safe task definition with optional Zod payload validation
 * and execution timeout configuration.
 */
declare function defineTask<TPayload = unknown, TResult = unknown, TDb = unknown>(options: DefineTaskOptions<TPayload, TResult, TDb>): TaskDefinition<TPayload, TResult, TDb>;

export { DefineTaskOptions, TaskDefinition, TaskValidationError, defineTask };
