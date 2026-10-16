import { z } from 'zod';
import type { DefineTaskOptions, TaskContext, TaskDefinition } from '../types.js';

export class TaskValidationError extends Error {
  public readonly issues: z.ZodIssue[];

  constructor(message: string, issues: z.ZodIssue[]) {
    super(message);
    this.name = 'TaskValidationError';
    this.issues = issues;
  }
}

/**
 * Creates a type-safe task definition with optional Zod payload validation
 * and execution timeout configuration.
 */
export function defineTask<TPayload = unknown, TResult = unknown, TDb = unknown>(
  options: DefineTaskOptions<TPayload, TResult, TDb>
): TaskDefinition<TPayload, TResult, TDb> {
  const { name, schema, timeoutMs, handler } = options;

  return {
    name,
    schema,
    timeoutMs,
    handler,

    async run(rawPayload: unknown, ctx: TaskContext<TDb>): Promise<TResult> {
      let validatedPayload: TPayload = rawPayload as TPayload;

      // 1. Zod schema validation
      if (schema) {
        const parseResult = schema.safeParse(rawPayload);
        if (!parseResult.success) {
          const formatted = parseResult.error.issues
            .map((i) => `${i.path.join('.') || 'root'}: ${i.message}`)
            .join('; ');
          const err = new TaskValidationError(
            `Task [${name}] payload validation failed: ${formatted}`,
            parseResult.error.issues
          );
          ctx.logger.error(err.message, err, { issues: parseResult.error.issues });
          throw err;
        }
        validatedPayload = parseResult.data;
      }

      // 2. Timeout handling with AbortController
      const taskTimeout = timeoutMs ?? 30000;
      const timeoutController = new AbortController();

      // Combine caller's signal and local task timeout signal
      const onParentAbort = () => timeoutController.abort(ctx.signal.reason);
      if (ctx.signal.aborted) {
        timeoutController.abort(ctx.signal.reason);
      } else {
        ctx.signal.addEventListener('abort', onParentAbort, { once: true });
      }

      const timerId = setTimeout(() => {
        timeoutController.abort(
          new Error(`Task [${name}] timed out after ${taskTimeout}ms`)
        );
      }, taskTimeout);

      const scopedContext: TaskContext<TDb> = {
        ...ctx,
        signal: timeoutController.signal
      };

      try {
        return await handler(validatedPayload, scopedContext);
      } finally {
        clearTimeout(timerId);
        ctx.signal.removeEventListener('abort', onParentAbort);
      }
    }
  };
}
