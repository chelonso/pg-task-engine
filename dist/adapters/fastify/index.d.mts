import { T as TaskEngine } from '../../engine-BS6Cv11c.mjs';
import '../../types-BHtRB4sA.mjs';
import 'zod';

interface FastifyPluginOptions {
    engine: TaskEngine;
    /** Automatically starts the worker on fastify.ready and stops on onClose (default: true) */
    manageWorkerLifecycle?: boolean;
    /** Optional preHandler authentication hook */
    preHandler?: any;
}
/**
 * Native Fastify Plugin for @shared/pg-task-engine
 */
declare function taskEngineFastifyPlugin(fastify: any, opts: FastifyPluginOptions): Promise<void>;

export { type FastifyPluginOptions, taskEngineFastifyPlugin };
