import { T as TaskEngine } from '../../engine-DrkGh7_9.js';
import '../../types-BHtRB4sA.js';
import 'zod';

interface NextRouteContext {
    params: {
        route?: string[];
    } | Promise<{
        route?: string[];
    }>;
}
/**
 * Creates Next.js App Router Catch-all Handlers (GET, POST, DELETE).
 * Place inside app/api/tasks/[...route]/route.ts:
 *
 * export const { GET, POST, DELETE } = createTaskEngineNextHandler(engine);
 */
declare function createTaskEngineNextHandler(engine: TaskEngine): {
    GET: (request: Request, ctx: NextRouteContext) => Promise<Response>;
    POST: (request: Request, ctx: NextRouteContext) => Promise<Response>;
    DELETE: (request: Request, ctx: NextRouteContext) => Promise<Response>;
};

export { type NextRouteContext, createTaskEngineNextHandler };
