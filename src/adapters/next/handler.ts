import type { TaskEngine } from '../../core/engine.js';

export interface NextRouteContext {
  params: {
    route?: string[];
  } | Promise<{ route?: string[] }>;
}

/**
 * Creates Next.js App Router Catch-all Handlers (GET, POST, DELETE).
 * Place inside app/api/tasks/[...route]/route.ts:
 *
 * export const { GET, POST, DELETE } = createTaskEngineNextHandler(engine);
 */
export function createTaskEngineNextHandler(engine: TaskEngine) {
  const controller = engine.http;

  const resolveParams = async (ctx: NextRouteContext) => {
    return ctx.params instanceof Promise ? await ctx.params : ctx.params;
  };

  const GET = async (request: Request, ctx: NextRouteContext): Promise<Response> => {
    const params = await resolveParams(ctx);
    const route = params.route || [];

    // GET /api/tasks/status/:id
    if (route[0] === 'status' && route[1]) {
      return await controller.handleGetStatus(route[1]);
    }

    return new Response(JSON.stringify({ error: 'Endpoint not found' }), {
      status: 404,
      headers: { 'Content-Type': 'application/json' }
    });
  };

  const POST = async (request: Request, ctx: NextRouteContext): Promise<Response> => {
    const params = await resolveParams(ctx);
    const route = params.route || [];

    // POST /api/tasks/add
    if (route[0] === 'add') {
      return await controller.handleEnqueue(request);
    }

    // POST /api/tasks/cron/execute
    if (route[0] === 'cron' && route[1] === 'execute') {
      return await controller.handleCronExecute(request);
    }

    // POST /api/tasks/events/publish
    if (route[0] === 'events' && route[1] === 'publish') {
      return await controller.handlePublishEvent(request);
    }

    return new Response(JSON.stringify({ error: 'Endpoint not found' }), {
      status: 404,
      headers: { 'Content-Type': 'application/json' }
    });
  };

  const DELETE = async (request: Request, ctx: NextRouteContext): Promise<Response> => {
    const params = await resolveParams(ctx);
    const route = params.route || [];

    // DELETE /api/tasks/cancel/:id
    if (route[0] === 'cancel' && route[1]) {
      const url = new URL(request.url);
      const tenantId = url.searchParams.get('tenant_id') || url.searchParams.get('tenantId') || undefined;
      return await controller.handleCancel(route[1], tenantId);
    }

    return new Response(JSON.stringify({ error: 'Endpoint not found' }), {
      status: 404,
      headers: { 'Content-Type': 'application/json' }
    });
  };

  return { GET, POST, DELETE };
}
