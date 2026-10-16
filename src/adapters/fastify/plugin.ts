import type { TaskEngine } from '../../core/engine.js';

export interface FastifyPluginOptions {
  engine: TaskEngine;
  /** Automatically starts the worker on fastify.ready and stops on onClose (default: true) */
  manageWorkerLifecycle?: boolean;
  /** Optional preHandler authentication hook */
  preHandler?: any;
}

/**
 * Native Fastify Plugin for @shared/pg-task-engine
 */
export async function taskEngineFastifyPlugin(fastify: any, opts: FastifyPluginOptions) {
  const { engine, manageWorkerLifecycle = true, preHandler } = opts;
  const controller = engine.http;

  const routeOptions = preHandler ? { preHandler } : {};

  // Helper handler for enqueue
  const handleEnqueueReq = async (req: any, reply: any) => {
    const fullUrl = `${req.protocol}://${req.hostname}${req.url}`;
    const webRequest = new Request(fullUrl, {
      method: req.method,
      headers: new Headers(req.headers as Record<string, string>),
      body: req.body ? JSON.stringify(req.body) : undefined
    });

    const webResponse = await controller.handleEnqueue(webRequest);
    const data = await webResponse.json();
    reply.status(webResponse.status);
    return data;
  };

  // 1. Enqueue job
  fastify.post('/add', routeOptions, handleEnqueueReq);
  fastify.post('/queue/add', routeOptions, handleEnqueueReq);

  // Helper handler for status
  const handleStatusReq = async (req: any, reply: any) => {
    const webResponse = await controller.handleGetStatus(req.params.id);
    const data = await webResponse.json();
    reply.status(webResponse.status);
    return data;
  };

  // 2. Get status
  fastify.get('/status/:id', routeOptions, handleStatusReq);
  fastify.get('/queue/status/:id', routeOptions, handleStatusReq);

  // Helper handler for cancel
  const handleCancelReq = async (req: any, reply: any) => {
    const tenantId = req.query?.tenant_id || req.query?.tenantId || req.user?.tenant_id;
    const webResponse = await controller.handleCancel(req.params.id, tenantId);
    const data = await webResponse.json();
    reply.status(webResponse.status);
    return data;
  };

  // 3. Cancel job
  fastify.delete('/cancel/:id', routeOptions, handleCancelReq);
  fastify.delete('/queue/cancel/:id', routeOptions, handleCancelReq);

  // Helper handler for cron execute
  const handleCronReq = async (req: any, reply: any) => {
    const fullUrl = `${req.protocol}://${req.hostname}${req.url}`;
    const webRequest = new Request(fullUrl, {
      method: req.method,
      headers: new Headers(req.headers as Record<string, string>),
      body: req.body ? JSON.stringify(req.body) : undefined
    });

    const webResponse = await controller.handleCronExecute(webRequest);
    const data = await webResponse.json();
    reply.status(webResponse.status);
    return data;
  };

  // 4. Cron execute (both /cron/execute and /execute for when mounted with prefix /cron)
  fastify.post('/cron/execute', routeOptions, handleCronReq);
  fastify.post('/execute', routeOptions, handleCronReq);

  // Helper handler for events publish
  const handleEventReq = async (req: any, reply: any) => {
    const fullUrl = `${req.protocol}://${req.hostname}${req.url}`;
    const webRequest = new Request(fullUrl, {
      method: req.method,
      headers: new Headers(req.headers as Record<string, string>),
      body: req.body ? JSON.stringify(req.body) : undefined
    });

    const webResponse = await controller.handlePublishEvent(webRequest);
    const data = await webResponse.json();
    reply.status(webResponse.status);
    return data;
  };

  // 5. Domain Events publish
  fastify.post('/events/publish', routeOptions, handleEventReq);
  fastify.post('/publish', routeOptions, handleEventReq);

  // Lifecycle management
  if (manageWorkerLifecycle) {
    fastify.ready(() => {
      engine.start();
    });

    fastify.addHook('onClose', async () => {
      await engine.stop();
    });
  }
}
