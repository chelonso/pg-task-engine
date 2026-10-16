// src/adapters/fastify/plugin.ts
async function taskEngineFastifyPlugin(fastify, opts) {
  const { engine, manageWorkerLifecycle = true, preHandler } = opts;
  const controller = engine.http;
  const routeOptions = preHandler ? { preHandler } : {};
  const handleEnqueueReq = async (req, reply) => {
    const fullUrl = `${req.protocol}://${req.hostname}${req.url}`;
    const webRequest = new Request(fullUrl, {
      method: req.method,
      headers: new Headers(req.headers),
      body: req.body ? JSON.stringify(req.body) : void 0
    });
    const webResponse = await controller.handleEnqueue(webRequest);
    const data = await webResponse.json();
    reply.status(webResponse.status);
    return data;
  };
  fastify.post("/add", routeOptions, handleEnqueueReq);
  fastify.post("/queue/add", routeOptions, handleEnqueueReq);
  const handleStatusReq = async (req, reply) => {
    const webResponse = await controller.handleGetStatus(req.params.id);
    const data = await webResponse.json();
    reply.status(webResponse.status);
    return data;
  };
  fastify.get("/status/:id", routeOptions, handleStatusReq);
  fastify.get("/queue/status/:id", routeOptions, handleStatusReq);
  const handleCancelReq = async (req, reply) => {
    const tenantId = req.query?.tenant_id || req.query?.tenantId || req.user?.tenant_id;
    const webResponse = await controller.handleCancel(req.params.id, tenantId);
    const data = await webResponse.json();
    reply.status(webResponse.status);
    return data;
  };
  fastify.delete("/cancel/:id", routeOptions, handleCancelReq);
  fastify.delete("/queue/cancel/:id", routeOptions, handleCancelReq);
  const handleCronReq = async (req, reply) => {
    const fullUrl = `${req.protocol}://${req.hostname}${req.url}`;
    const webRequest = new Request(fullUrl, {
      method: req.method,
      headers: new Headers(req.headers),
      body: req.body ? JSON.stringify(req.body) : void 0
    });
    const webResponse = await controller.handleCronExecute(webRequest);
    const data = await webResponse.json();
    reply.status(webResponse.status);
    return data;
  };
  fastify.post("/cron/execute", routeOptions, handleCronReq);
  fastify.post("/execute", routeOptions, handleCronReq);
  const handleEventReq = async (req, reply) => {
    const fullUrl = `${req.protocol}://${req.hostname}${req.url}`;
    const webRequest = new Request(fullUrl, {
      method: req.method,
      headers: new Headers(req.headers),
      body: req.body ? JSON.stringify(req.body) : void 0
    });
    const webResponse = await controller.handlePublishEvent(webRequest);
    const data = await webResponse.json();
    reply.status(webResponse.status);
    return data;
  };
  fastify.post("/events/publish", routeOptions, handleEventReq);
  fastify.post("/publish", routeOptions, handleEventReq);
  if (manageWorkerLifecycle) {
    fastify.ready(() => {
      engine.start();
    });
    fastify.addHook("onClose", async () => {
      await engine.stop();
    });
  }
}

export { taskEngineFastifyPlugin };
//# sourceMappingURL=index.mjs.map
//# sourceMappingURL=index.mjs.map