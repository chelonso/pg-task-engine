// src/adapters/next/handler.ts
function createTaskEngineNextHandler(engine) {
  const controller = engine.http;
  const resolveParams = async (ctx) => {
    return ctx.params instanceof Promise ? await ctx.params : ctx.params;
  };
  const GET = async (request, ctx) => {
    const params = await resolveParams(ctx);
    const route = params.route || [];
    if (route[0] === "status" && route[1]) {
      return await controller.handleGetStatus(route[1]);
    }
    return new Response(JSON.stringify({ error: "Endpoint not found" }), {
      status: 404,
      headers: { "Content-Type": "application/json" }
    });
  };
  const POST = async (request, ctx) => {
    const params = await resolveParams(ctx);
    const route = params.route || [];
    if (route[0] === "add") {
      return await controller.handleEnqueue(request);
    }
    if (route[0] === "cron" && route[1] === "execute") {
      return await controller.handleCronExecute(request);
    }
    if (route[0] === "events" && route[1] === "publish") {
      return await controller.handlePublishEvent(request);
    }
    return new Response(JSON.stringify({ error: "Endpoint not found" }), {
      status: 404,
      headers: { "Content-Type": "application/json" }
    });
  };
  const DELETE = async (request, ctx) => {
    const params = await resolveParams(ctx);
    const route = params.route || [];
    if (route[0] === "cancel" && route[1]) {
      const url = new URL(request.url);
      const tenantId = url.searchParams.get("tenant_id") || url.searchParams.get("tenantId") || void 0;
      return await controller.handleCancel(route[1], tenantId);
    }
    return new Response(JSON.stringify({ error: "Endpoint not found" }), {
      status: 404,
      headers: { "Content-Type": "application/json" }
    });
  };
  return { GET, POST, DELETE };
}

export { createTaskEngineNextHandler };
//# sourceMappingURL=index.mjs.map
//# sourceMappingURL=index.mjs.map