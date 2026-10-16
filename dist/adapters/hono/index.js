'use strict';

var __require = /* @__PURE__ */ ((x) => typeof require !== "undefined" ? require : typeof Proxy !== "undefined" ? new Proxy(x, {
  get: (a, b) => (typeof require !== "undefined" ? require : a)[b]
}) : x)(function(x) {
  if (typeof require !== "undefined") return require.apply(this, arguments);
  throw Error('Dynamic require of "' + x + '" is not supported');
});

// src/adapters/hono/routes.ts
function createTaskEngineHono(engine, HonoClass) {
  const Hono = HonoClass || globalThis.Hono;
  if (!Hono) {
    try {
      const honoPkg = __require("hono");
      const App = honoPkg.Hono;
      return buildHonoRoutes(new App(), engine);
    } catch {
      throw new Error(
        "HonoAdapter: Hono is not installed. Please install hono (`npm i hono`) or pass Hono class to createTaskEngineHono(engine, Hono)."
      );
    }
  }
  return buildHonoRoutes(new Hono(), engine);
}
function buildHonoRoutes(app, engine) {
  const controller = engine.http;
  app.post("/add", async (c) => {
    return await controller.handleEnqueue(c.req.raw);
  });
  app.get("/status/:id", async (c) => {
    const id = c.req.param("id");
    return await controller.handleGetStatus(id);
  });
  app.delete("/cancel/:id", async (c) => {
    const id = c.req.param("id");
    const tenantId = c.req.query("tenant_id") || c.req.query("tenantId");
    return await controller.handleCancel(id, tenantId);
  });
  app.post("/cron/execute", async (c) => {
    return await controller.handleCronExecute(c.req.raw);
  });
  app.post("/events/publish", async (c) => {
    return await controller.handlePublishEvent(c.req.raw);
  });
  return app;
}

exports.buildHonoRoutes = buildHonoRoutes;
exports.createTaskEngineHono = createTaskEngineHono;
//# sourceMappingURL=index.js.map
//# sourceMappingURL=index.js.map