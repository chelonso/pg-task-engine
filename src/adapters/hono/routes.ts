import type { TaskEngine } from '../../core/engine.js';

export interface HonoAdapterOptions {
  engine: TaskEngine;
}

/**
 * Creates a Hono sub-application router mounted with task engine endpoints.
 */
export function createTaskEngineHono(engine: TaskEngine, HonoClass?: any) {
  // If Hono class is not passed, attempt dynamic require or expect global Hono
  const Hono = HonoClass || (globalThis as any).Hono;
  if (!Hono) {
    try {
      // @ts-ignore
      const honoPkg = require('hono');
      const App = honoPkg.Hono;
      return buildHonoRoutes(new App(), engine);
    } catch {
      throw new Error(
        'HonoAdapter: Hono is not installed. Please install hono (`npm i hono`) or pass Hono class to createTaskEngineHono(engine, Hono).'
      );
    }
  }

  return buildHonoRoutes(new Hono(), engine);
}

export function buildHonoRoutes(app: any, engine: TaskEngine) {
  const controller = engine.http;

  app.post('/add', async (c: any) => {
    return await controller.handleEnqueue(c.req.raw);
  });

  app.get('/status/:id', async (c: any) => {
    const id = c.req.param('id');
    return await controller.handleGetStatus(id);
  });

  app.delete('/cancel/:id', async (c: any) => {
    const id = c.req.param('id');
    const tenantId = c.req.query('tenant_id') || c.req.query('tenantId');
    return await controller.handleCancel(id, tenantId);
  });

  app.post('/cron/execute', async (c: any) => {
    return await controller.handleCronExecute(c.req.raw);
  });

  app.post('/events/publish', async (c: any) => {
    return await controller.handlePublishEvent(c.req.raw);
  });

  return app;
}
