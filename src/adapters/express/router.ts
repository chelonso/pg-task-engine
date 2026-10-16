import type { TaskEngine } from '../../core/engine.js';

export function createTaskEngineExpress(engine: TaskEngine, expressRouter?: any) {
  let router = expressRouter;
  if (!router) {
    try {
      // @ts-ignore
      const express = require('express');
      router = express.Router();
    } catch {
      throw new Error(
        'ExpressAdapter: Express is not installed. Please install express or pass an existing express.Router() instance.'
      );
    }
  }

  const controller = engine.http;

  const toWebRequest = (req: any): Request => {
    const protocol = req.protocol || 'http';
    const host = req.get('host') || 'localhost';
    const url = `${protocol}://${host}${req.originalUrl || req.url}`;

    const headers = new Headers();
    for (const [key, val] of Object.entries(req.headers)) {
      if (val !== undefined) {
        if (Array.isArray(val)) {
          val.forEach((v) => headers.append(key, v));
        } else {
          headers.set(key, String(val));
        }
      }
    }

    const hasBody = req.method !== 'GET' && req.method !== 'HEAD' && req.body;
    const body = hasBody
      ? typeof req.body === 'string' || Buffer.isBuffer(req.body)
        ? req.body
        : JSON.stringify(req.body)
      : undefined;

    return new Request(url, {
      method: req.method,
      headers,
      body
    });
  };

  const sendWebResponse = async (res: any, webRes: Response) => {
    res.status(webRes.status);
    webRes.headers.forEach((value, key) => {
      res.setHeader(key, value);
    });

    const data = await webRes.text();
    res.send(data);
  };

  router.post('/add', async (req: any, res: any, next: any) => {
    try {
      const webReq = toWebRequest(req);
      const webRes = await controller.handleEnqueue(webReq);
      await sendWebResponse(res, webRes);
    } catch (err) {
      next(err);
    }
  });

  router.get('/status/:id', async (req: any, res: any, next: any) => {
    try {
      const webRes = await controller.handleGetStatus(req.params.id);
      await sendWebResponse(res, webRes);
    } catch (err) {
      next(err);
    }
  });

  router.delete('/cancel/:id', async (req: any, res: any, next: any) => {
    try {
      const tenantId = req.query?.tenant_id || req.query?.tenantId || req.user?.tenant_id;
      const webRes = await controller.handleCancel(req.params.id, tenantId);
      await sendWebResponse(res, webRes);
    } catch (err) {
      next(err);
    }
  });

  router.post('/cron/execute', async (req: any, res: any, next: any) => {
    try {
      const webReq = toWebRequest(req);
      const webRes = await controller.handleCronExecute(webReq);
      await sendWebResponse(res, webRes);
    } catch (err) {
      next(err);
    }
  });

  router.post('/events/publish', async (req: any, res: any, next: any) => {
    try {
      const webReq = toWebRequest(req);
      const webRes = await controller.handlePublishEvent(webReq);
      await sendWebResponse(res, webRes);
    } catch (err) {
      next(err);
    }
  });

  return router;
}
