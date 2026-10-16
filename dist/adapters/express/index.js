'use strict';

var __require = /* @__PURE__ */ ((x) => typeof require !== "undefined" ? require : typeof Proxy !== "undefined" ? new Proxy(x, {
  get: (a, b) => (typeof require !== "undefined" ? require : a)[b]
}) : x)(function(x) {
  if (typeof require !== "undefined") return require.apply(this, arguments);
  throw Error('Dynamic require of "' + x + '" is not supported');
});

// src/adapters/express/router.ts
function createTaskEngineExpress(engine, expressRouter) {
  let router = expressRouter;
  if (!router) {
    try {
      const express = __require("express");
      router = express.Router();
    } catch {
      throw new Error(
        "ExpressAdapter: Express is not installed. Please install express or pass an existing express.Router() instance."
      );
    }
  }
  const controller = engine.http;
  const toWebRequest = (req) => {
    const protocol = req.protocol || "http";
    const host = req.get("host") || "localhost";
    const url = `${protocol}://${host}${req.originalUrl || req.url}`;
    const headers = new Headers();
    for (const [key, val] of Object.entries(req.headers)) {
      if (val !== void 0) {
        if (Array.isArray(val)) {
          val.forEach((v) => headers.append(key, v));
        } else {
          headers.set(key, String(val));
        }
      }
    }
    const hasBody = req.method !== "GET" && req.method !== "HEAD" && req.body;
    const body = hasBody ? typeof req.body === "string" || Buffer.isBuffer(req.body) ? req.body : JSON.stringify(req.body) : void 0;
    return new Request(url, {
      method: req.method,
      headers,
      body
    });
  };
  const sendWebResponse = async (res, webRes) => {
    res.status(webRes.status);
    webRes.headers.forEach((value, key) => {
      res.setHeader(key, value);
    });
    const data = await webRes.text();
    res.send(data);
  };
  router.post("/add", async (req, res, next) => {
    try {
      const webReq = toWebRequest(req);
      const webRes = await controller.handleEnqueue(webReq);
      await sendWebResponse(res, webRes);
    } catch (err) {
      next(err);
    }
  });
  router.get("/status/:id", async (req, res, next) => {
    try {
      const webRes = await controller.handleGetStatus(req.params.id);
      await sendWebResponse(res, webRes);
    } catch (err) {
      next(err);
    }
  });
  router.delete("/cancel/:id", async (req, res, next) => {
    try {
      const tenantId = req.query?.tenant_id || req.query?.tenantId || req.user?.tenant_id;
      const webRes = await controller.handleCancel(req.params.id, tenantId);
      await sendWebResponse(res, webRes);
    } catch (err) {
      next(err);
    }
  });
  router.post("/cron/execute", async (req, res, next) => {
    try {
      const webReq = toWebRequest(req);
      const webRes = await controller.handleCronExecute(webReq);
      await sendWebResponse(res, webRes);
    } catch (err) {
      next(err);
    }
  });
  router.post("/events/publish", async (req, res, next) => {
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

exports.createTaskEngineExpress = createTaskEngineExpress;
//# sourceMappingURL=index.js.map
//# sourceMappingURL=index.js.map