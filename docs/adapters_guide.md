# HTTP Framework Adapters Guide: `@shared/pg-task-engine`

This guide demonstrates how to mount the task engine into **Fastify**, **Hono**, **Express**, and **Next.js App Router**.

---

## 1. Fastify Plugin

The Fastify adapter provides a native plugin that exposes `/add`, `/status/:id`, `/cancel/:id`, `/cron/execute`, and `/events/publish`. It also connects worker startup and graceful shutdown with Fastify's lifecycle hooks.

```typescript
import Fastify from 'fastify';
import { TaskEngine } from '@shared/pg-task-engine';
import { taskEngineFastifyPlugin } from '@shared/pg-task-engine/fastify';
import { createSupabaseAdapter } from '@shared/pg-task-engine/db';
import { supabaseAdmin } from './supabaseClient';

const fastify = Fastify({ logger: true });

const engine = new TaskEngine({
  db: createSupabaseAdapter(supabaseAdmin),
  concurrency: 5,
  pollIntervalMs: 3000
});

// Register plugin with route prefix
await fastify.register(taskEngineFastifyPlugin, {
  prefix: '/api/tasks',
  engine,
  manageWorkerLifecycle: true // Automatically starts on fastify.ready() and stops on onClose()
});

await fastify.listen({ port: 3000, host: '0.0.0.0' });
```

---

## 2. Hono Sub-App

Because Hono is built natively on standard Web Fetch APIs (`Request` / `Response`), routing delegates with zero conversion overhead:

```typescript
import { Hono } from 'hono';
import { TaskEngine } from '@shared/pg-task-engine';
import { createTaskEngineHono } from '@shared/pg-task-engine/hono';
import { createDrizzleAdapter } from '@shared/pg-task-engine/db';
import { drizzleDb } from './db';

const app = new Hono();

const engine = new TaskEngine({
  db: createDrizzleAdapter(drizzleDb),
  concurrency: 3
});

// Start background worker
engine.start();

// Mount task engine routes under /tasks
const taskRoutes = createTaskEngineHono(engine, Hono);
app.route('/tasks', taskRoutes);

export default app;
```

---

## 3. Express Router

The Express adapter maps Node.js `IncomingMessage` streams and headers to standard `Request` and `Response`:

```typescript
import express from 'express';
import { TaskEngine } from '@shared/pg-task-engine';
import { createTaskEngineExpress } from '@shared/pg-task-engine/express';
import { createPrismaAdapter } from '@shared/pg-task-engine/db';
import { PrismaClient } from '@prisma/client';

const app = express();
app.use(express.json());

const prisma = new PrismaClient();
const engine = new TaskEngine({
  db: createPrismaAdapter(prisma),
  concurrency: 2
});

engine.start();

// Mount task routes
app.use('/api/tasks', createTaskEngineExpress(engine));

app.listen(3000, () => {
  console.log('Server running on port 3000');
});
```

---

## 4. Next.js App Router (Route Handlers)

Create a single catch-all route handler inside `app/api/tasks/[...route]/route.ts`:

```typescript
// app/api/tasks/[...route]/route.ts
import { TaskEngine } from '@shared/pg-task-engine';
import { createTaskEngineNextHandler } from '@shared/pg-task-engine/next';
import { createPgAdapter } from '@shared/pg-task-engine/db';
import { Pool } from 'pg';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const engine = new TaskEngine({
  db: createPgAdapter(pool)
});

// Export GET, POST, DELETE handlers for Next.js App Router
export const { GET, POST, DELETE } = createTaskEngineNextHandler(engine);
```
