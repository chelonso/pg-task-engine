# `@shared/pg-task-engine`

> **High-performance, decoupled asynchronous job queue, cron scheduler, and domain events engine for PostgreSQL and TypeScript.**

Zero external message brokers required (No Redis, BullMQ, or RabbitMQ). Native production adapters for **Fastify**, **Hono**, **Express**, and **Next.js App Router**, with support for **Supabase**, **Prisma**, **Drizzle**, and raw **PostgreSQL (`pg` / `postgres.js`)**.

[![TypeScript](https://img.shields.io/badge/TypeScript-5.5+-blue.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

---

## ⚡ Features

- 🐘 **PostgreSQL as Broker**: Non-blocking atomic worker polling using `FOR UPDATE SKIP LOCKED`.
- 🛡️ **Zero Redis / SQS Overhead**: Store, audit, and process background tasks inside your existing database with full ACID transactions.
- 📐 **Web Standards First**: Core controller built on native `Request` and `Response` Web Fetch APIs.
- 🔌 **Framework Adapters**: Out-of-the-box native adapters for **Fastify**, **Hono**, **Express**, and **Next.js App Router**.
- 🗄️ **Multi-ORM Database Connectors**: First-class adapters for **Supabase**, **Prisma**, **Drizzle**, and **`pg` / `postgres.js`**.
- 🔒 **Type-Safe Tasks (`defineTask`)**: Optional runtime payload validation using **Zod** with automatic fail-fast error handling.
- ⏱️ **Resilient Worker**: Exponential backoff retries ($2^{\text{attempts}} \times 5$s), cancellation tokens via `AbortSignal`, configurable timeouts, concurrency controls, and graceful shutdown on `SIGTERM`/`SIGINT`.
- ⚡ **Hybrid Execution (JS + SQL RPC)**: Run TypeScript task handlers or invoke direct PostgreSQL functions with `rpc:function_name`.
- 📬 **Domain Events Outbox**: Transactional event publishing and subscriber routing into background queues.

---

## 📦 Installation

```bash
npm install @shared/pg-task-engine zod
```

### Optional Peer Dependencies (Install what you use)
```bash
# For Fastify
npm install fastify

# For Hono
npm install hono

# For Express
npm install express @types/express

# For Supabase
npm install @supabase/supabase-js
```

---

## 🚀 Quick Start

### 1. Initialize Database Schema
Run the provided SQL migration scripts against your PostgreSQL database:

```bash
psql -d my_database -f sql/01_schema_common.sql
psql -d my_database -f sql/02_functions_procedures.sql
```

### 2. Define a Task
```typescript
// tasks/sendWelcome.ts
import { z } from 'zod';
import { defineTask } from '@shared/pg-task-engine';

export const sendWelcomeTask = defineTask({
  name: 'send-welcome-email',
  schema: z.object({
    userId: z.string().uuid(),
    email: z.string().email()
  }),
  timeoutMs: 10000,
  handler: async (payload, ctx) => {
    ctx.logger.info(`Sending email to ${payload.email}`);
    // Use ctx.db to query database
    // Use ctx.signal for cancellable HTTP fetch
    return { sent: true };
  }
});
```

### 3. Instantiate Engine & Start Worker
```typescript
// server.ts
import { TaskEngine } from '@shared/pg-task-engine';
import { createSupabaseAdapter } from '@shared/pg-task-engine/db';
import { supabaseAdmin } from './supabaseClient';
import { sendWelcomeTask } from './tasks/sendWelcome';

export const engine = new TaskEngine({
  db: createSupabaseAdapter(supabaseAdmin),
  concurrency: 3,
  pollIntervalMs: 5000,
  tasks: [sendWelcomeTask]
});

// Start background worker loop
engine.start();

// Enqueue a job programmatically
await engine.queue.enqueue({
  tenantId: 'd1a2f3e4-5b6c-7d8e-9f0a-1b2c3d4e5f6a',
  targetPath: 'send-welcome-email',
  payload: { userId: '11111111-2222-3333-4444-555555555555', email: 'user@example.com' }
});
```

---

## 🔌 Framework Integration

### Fastify Plugin
```typescript
import Fastify from 'fastify';
import { taskEngineFastifyPlugin } from '@shared/pg-task-engine/fastify';

const fastify = Fastify();
await fastify.register(taskEngineFastifyPlugin, {
  prefix: '/api/tasks',
  engine,
  manageWorkerLifecycle: true
});
```

### Hono Router
```typescript
import { Hono } from 'hono';
import { createTaskEngineHono } from '@shared/pg-task-engine/hono';

const app = new Hono();
app.route('/api/tasks', createTaskEngineHono(engine, Hono));
```

### Express Router
```typescript
import express from 'express';
import { createTaskEngineExpress } from '@shared/pg-task-engine/express';

const app = express();
app.use(express.json());
app.use('/api/tasks', createTaskEngineExpress(engine));
```

### Next.js App Router (`app/api/tasks/[...route]/route.ts`)
```typescript
import { createTaskEngineNextHandler } from '@shared/pg-task-engine/next';
import { engine } from '@/lib/taskEngine';

export const { GET, POST, DELETE } = createTaskEngineNextHandler(engine);
```

---

## 🗄️ Database Adapters

| Adapter | Import | Usage |
|---|---|---|
| **Supabase** | `@shared/pg-task-engine/db` | `createSupabaseAdapter(supabaseClient)` |
| **Prisma** | `@shared/pg-task-engine/db` | `createPrismaAdapter(prismaClient)` |
| **Drizzle** | `@shared/pg-task-engine/db` | `createDrizzleAdapter(drizzleDb)` |
| **PostgreSQL (`pg` / `postgres.js`)** | `@shared/pg-task-engine/db` | `createPgAdapter(poolOrClient)` |

---

## 📚 Documentation

- [Architecture Overview](docs/architecture.md)
- [HTTP API Reference](docs/api_reference.md)
- [Framework Adapters Guide](docs/adapters_guide.md)
- [Tasks & Direct RPC Guide](docs/tasks_and_rpc_guide.md)

---

## 📄 License

MIT © GiStar SaaS Architecture Team
