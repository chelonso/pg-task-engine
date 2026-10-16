# Architecture Overview: `@shared/pg-task-engine`

`@shared/pg-task-engine` is a high-performance, decoupled asynchronous job queue, cron scheduler, and domain events engine built on top of **PostgreSQL** and TypeScript.

---

## 🏗️ Core Principles

1. **PostgreSQL as the Message Broker**:
   - Zero external broker dependencies (no Redis, BullMQ, RabbitMQ, or SQS to provision, pay for, and maintain).
   - Atomic multi-tenant ACID consistency: job enqueuing and database business transactions run in the same commit.
   - Non-blocking concurrent worker polling powered by PostgreSQL `FOR UPDATE SKIP LOCKED`.

2. **Web Standards First**:
   - The entire HTTP controller layer uses native `Request` and `Response` Web Fetch APIs.
   - Decoupled from runtime environments: runs identically in Node.js, Fastify, Express, Hono, Next.js App Router, or Edge environments.

3. **Inversion of Control & Injected Database Adapters**:
   - The engine does not dictate database client libraries.
   - Connectors for **Supabase**, **Prisma**, **Drizzle**, and raw **PostgreSQL (`pg` / `postgres.js`)** normalize queries while exposing the raw client in `TaskContext.db`.

4. **Hybrid Execution Model**:
   - **TypeScript Tasks**: Handlers defined with `defineTask`, validated by Zod schemas, and protected by cancellation tokens (`AbortSignal`).
   - **Direct Database RPCs**: Tasks prefixed with `rpc:` (e.g., `rpc:fn_reconcile_attendance`) execute directly inside the database via `db.rpc(...)`, bypassing Node.js data transfer overhead.

---

## 🔄 End-to-End Execution Flow

```mermaid
sequenceDiagram
    autonumber
    participant Client as 👤 Client / UI / Service
    participant Adapter as 🔌 HTTP Adapter (Fastify / Hono / Express / Next)
    participant Controller as 🎮 TaskHttpController
    participant Queue as 📥 TaskQueue
    participant DB_Broker as 🗄️ common.queued_jobs
    participant Worker as ⚙️ TaskWorker Loop
    participant Task as 📜 Task Definition / RPC
    participant DB_Logs as 📊 common.queued_job_logs

    Client->>Adapter: POST /add { tenantId, targetPath, payload }
    Adapter->>Controller: handleEnqueue(webRequest)
    Controller->>Queue: enqueue(options)
    Queue->>DB_Broker: INSERT INTO common.queued_jobs (status='pending')
    DB_Broker-->>Controller: Job ID (UUID)
    Controller-->>Client: 202 Accepted { success: true, jobId, message: 'Enqueued' }

    loop Worker Polling Loop (0ms on hit / 5000ms idle)
        Worker->>DB_Broker: common.fn_poll_next_job() [FOR UPDATE SKIP LOCKED]
        DB_Broker-->>Worker: Atomically locked job (status='processing')
        
        alt Target path is 'rpc:procedure_name'
            Worker->>DB_Broker: Execute db.rpc(procedure_name, { p_tenant_id, p_payload })
        else Registered TypeScript Task
            Worker->>Task: taskDef.run(payload, context)
            Note over Task: Zod validation + AbortSignal timeout
        end

        alt Execution Success
            Worker->>DB_Broker: DELETE FROM common.queued_jobs WHERE id = jobId
            Worker->>DB_Logs: INSERT INTO common.queued_job_logs (status='success')
        else Execution Failed (attempts < maxAttempts)
            Worker->>DB_Broker: UPDATE queued_jobs SET status='pending', attempts++, run_at=now()+backoff
            Worker->>DB_Logs: INSERT INTO common.queued_job_logs (status='failed')
        else Execution Failed (exhausted retries)
            Worker->>DB_Broker: DELETE FROM common.queued_jobs WHERE id = jobId
            Worker->>DB_Logs: INSERT INTO common.queued_job_logs (status='failed', final)
        end
    end
```

---

## 📦 Domain Events Outbox Architecture

```mermaid
flowchart LR
    A["Application Service / Mutation"] -->|Publish Event| B[("common.domain_events<br/>(Transactional Outbox)")]
    B -->|Trigger / RPC| C["common.fn_process_domain_event()"]
    C -->|Query Subscriptions| D[("common.event_subscriptions")]
    D -->|Fan-Out Enqueue| E[("common.queued_jobs")]
    E -->|Worker Pick-up| F["Asynchronous Task Handlers"]
```

1. Business transactions write events into `common.domain_events` within the same database commit.
2. `common.fn_process_domain_event` inspects `common.event_subscriptions` and fans out the event into isolated `common.queued_jobs` entries per matching target task.
3. Workers pick up each task independently, maintaining failure isolation across event consumers.
