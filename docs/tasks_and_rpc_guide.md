# Tasks & RPC Guide: `@shared/pg-task-engine`

This guide explains how to write type-safe TypeScript tasks and how to dispatch direct PostgreSQL stored procedures.

---

## 1. Defining Type-Safe Tasks with Zod (`defineTask`)

The `defineTask` helper enables schema validation and configurable execution deadlines.

```typescript
import { z } from 'zod';
import { defineTask } from '@shared/pg-task-engine';

// 1. Define payload schema
const SendWelcomePayloadSchema = z.object({
  userId: z.string().uuid(),
  userEmail: z.string().email(),
  channel: z.enum(['email', 'whatsapp']).default('email')
});

// 2. Infer TypeScript payload type
type SendWelcomePayload = z.infer<typeof SendWelcomePayloadSchema>;

// 3. Define task handler
export const sendWelcomeTask = defineTask<SendWelcomePayload, { sent: boolean }>({
  name: 'send-welcome-notification',
  schema: SendWelcomePayloadSchema,
  timeoutMs: 15000, // 15 seconds deadline

  handler: async (payload, ctx) => {
    ctx.logger.info(`Sending welcome notification to ${payload.userEmail}`, {
      attempt: ctx.attempt,
      maxAttempts: ctx.maxAttempts
    });

    // Use injected database or ORM client
    // ctx.db contains the raw client (SupabaseClient, Prisma, Drizzle, etc.)
    const userProfile = await ctx.db
      .from('users_profiles')
      .select('first_name')
      .eq('user_id', payload.userId)
      .single();

    // Use ctx.signal for fetch / HTTP calls to abort automatically on timeout
    const res = await fetch('https://api.notifications.internal/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email: payload.userEmail,
        name: userProfile.data?.first_name
      }),
      signal: ctx.signal
    });

    if (!res.ok) {
      throw new Error(`Notification provider failed with HTTP ${res.status}`);
    }

    return { sent: true };
  }
});
```

### Schema Validation & Fail-Fast Behavior
If a task receives an invalid payload (e.g. invalid UUID or missing required fields):
- `defineTask` throws `TaskValidationError`.
- The worker immediately records the validation error in `common.queued_job_logs`.
- The job is **not retried**, saving compute resources on corrupted payloads.

---

## 2. Using `TaskContext`

Every handler receives a rich execution context:

| Property | Type | Description |
|---|---|---|
| `ctx.tenantId` | `string` (UUID) | Tenant identifier for multi-tenant row-level security |
| `ctx.jobId` | `string` (UUID) | Current running job ID |
| `ctx.attempt` | `number` | Current attempt counter (1, 2, 3...) |
| `ctx.maxAttempts` | `number` | Configured maximum retry attempts |
| `ctx.db` | `TDb` | The raw injected database / ORM client |
| `ctx.signal` | `AbortSignal` | Cancellation token (triggered on worker shutdown or timeout) |
| `ctx.logger` | `TaskLogger` | Scoped logger automatically prepending `[jobId][tenantId]` |

---

## 3. Direct SQL Stored Procedures (`rpc:`)

When a task requires heavy data transformations, bulk updates, or attendance reconciliation, executing inside PostgreSQL is far more efficient than transporting rows across the network into Node.js.

Any task whose `target_path` starts with `rpc:` bypasses Node.js handler resolution and invokes the PostgreSQL function directly:

### 1. SQL Function Definition in PostgreSQL
```sql
CREATE OR REPLACE FUNCTION common.fn_reconcile_attendance(
    p_tenant_id UUID,
    p_payload JSONB
)
RETURNS JSONB AS $$
DECLARE
    v_updated_count INT;
BEGIN
    -- Business logic inside Postgres
    UPDATE gistar.attendances
    SET status = 'reconciled'
    WHERE tenant_id = p_tenant_id
      AND status = 'pending';

    GET DIAGNOSTICS v_updated_count = ROW_COUNT;

    RETURN jsonb_build_object('reconciled', v_updated_count);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

### 2. Enqueueing from TypeScript
```typescript
await engine.queue.enqueue({
  tenantId: 'd1a2f3e4-5b6c-7d8e-9f0a-1b2c3d4e5f6a',
  targetPath: 'rpc:fn_reconcile_attendance',
  payload: { date: '2026-10-16' }
});
```

### 3. Worker Execution
The worker invokes:
```typescript
const { data, error } = await db.rpc('fn_reconcile_attendance', {
  p_tenant_id: job.tenant_id,
  p_payload: job.payload
});
```
The result returned by PostgreSQL is saved directly into `common.queued_job_logs.response_payload`.
