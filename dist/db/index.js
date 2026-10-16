'use strict';

// src/db/adapter.ts
function resolveDatabaseAdapter(db) {
  if (!db || typeof db !== "object") {
    throw new Error("TaskEngine: Invalid database client provided.");
  }
  if ("rpc" in db && typeof db.rpc === "function") {
    return db;
  }
  throw new Error(
    "TaskEngine: Provided db instance does not implement DatabaseAdapter. Use createSupabaseAdapter, createPrismaAdapter, createDrizzleAdapter, or createPgAdapter."
  );
}

// src/db/supabase.ts
function createSupabaseAdapter(client, options) {
  const schema = options?.schema || "common";
  const getTable = (table) => {
    return client.schema ? client.schema(schema).from(table) : client.from(table);
  };
  return {
    raw: client,
    async rpc(functionName, args) {
      const res = await client.rpc(functionName, args);
      if (res.error) {
        const err = res.error instanceof Error ? res.error : new Error(String(res.error?.message || res.error));
        return { data: null, error: err };
      }
      return { data: res.data, error: null };
    },
    async enqueueJob(opts) {
      const { data, error } = await this.rpc("fn_enqueue_job", {
        p_tenant_id: opts.tenantId,
        p_target_path: opts.targetPath,
        p_payload: opts.payload ?? {},
        p_run_at: opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString(),
        p_max_attempts: opts.maxAttempts ?? 3
      });
      if (error || !data) {
        const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString();
        const res = await getTable("queued_jobs").insert({
          tenant_id: opts.tenantId,
          target_path: opts.targetPath,
          payload: opts.payload ?? {},
          run_at: runAt,
          max_attempts: opts.maxAttempts ?? 3,
          status: "pending"
        }).select("id").single();
        if (res.error) throw new Error(`Failed to enqueue job: ${res.error.message}`);
        return { id: res.data.id };
      }
      return { id: data };
    },
    async removeJob(jobId) {
      const res = await getTable("queued_jobs").delete().eq("id", jobId);
      if (res.error) {
        throw new Error(`Failed to remove job ${jobId}: ${res.error.message}`);
      }
    },
    async rescheduleJob(opts) {
      const res = await getTable("queued_jobs").update({
        status: "pending",
        attempts: opts.attempts,
        run_at: new Date(opts.runAt).toISOString(),
        updated_at: (/* @__PURE__ */ new Date()).toISOString()
      }).eq("id", opts.jobId);
      if (res.error) {
        throw new Error(`Failed to reschedule job ${opts.jobId}: ${res.error.message}`);
      }
    },
    async recordJobLog(log) {
      const res = await getTable("queued_job_logs").insert({
        job_id: log.jobId,
        tenant_id: log.tenantId,
        target_path: log.targetPath,
        status: log.status,
        started_at: log.startedAt,
        finished_at: log.finishedAt,
        duration_ms: log.durationMs,
        response_payload: log.responsePayload ?? null,
        error_message: log.errorMessage ?? null,
        attempts: log.attempts
      });
      if (res.error) {
        throw new Error(`Failed to record queue log: ${res.error.message}`);
      }
    },
    async getJob(jobId) {
      const res = await getTable("queued_jobs").select("*").eq("id", jobId).maybeSingle();
      if (res.error) throw new Error(`Failed to fetch job ${jobId}: ${res.error.message}`);
      return res.data || null;
    },
    async cancelJob(jobId, tenantId) {
      let query = getTable("queued_jobs").delete().eq("id", jobId).eq("status", "pending");
      if (tenantId) {
        query = query.eq("tenant_id", tenantId);
      }
      const res = await query.select("id");
      if (res.error) throw new Error(`Failed to cancel job: ${res.error.message}`);
      return res.data && res.data.length > 0 || false;
    },
    async recordScheduledLog(log) {
      if (log.id) {
        const res2 = await getTable("scheduled_job_logs").update({
          status: log.status,
          finished_at: log.finishedAt ?? (/* @__PURE__ */ new Date()).toISOString(),
          duration_ms: log.durationMs ?? null,
          response_payload: log.responsePayload ?? null,
          error_message: log.errorMessage ?? null
        }).eq("id", log.id).select("id").single();
        if (res2.error) throw new Error(`Failed to update scheduled log: ${res2.error.message}`);
        await getTable("scheduled_jobs").update({
          status: log.status,
          last_run_at: log.finishedAt ?? (/* @__PURE__ */ new Date()).toISOString()
        }).eq("id", log.scheduledJobId);
        return { id: log.id };
      }
      const res = await getTable("scheduled_job_logs").insert({
        job_id: log.scheduledJobId,
        tenant_id: log.tenantId,
        status: log.status,
        triggered_by: log.triggeredBy,
        started_at: log.startedAt,
        finished_at: log.finishedAt ?? null,
        duration_ms: log.durationMs ?? null,
        response_payload: log.responsePayload ?? null,
        error_message: log.errorMessage ?? null
      }).select("id").single();
      if (res.error) throw new Error(`Failed to record scheduled log: ${res.error.message}`);
      return { id: res.data.id };
    }
  };
}
function createSupabaseRestAdapter(options) {
  const { url, serviceKey, schema = "common" } = options;
  const baseUrl = url.replace(/\/+$/, "");
  const defaultHeaders = {
    apikey: serviceKey,
    Authorization: `Bearer ${serviceKey}`,
    "Content-Type": "application/json"
  };
  const rawClient = {
    url: baseUrl,
    serviceKey,
    async query(path, opts = {}) {
      const targetSchema = opts.schema || "gistar";
      const cleanPath = path.replace(/^\/+/, "");
      const res = await fetch(`${baseUrl}/rest/v1/${cleanPath}`, {
        ...opts,
        headers: {
          ...defaultHeaders,
          "Accept-Profile": targetSchema,
          "Content-Profile": targetSchema,
          ...opts.headers
        }
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`DB Query Error: ${res.status} - ${txt}`);
      }
      return res.json();
    },
    async rpc(fn, args, opts = {}) {
      const targetSchema = opts.schema || schema;
      try {
        const res = await fetch(`${baseUrl}/rest/v1/rpc/${fn}`, {
          method: "POST",
          headers: {
            ...defaultHeaders,
            "Accept-Profile": targetSchema,
            "Content-Profile": targetSchema
          },
          body: JSON.stringify(args ?? {})
        });
        if (!res.ok) {
          const txt = await res.text();
          return { data: null, error: new Error(`RPC Error (${res.status}): ${txt}`) };
        }
        const data = await res.json();
        return { data, error: null };
      } catch (err) {
        return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
      }
    }
  };
  return {
    raw: rawClient,
    async rpc(functionName, args) {
      return rawClient.rpc(functionName, args, { schema });
    },
    async enqueueJob(opts) {
      const { data, error } = await this.rpc("fn_enqueue_job", {
        p_tenant_id: opts.tenantId,
        p_target_path: opts.targetPath,
        p_payload: opts.payload ?? {},
        p_run_at: opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString(),
        p_max_attempts: opts.maxAttempts ?? 3
      });
      if (!error && data) {
        return { id: typeof data === "object" && data?.id ? data.id : String(data) };
      }
      const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString();
      const res = await fetch(`${baseUrl}/rest/v1/queued_jobs`, {
        method: "POST",
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema,
          "Content-Profile": schema,
          Prefer: "return=representation"
        },
        body: JSON.stringify({
          tenant_id: opts.tenantId,
          target_path: opts.targetPath,
          payload: opts.payload ?? {},
          run_at: runAt,
          max_attempts: opts.maxAttempts ?? 3,
          status: "pending"
        })
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to enqueue job: ${res.status} - ${txt}`);
      }
      const rows = await res.json();
      return { id: rows[0].id };
    },
    async removeJob(jobId) {
      const res = await fetch(`${baseUrl}/rest/v1/queued_jobs?id=eq.${jobId}`, {
        method: "DELETE",
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema,
          "Content-Profile": schema
        }
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to remove job ${jobId}: ${res.status} - ${txt}`);
      }
    },
    async rescheduleJob(opts) {
      const res = await fetch(`${baseUrl}/rest/v1/queued_jobs?id=eq.${opts.jobId}`, {
        method: "PATCH",
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema,
          "Content-Profile": schema
        },
        body: JSON.stringify({
          status: "pending",
          attempts: opts.attempts,
          run_at: new Date(opts.runAt).toISOString(),
          updated_at: (/* @__PURE__ */ new Date()).toISOString()
        })
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to reschedule job ${opts.jobId}: ${res.status} - ${txt}`);
      }
    },
    async recordJobLog(log) {
      const res = await fetch(`${baseUrl}/rest/v1/queued_job_logs`, {
        method: "POST",
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema,
          "Content-Profile": schema
        },
        body: JSON.stringify({
          job_id: log.jobId,
          tenant_id: log.tenantId,
          target_path: log.targetPath,
          status: log.status,
          started_at: log.startedAt,
          finished_at: log.finishedAt,
          duration_ms: log.durationMs,
          response_payload: log.responsePayload ?? null,
          error_message: log.errorMessage ?? null,
          attempts: log.attempts
        })
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to record queue log: ${res.status} - ${txt}`);
      }
    },
    async getJob(jobId) {
      const res = await fetch(`${baseUrl}/rest/v1/queued_jobs?id=eq.${jobId}&limit=1`, {
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema
        }
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to get job ${jobId}: ${res.status} - ${txt}`);
      }
      const data = await res.json();
      return data && data.length > 0 ? data[0] : null;
    },
    async cancelJob(jobId, tenantId) {
      let urlStr = `${baseUrl}/rest/v1/queued_jobs?id=eq.${jobId}&status=eq.pending`;
      if (tenantId) urlStr += `&tenant_id=eq.${tenantId}`;
      const res = await fetch(urlStr, {
        method: "DELETE",
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema,
          Prefer: "return=representation"
        }
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to cancel job: ${res.status} - ${txt}`);
      }
      const data = await res.json();
      return Array.isArray(data) && data.length > 0;
    },
    async recordScheduledLog(log) {
      if (log.id) {
        const logRes = await fetch(`${baseUrl}/rest/v1/scheduled_job_logs?id=eq.${log.id}`, {
          method: "PATCH",
          headers: {
            ...defaultHeaders,
            "Accept-Profile": schema,
            "Content-Profile": schema
          },
          body: JSON.stringify({
            status: log.status,
            finished_at: log.finishedAt ?? (/* @__PURE__ */ new Date()).toISOString(),
            duration_ms: log.durationMs ?? null,
            response_payload: log.responsePayload ?? null,
            error_message: log.errorMessage ?? null
          })
        });
        if (!logRes.ok) {
          const txt = await logRes.text();
          throw new Error(`Failed to update scheduled log: ${logRes.status} - ${txt}`);
        }
        await fetch(`${baseUrl}/rest/v1/scheduled_jobs?id=eq.${log.scheduledJobId}`, {
          method: "PATCH",
          headers: {
            ...defaultHeaders,
            "Accept-Profile": schema,
            "Content-Profile": schema
          },
          body: JSON.stringify({
            status: log.status,
            last_run_at: log.finishedAt ?? (/* @__PURE__ */ new Date()).toISOString()
          })
        }).catch(() => {
        });
        return { id: log.id };
      }
      const res = await fetch(`${baseUrl}/rest/v1/scheduled_job_logs`, {
        method: "POST",
        headers: {
          ...defaultHeaders,
          "Accept-Profile": schema,
          "Content-Profile": schema,
          Prefer: "return=representation"
        },
        body: JSON.stringify({
          job_id: log.scheduledJobId,
          tenant_id: log.tenantId,
          status: log.status,
          triggered_by: log.triggeredBy,
          started_at: log.startedAt,
          finished_at: log.finishedAt ?? null,
          duration_ms: log.durationMs ?? null,
          response_payload: log.responsePayload ?? null,
          error_message: log.errorMessage ?? null
        })
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Failed to record scheduled log: ${res.status} - ${txt}`);
      }
      const data = await res.json();
      return { id: data[0].id };
    }
  };
}

// src/db/prisma.ts
function createPrismaAdapter(prisma, options) {
  const schema = options?.schema || "common";
  return {
    raw: prisma,
    async rpc(functionName, args) {
      try {
        if (!args || Object.keys(args).length === 0) {
          const rows2 = await prisma.$queryRawUnsafe(
            `SELECT * FROM ${schema}.${functionName}()`
          );
          return { data: rows2 && rows2.length > 0 ? rows2 : null, error: null };
        }
        const keys = Object.keys(args);
        const placeholders = keys.map((_, i) => `$${i + 1}`).join(", ");
        const values = Object.values(args).map(
          (val) => typeof val === "object" && val !== null ? JSON.stringify(val) : val
        );
        const rows = await prisma.$queryRawUnsafe(
          `SELECT * FROM ${schema}.${functionName}(${placeholders})`,
          ...values
        );
        return { data: rows && rows.length > 0 ? rows : null, error: null };
      } catch (err) {
        return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
      }
    },
    async enqueueJob(opts) {
      const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString();
      const payloadJson = JSON.stringify(opts.payload ?? {});
      const rows = await prisma.$queryRawUnsafe(
        `INSERT INTO ${schema}.queued_jobs (tenant_id, target_path, payload, run_at, max_attempts, status)
         VALUES ($1::uuid, $2, $3::jsonb, $4::timestamptz, $5, 'pending')
         RETURNING id`,
        opts.tenantId,
        opts.targetPath,
        payloadJson,
        runAt,
        opts.maxAttempts ?? 3
      );
      if (!rows || rows.length === 0) {
        throw new Error("PrismaAdapter: Failed to enqueue job, no ID returned.");
      }
      return { id: rows[0].id };
    },
    async removeJob(jobId) {
      await prisma.$executeRawUnsafe(
        `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid`,
        jobId
      );
    },
    async rescheduleJob(opts) {
      await prisma.$executeRawUnsafe(
        `UPDATE ${schema}.queued_jobs
         SET status = 'pending', attempts = $1, run_at = $2::timestamptz, updated_at = NOW()
         WHERE id = $3::uuid`,
        opts.attempts,
        new Date(opts.runAt).toISOString(),
        opts.jobId
      );
    },
    async recordJobLog(log) {
      await prisma.$executeRawUnsafe(
        `INSERT INTO ${schema}.queued_job_logs 
         (job_id, tenant_id, target_path, status, started_at, finished_at, duration_ms, response_payload, error_message, attempts)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8::jsonb, $9, $10)`,
        log.jobId,
        log.tenantId,
        log.targetPath,
        log.status,
        log.startedAt,
        log.finishedAt,
        log.durationMs,
        log.responsePayload ? JSON.stringify(log.responsePayload) : null,
        log.errorMessage ?? null,
        log.attempts
      );
    },
    async getJob(jobId) {
      const rows = await prisma.$queryRawUnsafe(
        `SELECT * FROM ${schema}.queued_jobs WHERE id = $1::uuid LIMIT 1`,
        jobId
      );
      return rows && rows.length > 0 ? rows[0] : null;
    },
    async cancelJob(jobId, tenantId) {
      if (tenantId) {
        const count2 = await prisma.$executeRawUnsafe(
          `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid AND tenant_id = $2::uuid AND status = 'pending'`,
          jobId,
          tenantId
        );
        return count2 > 0;
      }
      const count = await prisma.$executeRawUnsafe(
        `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid AND status = 'pending'`,
        jobId
      );
      return count > 0;
    },
    async recordScheduledLog(log) {
      const rows = await prisma.$queryRawUnsafe(
        `INSERT INTO ${schema}.scheduled_job_logs
         (job_id, tenant_id, status, triggered_by, started_at, finished_at, duration_ms, response_payload, error_message)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8::jsonb, $9)
         RETURNING id`,
        log.scheduledJobId,
        log.tenantId,
        log.status,
        log.triggeredBy,
        log.startedAt,
        log.finishedAt ?? null,
        log.durationMs ?? null,
        log.responsePayload ? JSON.stringify(log.responsePayload) : null,
        log.errorMessage ?? null
      );
      return { id: rows[0].id };
    }
  };
}

// src/db/drizzle.ts
function createDrizzleAdapter(drizzleDb, options) {
  const schema = options?.schema || "common";
  const runSql = async (sqlQuery, params = []) => {
    const result = await drizzleDb.execute({
      sql: sqlQuery,
      params
    });
    if (Array.isArray(result)) return result;
    if (result && Array.isArray(result.rows)) return result.rows;
    return [];
  };
  return {
    raw: drizzleDb,
    async rpc(functionName, args) {
      try {
        if (!args || Object.keys(args).length === 0) {
          const rows2 = await runSql(`SELECT * FROM ${schema}.${functionName}()`);
          return { data: rows2 && rows2.length > 0 ? rows2 : null, error: null };
        }
        const keys = Object.keys(args);
        const placeholders = keys.map((_, i) => `$${i + 1}`).join(", ");
        const values = Object.values(args).map(
          (val) => typeof val === "object" && val !== null ? JSON.stringify(val) : val
        );
        const rows = await runSql(
          `SELECT * FROM ${schema}.${functionName}(${placeholders})`,
          values
        );
        return { data: rows && rows.length > 0 ? rows : null, error: null };
      } catch (err) {
        return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
      }
    },
    async enqueueJob(opts) {
      const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString();
      const payloadJson = JSON.stringify(opts.payload ?? {});
      const rows = await runSql(
        `INSERT INTO ${schema}.queued_jobs (tenant_id, target_path, payload, run_at, max_attempts, status)
         VALUES ($1::uuid, $2, $3::jsonb, $4::timestamptz, $5, 'pending')
         RETURNING id`,
        [opts.tenantId, opts.targetPath, payloadJson, runAt, opts.maxAttempts ?? 3]
      );
      if (!rows || rows.length === 0) throw new Error("DrizzleAdapter: Failed to enqueue job.");
      return { id: rows[0].id };
    },
    async removeJob(jobId) {
      await runSql(`DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid`, [jobId]);
    },
    async rescheduleJob(opts) {
      await runSql(
        `UPDATE ${schema}.queued_jobs
         SET status = 'pending', attempts = $1, run_at = $2::timestamptz, updated_at = NOW()
         WHERE id = $3::uuid`,
        [opts.attempts, new Date(opts.runAt).toISOString(), opts.jobId]
      );
    },
    async recordJobLog(log) {
      await runSql(
        `INSERT INTO ${schema}.queued_job_logs 
         (job_id, tenant_id, target_path, status, started_at, finished_at, duration_ms, response_payload, error_message, attempts)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8::jsonb, $9, $10)`,
        [
          log.jobId,
          log.tenantId,
          log.targetPath,
          log.status,
          log.startedAt,
          log.finishedAt,
          log.durationMs,
          log.responsePayload ? JSON.stringify(log.responsePayload) : null,
          log.errorMessage ?? null,
          log.attempts
        ]
      );
    },
    async getJob(jobId) {
      const rows = await runSql(
        `SELECT * FROM ${schema}.queued_jobs WHERE id = $1::uuid LIMIT 1`,
        [jobId]
      );
      return rows && rows.length > 0 ? rows[0] : null;
    },
    async cancelJob(jobId, tenantId) {
      if (tenantId) {
        await runSql(
          `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid AND tenant_id = $2::uuid AND status = 'pending'`,
          [jobId, tenantId]
        );
        return true;
      }
      await runSql(
        `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid AND status = 'pending'`,
        [jobId]
      );
      return true;
    },
    async recordScheduledLog(log) {
      const rows = await runSql(
        `INSERT INTO ${schema}.scheduled_job_logs
         (job_id, tenant_id, status, triggered_by, started_at, finished_at, duration_ms, response_payload, error_message)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8::jsonb, $9)
         RETURNING id`,
        [
          log.scheduledJobId,
          log.tenantId,
          log.status,
          log.triggeredBy,
          log.startedAt,
          log.finishedAt ?? null,
          log.durationMs ?? null,
          log.responsePayload ? JSON.stringify(log.responsePayload) : null,
          log.errorMessage ?? null
        ]
      );
      return { id: rows[0].id };
    }
  };
}

// src/db/pg.ts
function createPgAdapter(clientOrPool, options) {
  const schema = options?.schema || "common";
  return {
    raw: clientOrPool,
    async rpc(functionName, args) {
      try {
        if (!args || Object.keys(args).length === 0) {
          const res2 = await clientOrPool.query(`SELECT * FROM ${schema}.${functionName}()`);
          return { data: res2.rows && res2.rows.length > 0 ? res2.rows : null, error: null };
        }
        const keys = Object.keys(args);
        const placeholders = keys.map((_, i) => `$${i + 1}`).join(", ");
        const values = Object.values(args).map(
          (val) => typeof val === "object" && val !== null ? JSON.stringify(val) : val
        );
        const res = await clientOrPool.query(
          `SELECT * FROM ${schema}.${functionName}(${placeholders})`,
          values
        );
        return { data: res.rows && res.rows.length > 0 ? res.rows : null, error: null };
      } catch (err) {
        return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
      }
    },
    async enqueueJob(opts) {
      const runAt = opts.runAt ? new Date(opts.runAt).toISOString() : (/* @__PURE__ */ new Date()).toISOString();
      const payloadJson = JSON.stringify(opts.payload ?? {});
      const res = await clientOrPool.query(
        `INSERT INTO ${schema}.queued_jobs (tenant_id, target_path, payload, run_at, max_attempts, status)
         VALUES ($1::uuid, $2, $3::jsonb, $4::timestamptz, $5, 'pending')
         RETURNING id`,
        [opts.tenantId, opts.targetPath, payloadJson, runAt, opts.maxAttempts ?? 3]
      );
      if (!res.rows || res.rows.length === 0) {
        throw new Error("PgAdapter: Failed to enqueue job.");
      }
      return { id: res.rows[0].id };
    },
    async removeJob(jobId) {
      await clientOrPool.query(`DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid`, [jobId]);
    },
    async rescheduleJob(opts) {
      await clientOrPool.query(
        `UPDATE ${schema}.queued_jobs
         SET status = 'pending', attempts = $1, run_at = $2::timestamptz, updated_at = NOW()
         WHERE id = $3::uuid`,
        [opts.attempts, new Date(opts.runAt).toISOString(), opts.jobId]
      );
    },
    async recordJobLog(log) {
      await clientOrPool.query(
        `INSERT INTO ${schema}.queued_job_logs 
         (job_id, tenant_id, target_path, status, started_at, finished_at, duration_ms, response_payload, error_message, attempts)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8::jsonb, $9, $10)`,
        [
          log.jobId,
          log.tenantId,
          log.targetPath,
          log.status,
          log.startedAt,
          log.finishedAt,
          log.durationMs,
          log.responsePayload ? JSON.stringify(log.responsePayload) : null,
          log.errorMessage ?? null,
          log.attempts
        ]
      );
    },
    async getJob(jobId) {
      const res = await clientOrPool.query(
        `SELECT * FROM ${schema}.queued_jobs WHERE id = $1::uuid LIMIT 1`,
        [jobId]
      );
      return res.rows && res.rows.length > 0 ? res.rows[0] : null;
    },
    async cancelJob(jobId, tenantId) {
      if (tenantId) {
        const res2 = await clientOrPool.query(
          `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid AND tenant_id = $2::uuid AND status = 'pending'`,
          [jobId, tenantId]
        );
        return (res2.rowCount ?? 0) > 0;
      }
      const res = await clientOrPool.query(
        `DELETE FROM ${schema}.queued_jobs WHERE id = $1::uuid AND status = 'pending'`,
        [jobId]
      );
      return (res.rowCount ?? 0) > 0;
    },
    async recordScheduledLog(log) {
      const res = await clientOrPool.query(
        `INSERT INTO ${schema}.scheduled_job_logs
         (job_id, tenant_id, status, triggered_by, started_at, finished_at, duration_ms, response_payload, error_message)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8::jsonb, $9)
         RETURNING id`,
        [
          log.scheduledJobId,
          log.tenantId,
          log.status,
          log.triggeredBy,
          log.startedAt,
          log.finishedAt ?? null,
          log.durationMs ?? null,
          log.responsePayload ? JSON.stringify(log.responsePayload) : null,
          log.errorMessage ?? null
        ]
      );
      return { id: res.rows[0].id };
    }
  };
}

exports.createDrizzleAdapter = createDrizzleAdapter;
exports.createPgAdapter = createPgAdapter;
exports.createPrismaAdapter = createPrismaAdapter;
exports.createSupabaseAdapter = createSupabaseAdapter;
exports.createSupabaseRestAdapter = createSupabaseRestAdapter;
exports.resolveDatabaseAdapter = resolveDatabaseAdapter;
//# sourceMappingURL=index.js.map
//# sourceMappingURL=index.js.map