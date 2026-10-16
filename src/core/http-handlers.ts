import type { TaskEngine } from './engine.js';

export class TaskHttpController {
  constructor(private engine: TaskEngine) {}

  /**
   * POST /add (Enqueue a job)
   */
  async handleEnqueue(request: Request): Promise<Response> {
    try {
      const body: any = await request.json().catch(() => ({}));
      const tenantId = body.tenantId || body.tenant_id;
      const targetPath = body.targetPath || body.target_path;
      const payload = body.payload ?? {};
      const runAt = body.runAt || body.run_at;
      const maxAttempts = body.maxAttempts || body.max_attempts;

      if (!tenantId) {
        return new Response(
          JSON.stringify({ error: 'Missing required field: tenantId (required for multi-tenant isolation)' }),
          { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
      }

      if (!targetPath) {
        return new Response(
          JSON.stringify({ error: 'Missing required field: targetPath' }),
          { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
      }

      const result = await this.engine.queue.enqueue({
        tenantId,
        targetPath,
        payload,
        runAt,
        maxAttempts
      });

      return new Response(
        JSON.stringify({
          success: true,
          jobId: result.id,
          job_id: result.id,
          status: result.status,
          message: 'Task successfully enqueued'
        }),
        {
          status: 202,
          headers: { 'Content-Type': 'application/json' }
        }
      );
    } catch (err: any) {
      return new Response(
        JSON.stringify({ error: err?.message || 'Internal server error while enqueuing task' }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * GET /status/:id (Get job status)
   */
  async handleGetStatus(jobId: string): Promise<Response> {
    try {
      if (!jobId) {
        return new Response(
          JSON.stringify({ error: 'Missing jobId parameter' }),
          { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
      }

      const status = await this.engine.queue.getStatus(jobId);

      return new Response(JSON.stringify(status), {
        status: status.status === 'not_found' ? 404 : 200,
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (err: any) {
      return new Response(
        JSON.stringify({ error: err?.message || 'Internal server error while retrieving task status' }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * DELETE /cancel/:id (Cancel a pending job)
   */
  async handleCancel(jobId: string, tenantId?: string): Promise<Response> {
    try {
      if (!jobId) {
        return new Response(
          JSON.stringify({ error: 'Missing jobId parameter' }),
          { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
      }

      const result = await this.engine.queue.cancel(jobId, tenantId);

      return new Response(JSON.stringify(result), {
        status: result.success ? 200 : 404,
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (err: any) {
      return new Response(
        JSON.stringify({ error: err?.message || 'Internal server error while cancelling task' }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * POST /cron/execute (Trigger scheduled cron execution)
   */
  async handleCronExecute(request: Request): Promise<Response> {
    try {
      const body: any = await request.json().catch(() => ({}));
      const jobId = body.jobId || body.job_id;
      const tenantId = body.tenantId || body.tenant_id;
      const targetPath = body.targetPath || body.target_path;
      const payload = body.payload ?? {};
      const triggeredBy = body.triggeredBy || body.triggered_by || 'api';

      if (!jobId || !tenantId || !targetPath) {
        return new Response(
          JSON.stringify({ error: 'Missing required parameters: jobId, tenantId, targetPath' }),
          { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
      }

      const result = await this.engine.scheduler.execute({
        jobId,
        tenantId,
        targetPath,
        payload,
        triggeredBy
      });

      return new Response(JSON.stringify(result), {
        status: result.status === 'success' ? 200 : 500,
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (err: any) {
      return new Response(
        JSON.stringify({ error: err?.message || 'Internal server error executing scheduled task' }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * POST /events/publish (Publish a domain event)
   */
  async handlePublishEvent(request: Request): Promise<Response> {
    try {
      const body: any = await request.json().catch(() => ({}));
      const tenantId = body.tenantId || body.tenant_id;
      const eventName = body.eventName || body.event_name || body.eventType || body.event_type;
      const payload = body.payload ?? {};
      const source = body.source || 'api';

      if (!tenantId || !eventName) {
        return new Response(
          JSON.stringify({ error: 'Missing required parameters: tenantId, event_name' }),
          { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
      }

      const result = await this.engine.events.publish({
        tenantId,
        eventName,
        payload,
        source
      });

      return new Response(JSON.stringify(result), {
        status: 202,
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (err: any) {
      return new Response(
        JSON.stringify({ error: err?.message || 'Internal server error publishing event' }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }
}
