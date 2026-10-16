-- ============================================================================
-- @shared/pg-task-engine
-- File: 02_functions_procedures.sql
-- Description: Stored Procedures & Functions for Atomic Job Polling and Event Routing
-- Schema: common
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. fn_poll_next_job()
-- Atomically selects, locks, and marks the next pending job as 'processing'
-- using FOR UPDATE SKIP LOCKED. Multiple concurrent workers can call this
-- simultaneously without deadlocks or contention.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION common.fn_poll_next_job()
RETURNS TABLE (
    id UUID,
    tenant_id UUID,
    target_path VARCHAR,
    payload JSONB,
    attempts INTEGER,
    max_attempts INTEGER
) AS $$
DECLARE
    v_job_record RECORD;
BEGIN
    UPDATE common.queued_jobs
    SET status = 'processing',
        updated_at = NOW()
    WHERE common.queued_jobs.id = (
        SELECT q.id 
        FROM common.queued_jobs q
        WHERE q.status = 'pending' 
          AND q.run_at <= NOW()
        ORDER BY q.run_at ASC, q.created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    RETURNING 
        common.queued_jobs.id, 
        common.queued_jobs.tenant_id, 
        common.queued_jobs.target_path, 
        common.queued_jobs.payload, 
        common.queued_jobs.attempts,
        common.queued_jobs.max_attempts
    INTO v_job_record;

    IF v_job_record.id IS NOT NULL THEN
        id := v_job_record.id;
        tenant_id := v_job_record.tenant_id;
        target_path := v_job_record.target_path;
        payload := v_job_record.payload;
        attempts := v_job_record.attempts;
        max_attempts := v_job_record.max_attempts;
        RETURN NEXT;
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ----------------------------------------------------------------------------
-- 2. fn_enqueue_job(...)
-- Helper to enqueue a background job from PostgreSQL triggers or RPCs.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION common.fn_enqueue_job(
    p_tenant_id UUID,
    p_target_path VARCHAR,
    p_payload JSONB DEFAULT '{}'::jsonb,
    p_run_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    p_max_attempts INTEGER DEFAULT 3
)
RETURNS UUID AS $$
DECLARE
    v_job_id UUID;
BEGIN
    INSERT INTO common.queued_jobs (
        tenant_id,
        target_path,
        payload,
        run_at,
        max_attempts
    ) VALUES (
        p_tenant_id,
        p_target_path,
        COALESCE(p_payload, '{}'::jsonb),
        COALESCE(p_run_at, NOW()),
        COALESCE(p_max_attempts, 3)
    )
    RETURNING id INTO v_job_id;

    RETURN v_job_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ----------------------------------------------------------------------------
-- 3. fn_publish_domain_event(...)
-- Helper to publish a domain event transactionally into the outbox.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION common.fn_publish_domain_event(
    p_tenant_id UUID,
    p_event_type VARCHAR,
    p_payload JSONB DEFAULT '{}'::jsonb,
    p_source VARCHAR DEFAULT 'system'
)
RETURNS UUID AS $$
DECLARE
    v_event_id UUID;
BEGIN
    INSERT INTO common.domain_events (
        tenant_id,
        event_type,
        payload,
        source
    ) VALUES (
        p_tenant_id,
        p_event_type,
        COALESCE(p_payload, '{}'::jsonb),
        COALESCE(p_source, 'system')
    )
    RETURNING id INTO v_event_id;

    RETURN v_event_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ----------------------------------------------------------------------------
-- 4. fn_process_domain_event(...)
-- Evaluates an event against subscriptions and enqueues matching jobs.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION common.fn_process_domain_event(
    p_event_id UUID
)
RETURNS INTEGER AS $$
DECLARE
    v_event RECORD;
    v_sub RECORD;
    v_count INTEGER := 0;
BEGIN
    SELECT * INTO v_event FROM common.domain_events WHERE id = p_event_id;
    IF v_event.id IS NULL THEN
        RAISE EXCEPTION 'Domain event with id % not found', p_event_id;
    END IF;

    FOR v_sub IN 
        SELECT target_path 
        FROM common.event_subscriptions 
        WHERE tenant_id = v_event.tenant_id 
          AND event_type = v_event.event_type 
          AND is_active = true
    LOOP
        PERFORM common.fn_enqueue_job(
            v_event.tenant_id,
            v_sub.target_path,
            jsonb_build_object(
                'event_id', v_event.id,
                'event_type', v_event.event_type,
                'event_payload', v_event.payload,
                'published_at', v_event.created_at
            )
        );
        v_count := v_count + 1;
    END LOOP;

    UPDATE common.domain_events
    SET status = 'processed',
        processed_at = NOW()
    WHERE id = p_event_id;

    RETURN v_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Grant execution permissions
GRANT EXECUTE ON FUNCTION common.fn_poll_next_job() TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION common.fn_enqueue_job(UUID, VARCHAR, JSONB, TIMESTAMP WITH TIME ZONE, INTEGER) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION common.fn_publish_domain_event(UUID, VARCHAR, JSONB, VARCHAR) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION common.fn_process_domain_event(UUID) TO authenticated, service_role;
