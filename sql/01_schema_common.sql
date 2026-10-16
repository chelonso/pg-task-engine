-- ============================================================================
-- @shared/pg-task-engine
-- File: 01_schema_common.sql
-- Description: DDL for Core Queues, Schedulers, Domain Events, and Audit Logs
-- Schema: common
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS common;

-- ----------------------------------------------------------------------------
-- 1. Active Broker Queue (queued_jobs)
-- Stores pending and in-flight tasks. Upon completion or max retries, rows
-- are archived into queued_job_logs and removed to keep this table small and fast.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS common.queued_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    target_path VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(50) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing')),
    run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Ultra-fast partial index for worker polling using FOR UPDATE SKIP LOCKED
CREATE INDEX IF NOT EXISTS idx_queued_jobs_poll 
ON common.queued_jobs (status, run_at, created_at) 
WHERE status = 'pending';

-- Index for tenant filtering and isolation
CREATE INDEX IF NOT EXISTS idx_queued_jobs_tenant ON common.queued_jobs (tenant_id);

-- ----------------------------------------------------------------------------
-- 2. Queue Audit Trail (queued_job_logs)
-- Permanent audit log of every execution attempt and final outcome.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS common.queued_job_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL,
    tenant_id UUID NOT NULL,
    target_path VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('success', 'failed')),
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    duration_ms INTEGER NOT NULL,
    response_payload JSONB NULL,
    error_message TEXT NULL,
    attempts INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_queued_job_logs_tenant ON common.queued_job_logs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_queued_job_logs_created ON common.queued_job_logs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_queued_job_logs_lookup ON common.queued_job_logs (job_id);

-- ----------------------------------------------------------------------------
-- 3. Scheduled Tasks Catalog (scheduled_jobs)
-- Periodic tasks / Crons definitions
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS common.scheduled_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    target_path VARCHAR(255) NOT NULL,
    cron_expression VARCHAR(100) NOT NULL,
    timezone VARCHAR(50) NOT NULL DEFAULT 'UTC',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT true,
    last_run_at TIMESTAMP WITH TIME ZONE NULL,
    next_run_at TIMESTAMP WITH TIME ZONE NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_tenant ON common.scheduled_jobs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_active ON common.scheduled_jobs (is_active) WHERE is_active = true;

-- ----------------------------------------------------------------------------
-- 4. Scheduled Tasks Logs (scheduled_job_logs)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS common.scheduled_job_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES common.scheduled_jobs(id) ON DELETE CASCADE,
    tenant_id UUID NOT NULL,
    target_path VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    triggered_by VARCHAR(50) NOT NULL DEFAULT 'cron' CHECK (triggered_by IN ('cron', 'manual', 'api')),
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE NULL,
    duration_ms INTEGER NULL,
    response_payload JSONB NULL,
    error_message TEXT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scheduled_job_logs_tenant ON common.scheduled_job_logs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_scheduled_job_logs_job ON common.scheduled_job_logs (scheduled_job_id);
CREATE INDEX IF NOT EXISTS idx_scheduled_job_logs_created ON common.scheduled_job_logs (created_at DESC);

-- ----------------------------------------------------------------------------
-- 5. Domain Events Outbox (domain_events)
-- Decoupled transactional event publishing
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS common.domain_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source VARCHAR(100) NOT NULL DEFAULT 'system',
    status VARCHAR(50) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processed', 'failed')),
    error_message TEXT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE NULL
);

CREATE INDEX IF NOT EXISTS idx_domain_events_status ON common.domain_events (status, created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_domain_events_tenant ON common.domain_events (tenant_id);

-- ----------------------------------------------------------------------------
-- 6. Event Subscriptions (event_subscriptions)
-- Routes domain events to queue targets
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS common.event_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    target_path VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_event_subscription UNIQUE (tenant_id, event_type, target_path)
);

CREATE INDEX IF NOT EXISTS idx_event_subscriptions_lookup ON common.event_subscriptions (event_type, is_active) WHERE is_active = true;
