# HTTP API Reference: `@shared/pg-task-engine`

The engine exposes standard RESTful endpoints through its native HTTP controller. All payloads use `application/json`.

---

## 1. Enqueue Job

Adds an asynchronous background job to the queue.

- **Method**: `POST`
- **Path**: `/add`
- **Status Code**: `202 Accepted`

### Request Body
```json
{
  "tenantId": "d1a2f3e4-5b6c-7d8e-9f0a-1b2c3d4e5f6a",
  "targetPath": "billing-generate-invoices",
  "payload": {
    "month": 10,
    "year": 2026,
    "dryRun": false
  },
  "runAt": "2026-10-16T12:00:00Z",
  "maxAttempts": 5
}
```

### Parameters
| Field | Type | Required | Description |
|---|---|---|---|
| `tenantId` | `string` (UUID) | **Yes** | Multi-tenant isolation UUID |
| `targetPath` | `string` | **Yes** | Task identifier registered in the engine or `rpc:stored_procedure` |
| `payload` | `object` | No | Arbitrary JSON payload passed to the task handler |
| `runAt` | `string` (ISO Date) | No | Schedule delayed execution (default: immediate `NOW()`) |
| `maxAttempts` | `number` | No | Maximum retry attempts on failure (default: `3`) |

### Response (`202 Accepted`)
```json
{
  "success": true,
  "jobId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "pending",
  "message": "Task successfully enqueued"
}
```

---

## 2. Get Job Status

Fetches the live status or historical execution audit of a specific job.

- **Method**: `GET`
- **Path**: `/status/:id`
- **Status Code**: `200 OK` (or `404 Not Found`)

### Response: Pending / In-Flight (`200 OK`)
```json
{
  "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "pending",
  "attempts": 0,
  "maxAttempts": 3,
  "targetPath": "billing-generate-invoices",
  "runAt": "2026-10-16T12:00:00.000Z"
}
```

### Response: Completed (`200 OK`)
```json
{
  "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "completed",
  "attempts": 1,
  "targetPath": "billing-generate-invoices",
  "durationMs": 412,
  "responsePayload": {
    "invoicesGenerated": 42
  },
  "errorMessage": null,
  "finishedAt": "2026-10-16T12:00:00.412Z"
}
```

---

## 3. Cancel Job

Cancels a pending job before worker pickup.

- **Method**: `DELETE`
- **Path**: `/cancel/:id`
- **Query Params (Optional)**: `?tenant_id=<UUID>` (for tenant safety)
- **Status Code**: `200 OK` (or `404 Not Found`)

### Response (`200 OK`)
```json
{
  "success": true,
  "message": "Job cancelled successfully"
}
```

---

## 4. Execute Scheduled Cron Task

Triggers the execution of a scheduled task directly (used by `pg_cron` or administrative dashboards).

- **Method**: `POST`
- **Path**: `/cron/execute`
- **Status Code**: `200 OK` (or `500 Internal Server Error`)

### Request Body
```json
{
  "jobId": "3b2c1d0e-9f8a-7b6c-5d4e-3f2a1b0c9d8e",
  "tenantId": "d1a2f3e4-5b6c-7d8e-9f0a-1b2c3d4e5f6a",
  "targetPath": "rpc:fn_reconcile_attendance",
  "payload": {},
  "triggeredBy": "cron"
}
```

### Response (`200 OK`)
```json
{
  "logId": "7a8b9c0d-1e2f-3a4b-5c6d-7e8f9a0b1c2d",
  "status": "success",
  "durationMs": 156,
  "result": {
    "reconciled": 18
  }
}
```

---

## 5. Publish Domain Event

Publishes an event into the transactional outbox table (`common.domain_events`).

- **Method**: `POST`
- **Path**: `/events/publish`
- **Status Code**: `202 Accepted`

### Request Body
```json
{
  "tenantId": "d1a2f3e4-5b6c-7d8e-9f0a-1b2c3d4e5f6a",
  "eventType": "payment.succeeded",
  "payload": {
    "subscriptionId": "sub_12345",
    "amount": 150000,
    "currency": "PYG"
  },
  "source": "pos_terminal"
}
```

### Response (`202 Accepted`)
```json
{
  "id": "e5f6a7b8-c9d0-1e2f-3a4b-5c6d7e8f9a0b",
  "status": "pending"
}
```
