import type { DatabaseAdapter, DomainEventOptions } from '../types.js';

export class DomainEventsManager {
  constructor(private db: DatabaseAdapter) {}

  /**
   * Publishes a domain event into the common.domain_events outbox table.
   */
  async publish(options: DomainEventOptions): Promise<{ id: string; status: 'pending' }> {
    const eventName = options.eventName || options.event_name || options.eventType;
    if (!options.tenantId) {
      throw new Error('DomainEvents: tenantId is required for multi-tenant isolation.');
    }
    if (!eventName) {
      throw new Error('DomainEvents: eventName is required.');
    }

    // 1. Try RPC fn_publish_domain_event
    const { data, error } = await this.db.rpc<string>('fn_publish_domain_event', {
      p_tenant_id: options.tenantId,
      p_event_name: eventName,
      p_payload: options.payload ?? {}
    });

    if (!error && data) {
      return { id: typeof data === 'object' && (data as any)?.id ? (data as any).id : String(data), status: 'pending' };
    }

    // 2. Direct table insert fallback
    if (this.db.raw && typeof (this.db.raw as any).query === 'function') {
      try {
        const rows = await (this.db.raw as any).query('domain_events', {
          method: 'POST',
          schema: 'common',
          headers: { Prefer: 'return=representation' },
          body: JSON.stringify({
            tenant_id: options.tenantId,
            event_name: eventName,
            payload: options.payload ?? {}
          })
        });
        const id = Array.isArray(rows) && rows[0]?.id ? rows[0].id : 'event-' + Date.now();
        return { id, status: 'pending' };
      } catch (insertErr: any) {
        throw new Error(`DomainEvents: Failed to publish event: ${insertErr?.message || error?.message}`);
      }
    }

    if (error || !data) {
      throw new Error(`DomainEvents: Failed to publish event: ${error?.message || 'No ID returned'}`);
    }

    return { id: String(data), status: 'pending' };
  }

  /**
   * Processes an outbox domain event, evaluating subscriptions and enqueuing jobs.
   */
  async processEvent(eventId: string): Promise<{ matchedSubscriptions: number }> {
    if (!eventId) {
      throw new Error('DomainEvents: eventId is required.');
    }

    const { data, error } = await this.db.rpc<number>('fn_process_domain_event', {
      p_event_id: eventId
    });

    if (error) {
      throw new Error(`DomainEvents: Failed to process event ${eventId}: ${error.message}`);
    }

    return { matchedSubscriptions: data ?? 0 };
  }
}
