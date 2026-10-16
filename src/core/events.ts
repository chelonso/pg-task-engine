import type { DatabaseAdapter, DomainEventOptions } from '../types.js';

export class DomainEventsManager {
  constructor(private db: DatabaseAdapter) {}

  /**
   * Publishes a domain event into the common.domain_events outbox table.
   */
  async publish(options: DomainEventOptions): Promise<{ id: string; status: 'pending' }> {
    if (!options.tenantId) {
      throw new Error('DomainEvents: tenantId is required for multi-tenant isolation.');
    }
    if (!options.eventType) {
      throw new Error('DomainEvents: eventType is required.');
    }

    const { data, error } = await this.db.rpc<string>('fn_publish_domain_event', {
      p_tenant_id: options.tenantId,
      p_event_type: options.eventType,
      p_payload: options.payload ?? {},
      p_source: options.source ?? 'system'
    });

    if (error || !data) {
      throw new Error(
        `DomainEvents: Failed to publish event: ${error?.message || 'No ID returned'}`
      );
    }

    return { id: data, status: 'pending' };
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
