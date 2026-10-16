import { a as DatabaseAdapter } from '../types-CfVXbtgI.js';
import 'zod';

/**
 * Normalizes input db into a full DatabaseAdapter.
 * If input already implements DatabaseAdapter, returns it.
 * If it has .rpc, wraps it as a Supabase-compatible adapter.
 */
declare function resolveDatabaseAdapter(db: unknown): DatabaseAdapter;

interface SupabaseClientLike {
    rpc(fn: string, args?: Record<string, unknown>): Promise<{
        data: unknown;
        error: unknown;
    }>;
    from(table: string): any;
    schema?(schema: string): any;
}
declare function createSupabaseAdapter(client: SupabaseClientLike, options?: {
    schema?: string;
}): DatabaseAdapter<SupabaseClientLike>;
interface SupabaseRestAdapterOptions {
    url: string;
    serviceKey: string;
    schema?: string;
}
interface SupabaseRestClient {
    url: string;
    serviceKey: string;
    query<T = any>(path: string, options?: {
        schema?: string;
        method?: string;
        headers?: Record<string, string>;
        body?: any;
    }): Promise<T>;
    rpc<T = any>(fn: string, args?: Record<string, unknown>, options?: {
        schema?: string;
    }): Promise<{
        data: T | null;
        error: Error | null;
    }>;
}
/**
 * Agnostic Supabase REST Adapter using native Web Fetch API.
 * Requires zero external dependencies. Ideal for lightweight services and microservices.
 */
declare function createSupabaseRestAdapter(options: SupabaseRestAdapterOptions): DatabaseAdapter<SupabaseRestClient>;

interface PrismaClientLike {
    $queryRawUnsafe<T = unknown>(query: string, ...values: unknown[]): Promise<T>;
    $executeRawUnsafe(query: string, ...values: unknown[]): Promise<number>;
}
declare function createPrismaAdapter(prisma: PrismaClientLike, options?: {
    schema?: string;
}): DatabaseAdapter<PrismaClientLike>;

interface DrizzleClientLike {
    execute<T = any>(query: any): Promise<{
        rows?: T[];
        [key: string]: any;
    }>;
}
declare function createDrizzleAdapter(drizzleDb: DrizzleClientLike, options?: {
    schema?: string;
}): DatabaseAdapter<DrizzleClientLike>;

interface PgQueryable {
    query<T = any>(text: string, params?: any[]): Promise<{
        rows: T[];
    }>;
}
declare function createPgAdapter(clientOrPool: PgQueryable, options?: {
    schema?: string;
}): DatabaseAdapter<PgQueryable>;

export { type DrizzleClientLike, type PgQueryable, type PrismaClientLike, type SupabaseClientLike, type SupabaseRestAdapterOptions, type SupabaseRestClient, createDrizzleAdapter, createPgAdapter, createPrismaAdapter, createSupabaseAdapter, createSupabaseRestAdapter, resolveDatabaseAdapter };
