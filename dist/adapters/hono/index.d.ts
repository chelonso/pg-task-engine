import { T as TaskEngine } from '../../engine-DrkGh7_9.js';
import '../../types-BHtRB4sA.js';
import 'zod';

interface HonoAdapterOptions {
    engine: TaskEngine;
}
/**
 * Creates a Hono sub-application router mounted with task engine endpoints.
 */
declare function createTaskEngineHono(engine: TaskEngine, HonoClass?: any): any;
declare function buildHonoRoutes(app: any, engine: TaskEngine): any;

export { type HonoAdapterOptions, buildHonoRoutes, createTaskEngineHono };
