import { defineConfig } from 'tsup';

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    'adapters/fastify/index': 'src/adapters/fastify/index.ts',
    'adapters/hono/index': 'src/adapters/hono/index.ts',
    'adapters/express/index': 'src/adapters/express/index.ts',
    'adapters/next/index': 'src/adapters/next/index.ts',
    'db/index': 'src/db/index.ts',
  },
  format: ['cjs', 'esm'],
  dts: true,
  splitting: false,
  sourcemap: true,
  clean: true,
  treeshake: true,
  external: [
    'fastify',
    'hono',
    'express',
    'next',
    '@supabase/supabase-js'
  ]
});
