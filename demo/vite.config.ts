import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// Base path: GitHub Pages project sites are served from `/<repo>/`.
// A relative base (`./`) makes the build portable to any subpath without
// hard-coding the repo name — robust for this single-page, no-router demo.
// Override with VITE_BASE (e.g. "/duckdb_aggjoin/") if you prefer absolute.
export default defineConfig({
  base: process.env.VITE_BASE ?? './',
  plugins: [vue()],
  // DuckDB-WASM + top-level await / BigInt need a modern target.
  build: { target: 'esnext' },
  esbuild: { target: 'esnext' },
  optimizeDeps: {
    // The actual engine .wasm + worker are fetched from a CDN at runtime
    // (see src/duckdb/engine.ts); Vite only bundles the JS API surface.
    exclude: ['@duckdb/duckdb-wasm'],
    esbuildOptions: { target: 'esnext' },
  },
  server: { port: 4180 },
})
