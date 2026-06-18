<script setup lang="ts">
defineProps<{
  version: string
  aggjoinLoaded: boolean
  ready: boolean
}>()
</script>

<template>
  <header class="hdr reveal">
    <div class="brand">
      <svg class="glyph" viewBox="0 0 32 32" aria-hidden="true">
        <path d="M5 8 H13 Q21 8 21 16 Q21 24 5 24" fill="none" stroke="var(--amber)" stroke-width="2.4" stroke-linecap="round" />
        <path d="M5 16 H18" fill="none" stroke="var(--amber)" stroke-width="2.4" stroke-linecap="round" />
        <circle cx="25" cy="16" r="3" fill="var(--amber)" />
      </svg>
      <div class="wordmark">
        <h1>AGGJOIN</h1>
        <p class="sub">aggregate-over-join · in your browser</p>
      </div>
    </div>

    <div class="status">
      <span class="tag" :class="ready ? 'green' : 'slate'">
        <span class="dot" />{{ ready ? 'engine ready' : 'booting' }}
      </span>
      <span v-if="aggjoinLoaded" class="tag amber">aggjoin loaded</span>
      <span v-if="version" class="tag slate mono">{{ version }}</span>
      <a class="tag slate ghlink" href="https://duckdb.org/docs/stable/core_extensions/overview" target="_blank" rel="noopener">
        DuckDB-WASM ↗
      </a>
    </div>
  </header>
</template>

<style scoped>
.hdr {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
  flex-wrap: wrap;
  padding: 22px 4px 20px;
  border-bottom: 1px solid var(--line);
}
.brand { display: flex; align-items: center; gap: 14px; }
.glyph { width: 38px; height: 38px; filter: drop-shadow(0 0 14px rgba(245, 180, 23, 0.4)); }
.wordmark h1 {
  margin: 0;
  font-family: var(--font-display);
  font-weight: 800;
  font-size: 23px;
  letter-spacing: 0.16em;
  line-height: 1;
}
.sub {
  margin: 5px 0 0;
  font-family: var(--font-mono);
  font-size: 11px;
  letter-spacing: 0.04em;
  color: var(--text-faint);
}
.status { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.ghlink:hover { color: var(--amber); border-color: var(--amber-line); }
</style>
