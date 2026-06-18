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
      <div class="wordmark">
        <h1>AGGJOIN</h1>
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
.brand { display: flex; align-items: center; }
.wordmark h1 {
  margin: 0;
  font-family: var(--font-display);
  font-weight: 800;
  font-size: 23px;
  letter-spacing: 0.12em;
  line-height: 1;
  color: var(--blue);
}
.status { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.ghlink:hover { color: var(--amber); border-color: var(--amber-line); }
</style>
