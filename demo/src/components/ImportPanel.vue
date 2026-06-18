<script setup lang="ts">
import { ref } from 'vue'

defineProps<{ busy: boolean }>()
const emit = defineEmits<{ files: [File[]] }>()

const dragging = ref(false)
const input = ref<HTMLInputElement | null>(null)

function pick(e: Event) {
  const files = Array.from((e.target as HTMLInputElement).files ?? [])
  if (files.length) emit('files', files)
  ;(e.target as HTMLInputElement).value = ''
}
function drop(e: DragEvent) {
  dragging.value = false
  const files = Array.from(e.dataTransfer?.files ?? [])
  if (files.length) emit('files', files)
}
</script>

<template>
  <section class="panel imp">
    <div class="bar"><span class="eyebrow">import data</span></div>

    <div
      class="zone"
      :class="{ over: dragging, busy }"
      @dragover.prevent="dragging = true"
      @dragleave.prevent="dragging = false"
      @drop.prevent="drop"
      @click="input?.click()"
    >
      <input ref="input" type="file" multiple accept=".csv,.tsv,.parquet,.json,.ndjson,.sql" hidden @change="pick" />
      <div class="ico">{{ busy ? '◴' : '⤓' }}</div>
      <p class="t">{{ busy ? 'importing…' : 'Drop a file or click to browse' }}</p>
      <div class="fmts">
        <span class="chip">CSV</span><span class="chip">TSV</span>
        <span class="chip">Parquet</span><span class="chip">JSON</span><span class="chip">.sql dump</span>
      </div>
    </div>
    <p class="note mono">
      Everything runs locally in DuckDB-WASM — files never leave your browser.
    </p>
  </section>
</template>

<style scoped>
.imp { display: flex; flex-direction: column; }
.bar { padding: 11px 16px; border-bottom: 1px solid var(--line); }
.zone {
  margin: 14px; padding: 26px 18px; text-align: center;
  border: 1.5px dashed var(--line-strong); border-radius: var(--r-lg);
  background: var(--ink-2); cursor: pointer;
  transition: border-color 0.15s ease, background 0.15s ease, transform 0.1s ease;
}
.zone:hover { border-color: var(--slate); }
.zone.over { border-color: var(--amber); background: var(--amber-soft); transform: scale(1.01); }
.zone.busy { pointer-events: none; opacity: 0.7; }
.ico { font-size: 26px; color: var(--amber); line-height: 1; }
.t { margin: 12px 0 14px; font-size: 13px; color: var(--text); }
.fmts { display: flex; flex-wrap: wrap; gap: 6px; justify-content: center; }
.chip {
  font-family: var(--font-mono); font-size: 10px; font-weight: 600; letter-spacing: 0.05em;
  padding: 3px 8px; border-radius: 999px;
  border: 1px solid var(--line-strong); color: var(--text-dim);
}
.note { padding: 0 16px 15px; font-size: 10.5px; color: var(--text-faint); text-align: center; }
</style>
