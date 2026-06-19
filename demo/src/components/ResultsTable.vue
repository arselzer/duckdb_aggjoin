<script setup lang="ts">
import type { QueryResult } from '../engine'

defineProps<{ result: QueryResult | null; error: string | null; exportBusy: boolean }>()
const emit = defineEmits<{
  export: ['csv' | 'parquet']
}>()

function cell(v: unknown): string {
  if (v === null) return 'NULL'
  return String(v)
}
</script>

<template>
  <section class="panel results">
    <div class="bar">
      <span class="eyebrow">result</span>
      <div v-if="result" class="meta-wrap">
        <span class="meta mono">
          {{ result.rowCount.toLocaleString() }} rows · {{ result.ms.toFixed(1) }} ms
        </span>
        <button class="mini" :disabled="exportBusy" @click="emit('export', 'csv')">CSV</button>
        <button class="mini" :disabled="exportBusy" @click="emit('export', 'parquet')">Parquet</button>
      </div>
    </div>

    <div v-if="error" class="err mono">
      <span class="tag" style="color: var(--red); border-color: rgba(236,106,79,.35)">error</span>
      <pre>{{ error }}</pre>
    </div>

    <div v-else-if="result" class="scroll">
      <table>
        <thead>
          <tr>
            <th class="rn">#</th>
            <th v-for="c in result.columns" :key="c">{{ c }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, ri) in result.rows" :key="ri">
            <td class="rn">{{ ri + 1 }}</td>
            <td v-for="(v, ci) in row" :key="ci" :class="{ null: v === null }">{{ cell(v) }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="result.truncated" class="trunc mono">
        showing first {{ result.rows.length.toLocaleString() }} of {{ result.rowCount.toLocaleString() }} rows
      </p>
    </div>

    <div v-else class="empty mono">no result yet</div>
  </section>
</template>

<style scoped>
.results { display: flex; flex-direction: column; min-height: 0; }
.bar {
  display: flex; align-items: center; justify-content: space-between; gap: 12px;
  padding: 11px 16px; border-bottom: 1px solid var(--line);
}
.meta-wrap { display: flex; align-items: center; justify-content: flex-end; gap: 8px; flex-wrap: wrap; }
.meta { font-size: 11px; color: var(--text-dim); }
.mini {
  border: 1px solid var(--line-strong);
  background: var(--panel-2);
  color: var(--text-dim);
  border-radius: var(--r);
  padding: 4px 8px;
  font-family: var(--font-mono);
  font-size: 10.5px;
  font-weight: 600;
}
.mini:hover:not(:disabled) { color: var(--blue); border-color: var(--blue-line); }
.mini:disabled { opacity: 0.45; }
.empty { padding: 30px 16px; color: var(--text-faint); font-size: 12px; text-align: center; }

.err { padding: 14px 16px; }
.err pre { margin: 10px 0 0; color: var(--red); font-size: 12px; white-space: pre-wrap; line-height: 1.5; }

.scroll { overflow: auto; max-height: 360px; }
table { border-collapse: collapse; width: 100%; font-family: var(--font-mono); font-size: 12px; }
thead th {
  position: sticky; top: 0; z-index: 1;
  text-align: left; padding: 9px 14px;
  background: var(--panel-2);
  color: var(--amber);
  font-weight: 600; font-size: 11px; letter-spacing: 0.03em;
  border-bottom: 1px solid var(--line-strong);
  white-space: nowrap;
}
tbody td { padding: 7px 14px; border-bottom: 1px solid var(--line); color: var(--text); white-space: nowrap; }
tbody tr:hover td { background: rgba(245, 180, 23, 0.04); }
td.null { color: var(--text-faint); font-style: italic; }
.rn { color: var(--text-faint); text-align: right; width: 1px; user-select: none; }
.trunc { padding: 10px 14px; color: var(--text-faint); font-size: 11px; }
</style>
