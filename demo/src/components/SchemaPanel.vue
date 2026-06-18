<script setup lang="ts">
import { ref } from 'vue'
import type { TableInfo } from '../engine'

defineProps<{ tables: TableInfo[] }>()
const emit = defineEmits<{ use: [string]; drop: [string] }>()

const open = ref<Record<string, boolean>>({})
</script>

<template>
  <section class="panel sch">
    <div class="bar">
      <span class="eyebrow">tables</span>
      <span class="cnt mono">{{ tables.length }}</span>
    </div>
    <div v-if="!tables.length" class="empty mono">no tables — import data or run an example</div>
    <ul v-else>
      <li v-for="t in tables" :key="t.name">
        <div class="trow">
          <button class="name mono" @click="open[t.name] = !open[t.name]">
            <span class="caret" :class="{ o: open[t.name] }">▸</span>{{ t.name }}
          </button>
          <span class="rows mono">{{ t.rows.toLocaleString() }}</span>
          <button class="act" title="SELECT *" @click="emit('use', t.name)">↳</button>
          <button class="act drop" title="DROP" @click="emit('drop', t.name)">✕</button>
        </div>
        <ul v-if="open[t.name]" class="cols">
          <li v-for="c in t.columns" :key="c.name" class="col mono">
            <span>{{ c.name }}</span><span class="ty">{{ c.type }}</span>
          </li>
        </ul>
      </li>
    </ul>
  </section>
</template>

<style scoped>
.sch { display: flex; flex-direction: column; }
.bar {
  display: flex; align-items: center; justify-content: space-between;
  padding: 11px 16px; border-bottom: 1px solid var(--line);
}
.cnt { font-size: 11px; color: var(--text-faint); }
.empty { padding: 18px 16px; font-size: 11px; color: var(--text-faint); text-align: center; }
ul { list-style: none; margin: 0; padding: 6px; }
.trow { display: grid; grid-template-columns: 1fr auto auto auto; align-items: center; gap: 6px; padding: 2px 4px; }
.name { background: none; border: 0; color: var(--text); font-size: 12px; text-align: left; display: flex; align-items: center; gap: 7px; padding: 5px 4px; }
.name:hover { color: var(--amber); }
.caret { color: var(--text-faint); transition: transform 0.12s ease; font-size: 9px; }
.caret.o { transform: rotate(90deg); }
.rows { font-size: 10.5px; color: var(--text-faint); }
.act { background: none; border: 0; color: var(--text-faint); font-size: 12px; padding: 4px 5px; border-radius: 3px; }
.act:hover { color: var(--amber); background: var(--amber-soft); }
.act.drop:hover { color: var(--red); background: rgba(236, 106, 79, 0.1); }
.cols { padding: 0 4px 6px 22px; }
.col { display: flex; justify-content: space-between; gap: 12px; padding: 3px 6px; font-size: 11px; color: var(--text-dim); }
.col .ty { color: var(--text-faint); }
</style>
