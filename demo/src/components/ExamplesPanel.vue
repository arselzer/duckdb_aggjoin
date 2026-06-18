<script setup lang="ts">
import { examples, KIND_LABEL, type Example } from '../data/examples'

defineProps<{ activeId: string | null; busy: boolean }>()
const emit = defineEmits<{ load: [Example] }>()
</script>

<template>
  <section class="panel ex">
    <div class="bar"><span class="eyebrow">examples</span><span class="hint mono">self-contained · builds its own data</span></div>
    <ul>
      <li
        v-for="ex in examples"
        :key="ex.id"
        class="card"
        :class="{ active: activeId === ex.id, busy }"
        @click="!busy && emit('load', ex)"
      >
        <div class="top">
          <span class="title">{{ ex.title }}</span>
          <span class="tag" :class="ex.kind === 'operator' ? 'amber' : 'cyan'">{{ KIND_LABEL[ex.kind] }}</span>
        </div>
        <p class="blurb">{{ ex.blurb }}</p>
        <p v-if="ex.dataset" class="ds mono">⤓ bundled dataset · {{ ex.dataset.sizeLabel }}</p>
      </li>
    </ul>
  </section>
</template>

<style scoped>
.ex { display: flex; flex-direction: column; }
.bar {
  display: flex; align-items: center; justify-content: space-between; gap: 10px;
  padding: 11px 16px; border-bottom: 1px solid var(--line);
}
.hint { font-size: 10px; color: var(--text-faint); }
ul { list-style: none; margin: 0; padding: 10px; display: grid; gap: 8px; }
.card {
  padding: 13px 14px; border-radius: var(--r);
  border: 1px solid var(--line); background: var(--ink-2);
  cursor: pointer; transition: border-color 0.14s ease, background 0.14s ease, transform 0.08s ease;
}
.card:hover { border-color: var(--slate); background: var(--panel-2); }
.card:active { transform: translateY(1px); }
.card.active { border-color: var(--amber-line); background: var(--amber-soft); }
.card.busy { pointer-events: none; opacity: 0.6; }
.top { display: flex; align-items: center; justify-content: space-between; gap: 10px; }
.title { font-family: var(--font-mono); font-size: 12.5px; font-weight: 600; color: var(--text); }
.blurb { margin: 7px 0 0; font-size: 11.5px; line-height: 1.5; color: var(--text-dim); }
.ds { margin: 8px 0 0; font-size: 10px; letter-spacing: 0.04em; color: var(--amber); opacity: 0.85; }
</style>
