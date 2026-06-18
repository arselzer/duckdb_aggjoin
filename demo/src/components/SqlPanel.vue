<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  modelValue: string
  busy: boolean
  ready: boolean
}>()
const emit = defineEmits<{
  'update:modelValue': [string]
  run: []
  benchmark: []
}>()

const lineCount = computed(() => Math.max(props.modelValue.split('\n').length, 1))

function onInput(e: Event) {
  emit('update:modelValue', (e.target as HTMLTextAreaElement).value)
}
function onKeydown(e: KeyboardEvent) {
  if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
    e.preventDefault()
    emit('benchmark')
  }
  if (e.key === 'Tab') {
    e.preventDefault()
    const ta = e.target as HTMLTextAreaElement
    const s = ta.selectionStart
    const v = ta.value
    emit('update:modelValue', v.slice(0, s) + '  ' + v.slice(ta.selectionEnd))
    requestAnimationFrame(() => (ta.selectionStart = ta.selectionEnd = s + 2))
  }
}
</script>

<template>
  <section class="panel editor">
    <div class="bar">
      <span class="eyebrow">query</span>
      <span class="hint mono">⌘↵ to benchmark</span>
    </div>
    <div class="surface">
      <div class="gutter mono" aria-hidden="true">
        <span v-for="n in lineCount" :key="n">{{ n }}</span>
      </div>
      <textarea
        class="code mono"
        :value="modelValue"
        spellcheck="false"
        autocomplete="off"
        autocapitalize="off"
        placeholder="-- write SQL, or pick an example →"
        @input="onInput"
        @keydown="onKeydown"
      />
    </div>
    <div class="actions">
      <button class="btn primary" :disabled="busy || !ready" @click="emit('benchmark')">
        <span v-if="busy" class="spin ring" />
        <span>{{ busy ? 'measuring…' : 'Benchmark ⚡' }}</span>
      </button>
      <button class="btn ghost" :disabled="busy || !ready" @click="emit('run')">Run once</button>
    </div>
  </section>
</template>

<style scoped>
.editor { display: flex; flex-direction: column; overflow: hidden; }
.bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 11px 16px;
  border-bottom: 1px solid var(--line);
}
.hint { font-size: 10.5px; color: var(--text-faint); }
.surface {
  display: flex;
  background: var(--ink-2);
  min-height: 220px;
}
.gutter {
  display: flex;
  flex-direction: column;
  padding: 14px 0;
  width: 42px;
  text-align: right;
  color: var(--text-faint);
  font-size: 12.5px;
  line-height: 1.65;
  user-select: none;
  border-right: 1px solid var(--line);
  background: rgba(0, 0, 0, 0.2);
}
.gutter span { padding-right: 12px; }
.code {
  flex: 1;
  resize: vertical;
  border: 0;
  outline: none;
  background: transparent;
  color: var(--text);
  padding: 14px 16px;
  font-size: 13px;
  line-height: 1.65;
  tab-size: 2;
  caret-color: var(--amber);
  min-height: 220px;
}
.code::placeholder { color: var(--text-faint); }
.actions { display: flex; gap: 10px; padding: 13px 16px; border-top: 1px solid var(--line); }
.btn {
  display: inline-flex;
  align-items: center;
  gap: 9px;
  border-radius: var(--r);
  padding: 10px 18px;
  font-family: var(--font-mono);
  font-size: 12.5px;
  font-weight: 600;
  letter-spacing: 0.04em;
  border: 1px solid transparent;
  transition: transform 0.08s ease, filter 0.15s ease, background 0.15s ease;
}
.btn:active:not(:disabled) { transform: translateY(1px); }
.btn:disabled { opacity: 0.45; }
.primary {
  color: #14100a;
  background: linear-gradient(180deg, var(--amber-bright), var(--amber));
  box-shadow: 0 8px 22px -10px rgba(245, 180, 23, 0.7);
}
.primary:hover:not(:disabled) { filter: brightness(1.08); }
.ghost { color: var(--text-dim); border-color: var(--line-strong); background: var(--panel-2); }
.ghost:hover:not(:disabled) { color: var(--text); border-color: var(--slate); }
.ring {
  width: 13px;
  height: 13px;
  border-radius: 99px;
  border: 2px solid rgba(20, 16, 10, 0.35);
  border-top-color: #14100a;
}
</style>
