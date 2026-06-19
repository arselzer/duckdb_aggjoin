<script setup lang="ts">
import { computed, ref } from 'vue'

const props = defineProps<{
  modelValue: string
  busy: boolean
  ready: boolean
  timeoutSeconds: number
}>()
const emit = defineEmits<{
  'update:modelValue': [string]
  'update:timeoutSeconds': [number]
  benchmark: []
  optimizedOnly: []
  nativeOnly: []
  cancel: []
  newQuery: []
}>()

const lineCount = computed(() => Math.max(props.modelValue.split('\n').length, 1))
const textarea = ref<HTMLTextAreaElement | null>(null)
const highlight = ref<HTMLElement | null>(null)

const sqlToken =
  /(--[^\n]*|'(?:''|[^'])*'|"(?:\"\"|[^"])*"|\b(?:SELECT|FROM|WHERE|JOIN|ON|GROUP|BY|ORDER|AS|CREATE|OR|REPLACE|TABLE|WITH|COUNT|SUM|AVG|MIN|MAX|VAR_POP|ROUND|AND|OR|IN|IS|NOT|NULL|LIMIT|SET|DROP|INSERT|INTO|VALUES|UNION|ALL|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|HAVING|CASE|WHEN|THEN|ELSE|END)\b|\b\d+(?:\.\d+)?\b)/gi

function esc(s: string) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}

function highlightSql(sql: string) {
  let out = ''
  let last = 0
  for (const m of sql.matchAll(sqlToken)) {
    const token = m[0]
    const idx = m.index ?? 0
    out += esc(sql.slice(last, idx))
    const cls = token.startsWith('--')
      ? 'comment'
      : token.startsWith("'") || token.startsWith('"')
        ? 'string'
        : /^\d/.test(token)
          ? 'number'
          : 'keyword'
    out += `<span class="tok-${cls}">${esc(token)}</span>`
    last = idx + token.length
  }
  out += esc(sql.slice(last))
  return out || '<span class="placeholder">-- write SQL, or pick an example</span>'
}

const highlightedSql = computed(() => `${highlightSql(props.modelValue)}\n`)

function syncScroll() {
  if (!textarea.value || !highlight.value) return
  highlight.value.scrollTop = textarea.value.scrollTop
  highlight.value.scrollLeft = textarea.value.scrollLeft
}

function onInput(e: Event) {
  emit('update:modelValue', (e.target as HTMLTextAreaElement).value)
  requestAnimationFrame(syncScroll)
}
function onTimeoutInput(e: Event) {
  const value = Number((e.target as HTMLInputElement).value)
  emit('update:timeoutSeconds', Number.isFinite(value) ? Math.max(0, value) : 0)
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
      <button class="mini" :disabled="busy" @click="emit('newQuery')">New query</button>
    </div>
    <div class="surface">
      <div class="gutter mono" aria-hidden="true">
        <span v-for="n in lineCount" :key="n">{{ n }}</span>
      </div>
      <div class="code-wrap">
        <pre ref="highlight" class="highlight mono" aria-hidden="true"><code v-html="highlightedSql" /></pre>
        <textarea
          ref="textarea"
          class="code mono"
          :value="modelValue"
          spellcheck="false"
          autocomplete="off"
          autocapitalize="off"
          placeholder="-- write SQL, or pick an example →"
          @input="onInput"
          @keydown="onKeydown"
          @scroll="syncScroll"
        />
      </div>
    </div>
    <div class="actions">
      <label class="timeout mono">
        <span>Timeout</span>
        <input
          type="number"
          min="0"
          step="5"
          :value="timeoutSeconds"
          :disabled="busy"
          @input="onTimeoutInput"
        />
        <span>s</span>
      </label>
      <button class="btn primary" :disabled="busy || !ready" @click="emit('benchmark')">
        <span v-if="busy" class="spin ring" />
        <span>{{ busy ? 'Running' : 'Benchmark' }}</span>
      </button>
      <button class="btn ghost" :disabled="busy || !ready" @click="emit('optimizedOnly')">
        Optimized only
      </button>
      <button class="btn ghost" :disabled="busy || !ready" @click="emit('nativeOnly')">
        Native only
      </button>
      <button v-if="busy" class="btn danger" :disabled="!ready" @click="emit('cancel')">
        Cancel
      </button>
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
.mini {
  border: 1px solid var(--line-strong);
  background: var(--panel-2);
  color: var(--text-dim);
  border-radius: var(--r);
  padding: 5px 9px;
  font-family: var(--font-mono);
  font-size: 10.5px;
  font-weight: 600;
}
.mini:hover:not(:disabled) { color: var(--blue); border-color: var(--blue-line); }
.surface {
  display: flex;
  background: var(--panel-2);
  min-height: 260px;
  min-width: 0;
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
  background: rgba(29, 83, 112, 0.05);
}
.gutter span { padding-right: 12px; }
.code-wrap {
  position: relative;
  flex: 1;
  min-width: 0;
  min-height: 260px;
}
.highlight,
.code {
  position: absolute;
  inset: 0;
  width: 100%;
  border: 0;
  background: transparent;
  padding: 14px 16px;
  font-size: 13px;
  line-height: 1.65;
  tab-size: 2;
  white-space: pre;
  overflow: auto;
}
.highlight {
  margin: 0;
  color: var(--text);
  pointer-events: none;
}
.highlight :deep(.tok-keyword) { color: var(--blue); font-weight: 600; }
.highlight :deep(.tok-string) { color: var(--amber); }
.highlight :deep(.tok-number) { color: var(--green); }
.highlight :deep(.tok-comment), .highlight :deep(.placeholder) { color: var(--text-faint); }
.code {
  resize: none;
  outline: none;
  color: transparent;
  -webkit-text-fill-color: transparent;
  caret-color: var(--amber);
  min-height: 260px;
}
.code::placeholder { color: var(--text-faint); }
.code::selection {
  background: rgba(15, 105, 134, 0.18);
  -webkit-text-fill-color: transparent;
}
.actions { display: flex; flex-wrap: wrap; gap: 10px; padding: 13px 16px; border-top: 1px solid var(--line); }
.timeout {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  color: var(--text-dim);
  font-size: 11px;
}
.timeout input {
  width: 58px;
  border: 1px solid var(--line-strong);
  border-radius: var(--r);
  background: var(--panel-2);
  color: var(--text);
  padding: 8px 7px;
  font-family: var(--font-mono);
  font-size: 12px;
}
.timeout input:disabled { opacity: 0.55; }
.btn {
  display: inline-flex;
  align-items: center;
  gap: 9px;
  border-radius: var(--r);
  padding: 10px 18px;
  font-family: var(--font-mono);
  font-size: 12.5px;
  font-weight: 600;
  letter-spacing: 0.02em;
  border: 1px solid transparent;
  transition: transform 0.08s ease, filter 0.15s ease, background 0.15s ease;
}
.btn:active:not(:disabled) { transform: translateY(1px); }
.btn:disabled { opacity: 0.45; }
.primary {
  color: white;
  background: linear-gradient(180deg, var(--blue), var(--cyan));
  box-shadow: 0 12px 24px -18px rgba(15, 105, 134, 0.8);
}
.primary:hover:not(:disabled) { filter: brightness(1.08); }
.ghost { color: var(--text-dim); border-color: var(--line-strong); background: var(--panel-2); }
.ghost:hover:not(:disabled) { color: var(--text); border-color: var(--slate); }
.danger { color: var(--red); border-color: rgba(200, 79, 60, 0.32); background: rgba(200, 79, 60, 0.08); }
.danger:hover:not(:disabled) { filter: brightness(1.05); }
.ring {
  width: 13px;
  height: 13px;
  border-radius: 99px;
  border: 2px solid rgba(255, 255, 255, 0.4);
  border-top-color: white;
}
</style>
