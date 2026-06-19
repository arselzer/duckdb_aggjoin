<script setup lang="ts">
import { computed, ref } from 'vue'
import { demoResources } from '../data/resources'

const props = defineProps<{ sql: string }>()

const copied = ref(false)

function shellQuote(value: string) {
  return `'${value.replace(/'/g, `'\\''`)}'`
}

const command = computed(() => {
  const clean = props.sql.trim().replace(/;\s*$/, '')
  const query = clean ? `${clean};` : 'SELECT 1;'
  return [
    `tar -xzf ${demoResources.native.file}`,
    `./duckdb -c ${shellQuote(`LOAD aggjoin; ${query}`)}`,
  ].join('\n')
})

async function copyCommand() {
  await navigator.clipboard.writeText(command.value)
  copied.value = true
  window.setTimeout(() => (copied.value = false), 1600)
}
</script>

<template>
  <section class="panel native">
    <div class="bar">
      <span class="eyebrow">native CLI</span>
      <button class="mini" @click="copyCommand">{{ copied ? 'Copied' : 'Copy' }}</button>
    </div>
    <pre class="mono"><code>{{ command }}</code></pre>
  </section>
</template>

<style scoped>
.native { overflow: hidden; }
.bar {
  display: flex; align-items: center; justify-content: space-between; gap: 10px;
  padding: 11px 16px; border-bottom: 1px solid var(--line);
}
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
.mini:hover { color: var(--blue); border-color: var(--blue-line); }
pre {
  margin: 0;
  padding: 13px 16px 15px;
  max-height: 132px;
  overflow: auto;
  color: var(--text-dim);
  font-size: 11.5px;
  line-height: 1.55;
  background: var(--panel-2);
}
</style>

