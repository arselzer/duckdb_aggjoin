<script setup lang="ts">
import type { BenchHistoryEntry, SavedSession } from '../data/workspace'

defineProps<{
  sessions: SavedSession[]
  history: BenchHistoryEntry[]
  busy: boolean
}>()

const emit = defineEmits<{
  save: []
  loadSession: [SavedSession]
  deleteSession: [string]
  loadHistory: [BenchHistoryEntry]
  clearHistory: []
}>()

function fmtTime(ts: number) {
  return new Date(ts).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}

function markerLabel(marker: string) {
  if (!marker || marker === 'none') return 'native'
  return marker.replace(/_/g, ' ')
}
</script>

<template>
  <section class="panel workspace">
    <div class="bar">
      <span class="eyebrow">workspace</span>
      <button class="mini" :disabled="busy" @click="emit('save')">Save query</button>
    </div>

    <div class="block">
      <div class="block-head">
        <span class="sub mono">saved</span>
        <span class="count mono">{{ sessions.length }}</span>
      </div>
      <ul v-if="sessions.length" class="items">
        <li v-for="session in sessions" :key="session.id">
          <button class="item-main" :disabled="busy" @click="emit('loadSession', session)">
            <strong>{{ session.name }}</strong>
            <span class="mono">{{ fmtTime(session.updatedAt) }}</span>
          </button>
          <button class="icon" :disabled="busy" title="Delete" @click="emit('deleteSession', session.id)">×</button>
        </li>
      </ul>
      <p v-else class="empty mono">no saved queries</p>
    </div>

    <div class="block">
      <div class="block-head">
        <span class="sub mono">history</span>
        <button v-if="history.length" class="link mono" :disabled="busy" @click="emit('clearHistory')">clear</button>
      </div>
      <ul v-if="history.length" class="items">
        <li v-for="entry in history" :key="entry.id">
          <button class="item-main" :disabled="busy" @click="emit('loadHistory', entry)">
            <strong>×{{ entry.speedup.toFixed(entry.speedup >= 10 ? 1 : 2) }} · {{ markerLabel(entry.rewrite) }}</strong>
            <span class="mono">
              {{ fmtTime(entry.at) }} · {{ entry.rowCount.toLocaleString() }} rows
            </span>
          </button>
        </li>
      </ul>
      <p v-else class="empty mono">no benchmark runs</p>
    </div>
  </section>
</template>

<style scoped>
.workspace { display: flex; flex-direction: column; }
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
.mini:hover:not(:disabled), .link:hover:not(:disabled), .icon:hover:not(:disabled) {
  color: var(--blue);
  border-color: var(--blue-line);
}
.block { padding: 10px; border-top: 1px solid var(--line); }
.block:first-of-type { border-top: 0; }
.block-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 7px; }
.sub { font-size: 10.5px; color: var(--text-faint); text-transform: uppercase; letter-spacing: 0.12em; }
.count { font-size: 10.5px; color: var(--text-faint); }
.link {
  border: 0;
  background: transparent;
  color: var(--text-faint);
  font-size: 10.5px;
  padding: 0;
}
.items { list-style: none; margin: 0; padding: 0; display: grid; gap: 7px; }
li { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 6px; align-items: stretch; }
.item-main {
  min-width: 0;
  text-align: left;
  border: 1px solid var(--line);
  background: var(--ink-2);
  color: var(--text);
  border-radius: var(--r);
  padding: 9px 10px;
}
.item-main:hover:not(:disabled) { border-color: var(--slate); background: var(--panel-2); }
.item-main strong {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 11.5px;
  line-height: 1.35;
}
.item-main span { display: block; margin-top: 3px; color: var(--text-faint); font-size: 10px; }
.icon {
  width: 30px;
  border: 1px solid var(--line);
  background: var(--ink-2);
  color: var(--text-faint);
  border-radius: var(--r);
  font-size: 16px;
  line-height: 1;
}
.empty { margin: 0; padding: 9px 2px 3px; color: var(--text-faint); font-size: 11px; }
button:disabled { opacity: 0.5; }
</style>
