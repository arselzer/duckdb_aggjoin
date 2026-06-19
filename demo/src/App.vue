<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { AggJoinEngine, type BenchResult, type ExportFormat, type QueryResult, type TableInfo } from './engine'
import { examples, type Example } from './data/examples'
import type { BenchHistoryEntry, SavedSession } from './data/workspace'
import AppHeader from './components/AppHeader.vue'
import SqlPanel from './components/SqlPanel.vue'
import BenchPanel from './components/BenchPanel.vue'
import ResultsTable from './components/ResultsTable.vue'
import ImportPanel from './components/ImportPanel.vue'
import ExamplesPanel from './components/ExamplesPanel.vue'
import SchemaPanel from './components/SchemaPanel.vue'
import ResourcesPanel from './components/ResourcesPanel.vue'
import NativeCommandPanel from './components/NativeCommandPanel.vue'
import WorkspacePanel from './components/WorkspacePanel.vue'

const engine = new AggJoinEngine()

const SQL_KEY = 'aggjoin.demo.sql'
const SESSIONS_KEY = 'aggjoin.demo.sessions'
const HISTORY_KEY = 'aggjoin.demo.history'
const TIMEOUT_KEY = 'aggjoin.demo.timeoutSeconds'
const MAX_HISTORY = 24
const MAX_SESSIONS = 24

function readJson<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key)
    return raw ? (JSON.parse(raw) as T) : fallback
  } catch {
    return fallback
  }
}

function writeJson<T>(key: string, value: T) {
  localStorage.setItem(key, JSON.stringify(value))
}

function makeId() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
}

function sessionName(text: string) {
  const firstSqlLine = text
    .split('\n')
    .map((line) => line.trim())
    .find((line) => line && !line.startsWith('--'))
  return (firstSqlLine ?? 'SQL query').replace(/\s+/g, ' ').slice(0, 72)
}

const ready = ref(false)
const bootStatus = ref('starting…')
const bootError = ref<string | null>(null)
const version = ref('')
const aggjoinLoaded = ref(false)

const restoredSql = readJson<string | null>(SQL_KEY, null)
const sql = ref(restoredSql ?? '-- pick an example to the left, or import data, then benchmark it.\nSELECT 1;')
const busy = ref(false)
const importing = ref(false)
const exporting = ref(false)
const bench = ref<BenchResult | null>(null)
const result = ref<QueryResult | null>(null)
const error = ref<string | null>(null)
const tables = ref<TableInfo[]>([])
const activeExampleId = ref<string | null>(null)
const timeoutSeconds = ref(readJson<number>(TIMEOUT_KEY, 30))
const sessions = ref<SavedSession[]>(readJson<SavedSession[]>(SESSIONS_KEY, []))
const history = ref<BenchHistoryEntry[]>(readJson<BenchHistoryEntry[]>(HISTORY_KEY, []))

const toast = ref<{ msg: string; kind: 'ok' | 'err' } | null>(null)
let toastTimer: number | undefined
function notify(msg: string, kind: 'ok' | 'err' = 'ok') {
  toast.value = { msg, kind }
  clearTimeout(toastTimer)
  toastTimer = window.setTimeout(() => (toast.value = null), 3800)
}

async function refreshTables() {
  try {
    tables.value = await engine.listTables()
  } catch { /* ignore */ }
}

onMounted(async () => {
  try {
    await engine.init((s) => (bootStatus.value = s))
    version.value = engine.version
    aggjoinLoaded.value = engine.aggjoinLoaded
    ready.value = true
    if (restoredSql) {
      await refreshTables()
    } else {
      // Warm first impression: build + benchmark the lead example immediately.
      await loadExample(examples[0], { silent: true })
    }
  } catch (e: any) {
    bootError.value = e?.message ?? String(e)
  }
})

watch(sql, (value) => {
  localStorage.setItem(SQL_KEY, JSON.stringify(value))
})

watch(sessions, (value) => writeJson(SESSIONS_KEY, value), { deep: true })
watch(history, (value) => writeJson(HISTORY_KEY, value), { deep: true })
watch(timeoutSeconds, (value) => writeJson(TIMEOUT_KEY, value))

function queryTimeoutMs() {
  const seconds = Number(timeoutSeconds.value)
  return Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : undefined
}

async function loadExample(ex: Example, opts: { silent?: boolean } = {}) {
  if (busy.value) return
  activeExampleId.value = ex.id
  busy.value = true
  error.value = null
  try {
    if (ex.dataset) {
      const datasets = Array.isArray(ex.dataset) ? ex.dataset : [ex.dataset]
      if (!opts.silent) {
        notify(`loading ${datasets.length === 1 ? datasets[0].file : `${datasets.length} parquet files`}…`)
      }
      for (const dataset of datasets) {
        const url = new URL(import.meta.env.BASE_URL + dataset.file, window.location.href).href
        await engine.attachParquet(url, dataset.table)
      }
    }
    await engine.runScript(ex.setup)
    sql.value = ex.query
    await refreshTables()
    if (ex.autoBenchmark === false) {
      bench.value = null
      result.value = null
      if (!opts.silent) notify(`loaded “${ex.title}” · use Optimized only or Benchmark`)
    } else {
      if (!opts.silent) notify(`built data for “${ex.title}”`)
      await runBenchmark()
    }
  } catch (e: any) {
    error.value = e?.message ?? String(e)
    bench.value = null
    if (!opts.silent) notify('failed to build example', 'err')
  } finally {
    busy.value = false
  }
}

async function runBenchmark() {
  if (!ready.value) return
  busy.value = true
  error.value = null
  try {
    const b = await engine.benchmark(sql.value, 3, queryTimeoutMs())
    bench.value = b
    result.value = b.result
    rememberBenchmark(b)
    if (b.rowsMatch === false) notify('aggjoin and native returned different row counts', 'err')
  } catch (e: any) {
    error.value = e?.message ?? String(e)
    bench.value = null
    result.value = null
  } finally {
    busy.value = false
  }
}

async function runOptimizedOnly() {
  if (!ready.value) return
  busy.value = true
  error.value = null
  try {
    const b = await engine.runOptimized(sql.value, 3, queryTimeoutMs())
    bench.value = b
    result.value = b.result
  } catch (e: any) {
    error.value = e?.message ?? String(e)
    bench.value = null
    result.value = null
  } finally {
    busy.value = false
  }
}

async function runNativeOnly() {
  if (!ready.value) return
  busy.value = true
  error.value = null
  try {
    const b = await engine.runNative(sql.value, 3, queryTimeoutMs())
    bench.value = b
    result.value = b.result
  } catch (e: any) {
    error.value = e?.message ?? String(e)
    bench.value = null
    result.value = null
  } finally {
    busy.value = false
  }
}

async function cancelQuery() {
  const cancelled = await engine.cancelActiveQuery()
  notify(cancelled ? 'cancel requested' : 'cancel not available for this query', cancelled ? 'ok' : 'err')
}

function rememberBenchmark(b: BenchResult) {
  if (b.mode !== 'benchmark' || b.speedup === null || b.aggjoinMs === null || b.nativeMs === null) return
  const entry: BenchHistoryEntry = {
    id: makeId(),
    at: Date.now(),
    sql: b.sql,
    rewrite: b.rewrite || 'none',
    speedup: b.speedup,
    aggjoinMs: b.aggjoinMs,
    nativeMs: b.nativeMs,
    rowCount: b.result.rowCount,
  }
  history.value = [entry, ...history.value].slice(0, MAX_HISTORY)
}

function saveSession() {
  const clean = sql.value.trim()
  if (!clean) return
  const session: SavedSession = {
    id: makeId(),
    name: sessionName(clean),
    sql: clean,
    updatedAt: Date.now(),
  }
  sessions.value = [session, ...sessions.value].slice(0, MAX_SESSIONS)
  notify('query saved')
}

function loadSession(session: SavedSession) {
  sql.value = session.sql
  activeExampleId.value = null
  bench.value = null
  result.value = null
  error.value = null
}

function deleteSession(id: string) {
  sessions.value = sessions.value.filter((session) => session.id !== id)
}

function loadHistoryEntry(entry: BenchHistoryEntry) {
  sql.value = entry.sql
  activeExampleId.value = null
  bench.value = null
  result.value = null
  error.value = null
}

function clearHistory() {
  history.value = []
}

async function downloadResult(format: ExportFormat) {
  if (!ready.value || exporting.value) return
  exporting.value = true
  try {
    const exported = await engine.exportQuery(sql.value, format)
    const blob = new Blob([exported.bytes.slice()], { type: exported.mime })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = exported.filename
    document.body.appendChild(link)
    link.click()
    link.remove()
    URL.revokeObjectURL(url)
    notify(`downloaded ${exported.filename}`)
  } catch (e: any) {
    notify('export failed', 'err')
    error.value = e?.message ?? String(e)
  } finally {
    exporting.value = false
  }
}

async function runOnce() {
  if (!ready.value) return
  busy.value = true
  error.value = null
  bench.value = null
  activeExampleId.value = null
  try {
    result.value = await engine.run(sql.value)
  } catch (e: any) {
    error.value = e?.message ?? String(e)
    result.value = null
  } finally {
    busy.value = false
  }
}

async function onFiles(files: File[]) {
  busy.value = true
  importing.value = true
  try {
    let last: TableInfo | null = null
    for (const f of files) {
      last = await engine.importFile(f)
      notify(`imported ${f.name} → ${last.name} (${last.rows.toLocaleString()} rows)`)
    }
    await refreshTables()
    if (last && last.columns.length) {
      sql.value = `SELECT * FROM "${last.name}" LIMIT 100;`
      activeExampleId.value = null
      await runOnce()
    }
  } catch (e: any) {
    error.value = e?.message ?? String(e)
    notify('import failed', 'err')
  } finally {
    importing.value = false
    busy.value = false
  }
}

function useTable(name: string) {
  sql.value = `SELECT * FROM "${name}" LIMIT 100;`
  activeExampleId.value = null
  runOnce()
}

function newQuery() {
  sql.value = '-- write a new aggregate-over-join query\nSELECT 1;'
  activeExampleId.value = null
  bench.value = null
  result.value = null
  error.value = null
}

async function dropTable(name: string) {
  await engine.dropTable(name)
  await refreshTables()
  notify(`dropped ${name}`)
}
</script>

<template>
  <div class="app">
    <AppHeader :version="version" :aggjoin-loaded="aggjoinLoaded" :ready="ready" />

    <!-- boot overlay -->
    <div v-if="!ready" class="boot">
      <div class="boot-card panel reveal">
        <template v-if="!bootError">
          <div class="boot-status mono">
            <span class="spin boot-ring" />
            <span>{{ bootStatus }}</span>
          </div>
          <p class="bn">Booting DuckDB-WASM &amp; the aggjoin extension — all client-side.</p>
        </template>
        <template v-else>
          <p class="be mono">boot failed</p>
          <pre class="bep mono">{{ bootError }}</pre>
        </template>
      </div>
    </div>

    <main v-else class="grid reveal">
      <aside class="rail">
        <ExamplesPanel :active-id="activeExampleId" :busy="busy" @load="loadExample" />
        <ImportPanel :busy="importing" @files="onFiles" />
        <WorkspacePanel
          :sessions="sessions"
          :history="history"
          :busy="busy"
          @save="saveSession"
          @load-session="loadSession"
          @delete-session="deleteSession"
          @load-history="loadHistoryEntry"
          @clear-history="clearHistory"
        />
        <ResourcesPanel />
        <SchemaPanel :tables="tables" @use="useTable" @drop="dropTable" />
      </aside>

      <div class="main">
        <SqlPanel
          v-model="sql"
          v-model:timeout-seconds="timeoutSeconds"
          :busy="busy"
          :ready="ready"
          @benchmark="runBenchmark"
          @optimized-only="runOptimizedOnly"
          @native-only="runNativeOnly"
          @cancel="cancelQuery"
          @new-query="newQuery"
        />
        <NativeCommandPanel :sql="sql" />
        <BenchPanel :bench="bench" :busy="busy" />
        <ResultsTable :result="result" :error="error" :export-busy="exporting" @export="downloadResult" />
      </div>
    </main>

    <footer class="foot">
      <span class="mono">aggjoin · a DuckDB optimizer extension for aggregate-over-join</span>
      <span class="mono dim">A/B = same data, optimizer toggled via <code>SET disabled_optimizers='extension'</code></span>
    </footer>

    <transition name="toast">
      <div v-if="toast" class="toast mono" :class="toast.kind">{{ toast.msg }}</div>
    </transition>
  </div>
</template>

<style scoped>
.app { max-width: 1320px; margin: 0 auto; padding: 0 26px 60px; }

.grid { display: grid; grid-template-columns: 348px 1fr; gap: 18px; margin-top: 20px; align-items: start; }
.rail { display: grid; gap: 18px; position: sticky; top: 18px; min-width: 0; }
.main { display: grid; gap: 18px; min-width: 0; }

@media (max-width: 980px) {
  .grid { grid-template-columns: 1fr; }
  .rail { position: static; }
}

/* boot overlay */
.boot { display: grid; place-items: center; min-height: 56vh; }
.boot-card { width: min(520px, 92vw); padding: 26px; text-align: center; }
.boot-status { display: inline-flex; align-items: center; justify-content: center; gap: 10px; color: var(--blue); font-size: 13px; }
.boot-ring {
  width: 16px; height: 16px; border-radius: 99px;
  border: 2px solid rgba(15, 105, 134, 0.22);
  border-top-color: var(--blue);
}
.bn { margin: 0; color: var(--text-faint); font-size: 12px; }
.be { color: var(--red); margin: 18px 0 8px; }
.bep { color: var(--text-dim); font-size: 11.5px; white-space: pre-wrap; text-align: left; line-height: 1.5; }

.foot {
  margin-top: 30px; padding-top: 18px; border-top: 1px solid var(--line);
  display: flex; align-items: center; justify-content: space-between; gap: 14px; flex-wrap: wrap;
  font-size: 11px; color: var(--text-faint);
}
.foot .dim code { color: var(--text-dim); }

.toast {
  position: fixed; right: 22px; bottom: 22px; z-index: 100;
  padding: 12px 16px; border-radius: var(--r-lg); font-size: 12px;
  background: var(--panel-3); border: 1px solid var(--line-strong);
  box-shadow: var(--shadow); max-width: min(420px, 88vw);
}
.toast.ok { border-left: 3px solid var(--amber); }
.toast.err { border-left: 3px solid var(--red); color: var(--red); }
.toast-enter-active, .toast-leave-active { transition: opacity 0.25s ease, transform 0.25s ease; }
.toast-enter-from, .toast-leave-to { opacity: 0; transform: translateY(10px); }
</style>
