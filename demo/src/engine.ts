// DuckDB-WASM engine for the AggJoin demo.
//
// Loads the DuckDB-WASM runtime from a CDN (single-threaded `-eh` bundle, so it
// works on GitHub Pages without COOP/COEP headers) and dynamically loads the
// local `aggjoin.duckdb_extension.wasm` via the `runtime.whereToLoad` override —
// the same mechanism the rewrite-y frontend uses. The extension is unsigned, so
// `allowUnsignedExtensions` + `allow_extensions_metadata_mismatch` are set.
//
// A/B benchmarking runs the SAME query on the SAME data with the optimizer
// enabled, then disabled via `SET disabled_optimizers='extension'` — no reinit,
// no data reload — so the speedup is a clean apples-to-apples measurement.

import * as duckdb from '@duckdb/duckdb-wasm'

// Pinned to the exact npm version whose DuckDB ABI (v1.4.3) the extension was
// compiled against (Emscripten 3.1.71). Do NOT float this — an ABI mismatch
// makes `LOAD aggjoin` fail at runtime.
const DUCKDB_WASM_VERSION = '1.32.0'
const JSDELIVR = `https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@${DUCKDB_WASM_VERSION}/dist`

// The DuckDB engine version behind the pinned npm package (the ABI tag). Official
// autoloadable extensions (parquet, json, …) are served per this version. Our
// whereToLoad override is global, so non-aggjoin extensions must be routed here —
// otherwise read_parquet / read_json autoload would resolve to our local path.
const DUCKDB_VERSION = 'v1.4.3'
const OFFICIAL_EXT_BASE = `https://extensions.duckdb.org/${DUCKDB_VERSION}/wasm_eh/`

export interface QueryResult {
  columns: string[]
  rows: unknown[][]
  rowCount: number
  ms: number
  truncated: boolean
}

export interface BenchResult {
  sql: string
  aggjoinMs: number
  nativeMs: number
  speedup: number
  result: QueryResult
  plan: string
  planHasAggjoin: boolean
  nativeRowCount: number
  rowsMatch: boolean
}

export interface TableInfo {
  name: string
  rows: number
  columns: { name: string; type: string }[]
}

const MAX_DISPLAY_ROWS = 1000

function sanitize(v: unknown): unknown {
  if (v === null || v === undefined) return null
  if (typeof v === 'bigint') return v.toString()
  if (v instanceof Date) return v.toISOString()
  if (v instanceof Uint8Array) return `\\x${Array.from(v.slice(0, 16)).map((b) => b.toString(16).padStart(2, '0')).join('')}${v.length > 16 ? '…' : ''}`
  if (typeof v === 'object') {
    try {
      return JSON.stringify(v, (_k, val) => (typeof val === 'bigint' ? val.toString() : val))
    } catch {
      return String(v)
    }
  }
  return v
}

export class AggJoinEngine {
  private db: duckdb.AsyncDuckDB | null = null
  private conn: duckdb.AsyncDuckDBConnection | null = null
  version = ''
  aggjoinLoaded = false

  /** Idempotent. Loads the runtime + extension and opens an in-memory database. */
  async init(onStatus?: (s: string) => void): Promise<void> {
    if (this.conn) return
    const say = (s: string) => onStatus?.(s)

    say('Fetching DuckDB-WASM runtime…')
    const workerUrl = `${JSDELIVR}/duckdb-browser-eh.worker.js`
    const wasmUrl = `${JSDELIVR}/duckdb-eh.wasm`

    // Directory that serves the extension (ends with `/`). Computed from the
    // app's base URL so it works at any GitHub Pages subpath.
    const extBase = new URL(import.meta.env.BASE_URL, window.location.href).href

    // A worker that installs `whereToLoad` BEFORE importing the official DuckDB
    // worker. `LOAD aggjoin` later calls `whereToLoad("aggjoin")` to get the URL.
    const workerScript =
      `self.runtime={whereToLoad:function(f){` +
      `return f==="aggjoin"` +
      `?${JSON.stringify(extBase)}+"aggjoin.duckdb_extension.wasm"` +
      `:${JSON.stringify(OFFICIAL_EXT_BASE)}+f+".duckdb_extension.wasm";` +
      `}};` +
      `importScripts(${JSON.stringify(workerUrl)});`
    const blobUrl = URL.createObjectURL(new Blob([workerScript], { type: 'application/javascript' }))
    const worker = new Worker(blobUrl)
    URL.revokeObjectURL(blobUrl)

    say('Instantiating engine…')
    this.db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker)
    await this.db.instantiate(wasmUrl)
    await this.db.open({ allowUnsignedExtensions: true })
    this.conn = await this.db.connect()

    try {
      const v = await this.conn.query('SELECT version() AS v')
      this.version = String(v.toArray()[0]?.v ?? '')
    } catch { /* non-fatal */ }

    say('Loading aggjoin extension…')
    try {
      await this.conn.query('SET allow_extensions_metadata_mismatch = true')
      await this.conn.query('LOAD aggjoin')
      await this.conn.query('SELECT 1') // ABI smoke-test: mismatch errors here
      this.aggjoinLoaded = true
    } catch (e) {
      this.aggjoinLoaded = false
      console.error('aggjoin failed to load:', e)
      throw new Error(
        'The aggjoin extension failed to load (likely an ABI mismatch). ' +
          'Ensure aggjoin.duckdb_extension.wasm was built for DuckDB-WASM ' +
          `${DUCKDB_WASM_VERSION} (v1.4.3 / Emscripten 3.1.71).`,
      )
    }
  }

  private c(): duckdb.AsyncDuckDBConnection {
    if (!this.conn) throw new Error('engine not initialized')
    return this.conn
  }

  private toResult(table: any, ms: number): QueryResult {
    const columns: string[] = table.schema.fields.map((f: any) => f.name)
    const total = table.numRows
    const arr = table.toArray()
    const slice = arr.slice(0, MAX_DISPLAY_ROWS)
    const rows = slice.map((row: any) => {
      const obj = row.toJSON ? row.toJSON() : row
      return columns.map((col) => sanitize(obj[col]))
    })
    return { columns, rows, rowCount: total, ms, truncated: total > MAX_DISPLAY_ROWS }
  }

  /** Run a single statement and return its result (timed). */
  async run(sql: string): Promise<QueryResult> {
    const t0 = performance.now()
    const table = await this.c().query(sql)
    const ms = performance.now() - t0
    return this.toResult(table, ms)
  }

  private async setOptimizer(enabled: boolean): Promise<void> {
    // `extension` is the optimizer category aggjoin registers under; disabling
    // it gives a true native baseline on the very same binary + data.
    await this.c().query(
      enabled ? "SET disabled_optimizers=''" : "SET disabled_optimizers='extension'",
    )
  }

  // One warm-up run (cold caches / codegen), then up to `maxRuns` timed runs
  // taking the best. Bails after the first timed run once it exceeds ~700 ms —
  // heavy queries (e.g. a real graph join) don't need three samples and the wait
  // would hurt the demo more than the extra precision helps.
  private async measureAdaptive(sql: string, maxRuns: number): Promise<{ ms: number; rowCount: number }> {
    await this.c().query(sql) // warm
    let best = Infinity
    let rowCount = 0
    for (let i = 0; i < maxRuns; i++) {
      const t0 = performance.now()
      const table = await this.c().query(sql)
      const ms = performance.now() - t0
      best = Math.min(best, ms)
      rowCount = table.numRows
      if (ms > 700) break
    }
    return { ms: best, rowCount }
  }

  /** Run `sql` with aggjoin on, then native; report both + the plan. */
  async benchmark(sql: string, runs = 3): Promise<BenchResult> {
    const clean = sql.trim().replace(/;\s*$/, '')

    // --- aggjoin ON ---
    await this.setOptimizer(true)
    const agg = await this.measureAdaptive(clean, runs)
    const display = await this.run(clean)

    // EXPLAIN (optimizer still on) — show the physical plan.
    let plan = ''
    try {
      const ex = await this.c().query(`EXPLAIN ${clean}`)
      plan = ex
        .toArray()
        .map((r: any) => (r.toJSON ? r.toJSON() : r))
        .map((o: any) => o.explain_value ?? '')
        .join('\n')
        .trim()
    } catch { /* EXPLAIN can fail on some statements; non-fatal */ }

    // --- native (aggjoin OFF) ---
    await this.setOptimizer(false)
    let nat: { ms: number; rowCount: number }
    try {
      nat = await this.measureAdaptive(clean, runs)
    } finally {
      await this.setOptimizer(true) // always restore
    }

    return {
      sql: clean,
      aggjoinMs: agg.ms,
      nativeMs: nat.ms,
      speedup: agg.ms > 0 ? nat.ms / agg.ms : 0,
      result: display,
      plan,
      planHasAggjoin: /AGGJOIN/i.test(plan),
      nativeRowCount: nat.rowCount,
      rowsMatch: nat.rowCount === display.rowCount,
    }
  }

  // ---- data import ----------------------------------------------------------

  private static tableNameFor(filename: string): string {
    const base = filename.replace(/\.[^.]+$/, '')
    const name = base.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^(\d)/, '_$1')
    return name || 'imported'
  }

  /** Register a CSV / Parquet / JSON file and materialise it as a table. */
  async importFile(file: File): Promise<TableInfo> {
    if (!this.db) throw new Error('engine not initialized')
    const bytes = new Uint8Array(await file.arrayBuffer())
    const ext = (file.name.split('.').pop() ?? '').toLowerCase()

    if (ext === 'sql') {
      await this.runScript(new TextDecoder().decode(bytes))
      const tables = await this.listTables()
      return tables[tables.length - 1] ?? { name: '(script)', rows: 0, columns: [] }
    }

    const table = AggJoinEngine.tableNameFor(file.name)
    const vfsName = `${table}__${ext}`
    await this.db.registerFileBuffer(vfsName, bytes)

    let reader: string
    if (ext === 'parquet') reader = `read_parquet('${vfsName}')`
    else if (ext === 'json' || ext === 'ndjson') reader = `read_json_auto('${vfsName}')`
    else if (ext === 'tsv') reader = `read_csv('${vfsName}', delim='\t', auto_detect=true, header=true)`
    else reader = `read_csv_auto('${vfsName}')` // csv + fallback

    await this.c().query(`CREATE OR REPLACE TABLE "${table}" AS SELECT * FROM ${reader}`)
    return (await this.describeTable(table))
  }

  /** Fetch a bundled Parquet file and materialise it as a table. */
  async attachParquet(url: string, table: string, onStatus?: (s: string) => void): Promise<TableInfo> {
    if (!this.db) throw new Error('engine not initialized')
    onStatus?.('downloading dataset…')
    const resp = await fetch(url)
    if (!resp.ok) throw new Error(`failed to fetch ${url} (HTTP ${resp.status})`)
    const bytes = new Uint8Array(await resp.arrayBuffer())
    onStatus?.('loading into DuckDB…')
    const vfs = `${table}.parquet`
    await this.db.registerFileBuffer(vfs, bytes)
    await this.c().query(`CREATE OR REPLACE TABLE "${table}" AS SELECT * FROM read_parquet('${vfs}')`)
    return this.describeTable(table)
  }

  /** Execute a multi-statement SQL script (e.g. a dump). */
  async runScript(text: string): Promise<{ statements: number }> {
    const statements = splitSql(text)
    let n = 0
    for (const stmt of statements) {
      const s = stmt.trim()
      if (!s) continue
      try {
        await this.c().query(s)
        n++
      } catch (e: any) {
        const head = s.length > 120 ? s.slice(0, 120) + '…' : s
        throw new Error(`Statement ${n + 1} failed: ${e?.message ?? e}\n\n${head}`)
      }
    }
    return { statements: n }
  }

  async listTables(): Promise<TableInfo[]> {
    const t = await this.c().query(
      "SELECT table_name AS n FROM information_schema.tables WHERE table_schema='main' ORDER BY 1",
    )
    const names = t.toArray().map((r: any) => String((r.toJSON ? r.toJSON() : r).n))
    const out: TableInfo[] = []
    for (const name of names) out.push(await this.describeTable(name))
    return out
  }

  private async describeTable(name: string): Promise<TableInfo> {
    const cols = await this.c().query(`PRAGMA table_info('${name}')`)
    const columns = cols.toArray().map((r: any) => {
      const o = r.toJSON ? r.toJSON() : r
      return { name: String(o.name), type: String(o.type) }
    })
    let rows = 0
    try {
      const cnt = await this.c().query(`SELECT count(*) AS c FROM "${name}"`)
      rows = Number((cnt.toArray()[0] as any)?.c ?? 0)
    } catch { /* ignore */ }
    return { name, rows, columns }
  }

  async dropTable(name: string): Promise<void> {
    await this.c().query(`DROP TABLE IF EXISTS "${name}"`)
  }
}

/**
 * Split a SQL script into top-level statements, respecting single/double quotes,
 * dollar-quotes, line comments (`--`) and block comments. Good enough for the
 * dumps a demo ingests; not a full SQL parser.
 */
export function splitSql(text: string): string[] {
  const out: string[] = []
  let cur = ''
  let i = 0
  const n = text.length
  let quote: string | null = null // "'" | '"' | dollar tag
  while (i < n) {
    const ch = text[i]
    const two = text.slice(i, i + 2)
    if (quote === null) {
      if (two === '--') {
        const nl = text.indexOf('\n', i)
        i = nl === -1 ? n : nl
        continue
      }
      if (two === '/*') {
        const end = text.indexOf('*/', i + 2)
        i = end === -1 ? n : end + 2
        continue
      }
      if (ch === "'" || ch === '"') {
        quote = ch
        cur += ch
        i++
        continue
      }
      const dollar = /^\$[a-zA-Z0-9_]*\$/.exec(text.slice(i))
      if (dollar) {
        quote = dollar[0]
        cur += dollar[0]
        i += dollar[0].length
        continue
      }
      if (ch === ';') {
        out.push(cur)
        cur = ''
        i++
        continue
      }
      cur += ch
      i++
    } else if (quote === "'" || quote === '"') {
      cur += ch
      if (ch === quote) {
        // doubled quote = escape, stay in string
        if (text[i + 1] === quote) {
          cur += quote
          i += 2
          continue
        }
        quote = null
      }
      i++
    } else {
      // dollar-quote: look for the matching closing tag
      if (text.slice(i, i + quote.length) === quote) {
        cur += quote
        i += quote.length
        quote = null
        continue
      }
      cur += ch
      i++
    }
  }
  if (cur.trim()) out.push(cur)
  return out
}
