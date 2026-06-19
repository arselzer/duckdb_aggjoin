export interface SavedSession {
  id: string
  name: string
  sql: string
  updatedAt: number
}

export interface BenchHistoryEntry {
  id: string
  at: number
  sql: string
  rewrite: string
  speedup: number
  aggjoinMs: number
  nativeMs: number
  rowCount: number
}

