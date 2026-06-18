# AggJoin — in-browser demo

A self-contained Vue 3 + DuckDB-WASM application that benchmarks the **aggjoin**
DuckDB extension live in the browser. It loads the extension dynamically, runs a
query with the optimizer **on** and then **off** on the *same* data, and shows
the speedup, the physical plan (with the `AGGJOIN` node highlighted), and the
results. You can also import your own CSV / TSV / Parquet / JSON files or `.sql`
dumps — everything stays client-side.

## How it works

- **Runtime**: DuckDB-WASM `1.32.0` (the single-threaded `-eh` bundle, loaded
  from jsDelivr). The `-eh` bundle needs **no** `COOP`/`COEP` headers, so it runs
  on GitHub Pages as-is.
- **Extension load**: a Worker is created from a blob that sets
  `self.runtime.whereToLoad` before importing the official DuckDB worker; a later
  `LOAD aggjoin` resolves to `public/aggjoin.duckdb_extension.wasm` via that hook.
  The extension is unsigned, so `allowUnsignedExtensions` +
  `allow_extensions_metadata_mismatch` are set. Because `whereToLoad` is a *global*
  override, any other extension DuckDB autoloads (e.g. `parquet` / `json` for
  imports, or the bundled-dataset example) is routed to the official
  `extensions.duckdb.org/v1.4.3/wasm_eh` CDN; only `aggjoin` is served locally.
- **A/B benchmark**: the same query runs with the optimizer enabled, then with
  `SET disabled_optimizers='extension'` (a true native baseline on the same
  binary and data — no reinit, no reload). Best-of-N timing each side.

> **ABI lock.** `public/aggjoin.duckdb_extension.wasm` must be built for the
> exact DuckDB-WASM version pinned in `src/engine.ts` (v1.4.3 ABI,
> Emscripten 3.1.71). Floating the npm version will break `LOAD aggjoin`. Rebuild
> the extension with `make wasm` in the repo root and copy the artifact here.

## Develop

```bash
cd demo
npm install
npm run dev      # http://localhost:4180
```

## Build

```bash
npm run build    # type-checks, then emits ./dist (relative base — portable)
npm run preview  # serve ./dist locally
```

## Deploy to GitHub Pages

A workflow at `../.github/workflows/deploy-pages.yml` builds `demo/` and publishes
`demo/dist` on every push to `main`. Enable it once:

1. **Settings → Pages → Build and deployment → Source: GitHub Actions.**
2. Push to `main` (or run the workflow manually). The site appears at
   `https://<user>.github.io/<repo>/`.

The Vite `base` is `./` (relative), so the bundle works at any subpath without
hard-coding the repo name. Override with `VITE_BASE=/myrepo/ npm run build` if you
prefer an absolute base.

## Updating the bundled extension

```bash
# from the repo root
make wasm
cp frontend/...  # or wherever build_wasm.sh deploys it
cp <built>/aggjoin.duckdb_extension.wasm demo/public/aggjoin.duckdb_extension.wasm
```

## Layout

```
demo/
  index.html
  src/
    engine.ts               # load runtime + extension, A/B benchmark, import, SQL split
                            # (kept out of a duckdb/ dir — the repo .gitignore ignores "duckdb")
    data/examples.ts        # curated self-contained example queries
    components/             # header, SQL editor, benchmark readout, results, import, schema
    App.vue
    style.css               # the instrument-panel design system
  public/
    aggjoin.duckdb_extension.wasm   # the 520 KB extension (ABI-pinned)
    dblp.parquet                    # 3.3 MB SNAP com-DBLP graph (the bundled-dataset example)
```

The `dblp.parquet` dataset is regenerated from the SNAP edge list with
`../scripts/prepare_dblp_parquet.sh` (then copied into `public/`).
