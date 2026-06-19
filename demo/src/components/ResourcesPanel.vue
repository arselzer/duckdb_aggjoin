<script setup lang="ts">
import { demoResources } from '../data/resources'
</script>

<template>
  <section class="panel resources">
    <div class="bar">
      <span class="eyebrow">downloads</span>
      <span class="hint mono">native + browser runtime</span>
    </div>

    <div class="body">
      <a class="download" :href="demoResources.native.file" download>
        <span>
          <strong>{{ demoResources.native.label }}</strong>
          <small class="mono">{{ demoResources.native.duckdbVersion }} · {{ demoResources.native.platform }}</small>
        </span>
        <span class="tag amber">{{ demoResources.native.size }}</span>
      </a>

      <div class="runtime">
        <div class="runtime-row">
          <span class="label mono">WASM package</span>
          <span class="mono">{{ demoResources.browser.duckdbWasmPackage }}</span>
        </div>
        <div class="runtime-row">
          <span class="label mono">Browser DuckDB</span>
          <span class="mono">{{ demoResources.browser.duckdbVersion }}</span>
        </div>
        <div class="runtime-row">
          <span class="label mono">Extension WASM</span>
          <span class="mono">{{ demoResources.browser.extensionSize }}</span>
        </div>
      </div>
    </div>
  </section>
</template>

<style scoped>
.resources { display: flex; flex-direction: column; }
.bar {
  display: flex; align-items: center; justify-content: space-between; gap: 10px;
  padding: 11px 16px; border-bottom: 1px solid var(--line);
}
.hint { font-size: 10px; color: var(--text-faint); }
.body { padding: 10px; display: grid; gap: 10px; }
.download {
  display: flex; align-items: center; justify-content: space-between; gap: 12px;
  padding: 13px 14px; border-radius: var(--r);
  border: 1px solid var(--amber-line); background: var(--amber-soft);
  color: var(--text);
}
.download:hover { color: var(--text); border-color: var(--amber); background: rgba(211, 138, 19, 0.16); }
.download strong { display: block; font-size: 12.5px; line-height: 1.3; }
.download small { display: block; margin-top: 3px; color: var(--text-dim); font-size: 10.5px; }
.runtime {
  display: grid; gap: 7px;
  padding: 11px 12px; border: 1px solid var(--line); border-radius: var(--r);
  background: var(--ink-2);
}
.runtime-row { display: flex; justify-content: space-between; gap: 12px; font-size: 11px; }
.runtime-row .label { color: var(--text-faint); }
</style>

