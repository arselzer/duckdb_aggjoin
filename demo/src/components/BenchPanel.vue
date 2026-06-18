<script setup lang="ts">
import { computed } from 'vue'
import type { BenchResult } from '../engine'

const props = defineProps<{ bench: BenchResult | null; busy: boolean }>()

const fmtMs = (ms: number) => (ms >= 1000 ? `${(ms / 1000).toFixed(2)} s` : `${ms.toFixed(1)} ms`)

const maxMs = computed(() => (props.bench ? Math.max(props.bench.aggjoinMs, props.bench.nativeMs, 0.01) : 1))
const aggPct = computed(() => (props.bench ? Math.max((props.bench.aggjoinMs / maxMs.value) * 100, 2) : 0))
const natPct = computed(() => (props.bench ? Math.max((props.bench.nativeMs / maxMs.value) * 100, 2) : 0))

const speedupLabel = computed(() => {
  if (!props.bench) return ''
  const s = props.bench.speedup
  if (s >= 100) return s.toFixed(0)
  if (s >= 10) return s.toFixed(1)
  return s.toFixed(2)
})
const faster = computed(() => (props.bench?.speedup ?? 0) >= 1.05)

const planLines = computed(() => (props.bench?.plan ? props.bench.plan.split('\n') : []))
</script>

<template>
  <section class="panel bench">
    <div class="bar">
      <span class="eyebrow">benchmark</span>
      <span v-if="bench" class="tag" :class="bench.rowsMatch ? 'green' : 'slate'">
        <span class="dot" />{{ bench.rowsMatch ? 'results match' : 'row counts differ' }}
      </span>
    </div>

    <!-- idle / busy placeholder -->
    <div v-if="!bench" class="idle">
      <div class="scope">
        <div class="sweep" :class="{ live: busy }" />
      </div>
      <p class="mono">{{ busy ? 'measuring native vs aggjoin…' : 'run a query to measure the speedup' }}</p>
    </div>

    <template v-else>
      <div class="headline">
        <div class="multiplier" :class="{ pos: faster }">
          <span class="x">×</span><span class="num">{{ speedupLabel }}</span>
        </div>
        <div class="cap">
          <p class="eyebrow">{{ faster ? 'aggjoin speedup' : 'no speedup on this shape' }}</p>
          <p class="mono small">
            {{ bench.planHasAggjoin ? 'fused AGGJOIN operator fired' : 'native frequency-propagation rewrite' }}
          </p>
        </div>
      </div>

      <div class="bars">
        <div class="row">
          <div class="lbl"><span class="swatch amber" /> AGGJOIN</div>
          <div class="track"><div class="fill amber" :style="{ width: aggPct + '%' }" /></div>
          <div class="val mono">{{ fmtMs(bench.aggjoinMs) }}</div>
        </div>
        <div class="row">
          <div class="lbl"><span class="swatch slate" /> NATIVE</div>
          <div class="track"><div class="fill slate" :style="{ width: natPct + '%' }" /></div>
          <div class="val mono">{{ fmtMs(bench.nativeMs) }}</div>
        </div>
      </div>

      <div v-if="planLines.length" class="plan">
        <div class="plan-head">
          <span class="eyebrow">physical plan · aggjoin on</span>
          <span v-if="bench.planHasAggjoin" class="tag amber">AGGJOIN node</span>
        </div>
        <pre class="mono"><code><span
          v-for="(ln, i) in planLines"
          :key="i"
          class="pln"
          :class="{ hot: /AGGJOIN/i.test(ln) }"
        >{{ ln }}
</span></code></pre>
      </div>
    </template>
  </section>
</template>

<style scoped>
.bench { display: flex; flex-direction: column; }
.bar {
  display: flex; align-items: center; justify-content: space-between;
  padding: 11px 16px; border-bottom: 1px solid var(--line);
}

.idle { padding: 34px 20px 38px; text-align: center; }
.idle p { color: var(--text-faint); font-size: 12px; margin: 18px 0 0; }
.scope {
  position: relative; height: 96px; border-radius: var(--r);
  border: 1px solid var(--line); overflow: hidden;
  background:
    linear-gradient(transparent calc(50% - 1px), var(--amber-line) 50%, transparent calc(50% + 1px)),
    repeating-linear-gradient(90deg, transparent 0 23px, var(--line) 23px 24px),
    var(--ink-2);
}
.sweep {
  position: absolute; top: 0; bottom: 0; width: 2px; left: 0;
  background: var(--amber); box-shadow: 0 0 16px var(--amber); opacity: 0.35;
}
.sweep.live { animation: sweepX 1.4s linear infinite; opacity: 0.9; }
@keyframes sweepX { from { left: -2%; } to { left: 102%; } }

.headline {
  display: flex; align-items: center; gap: 20px;
  padding: 24px 20px 18px;
}
.multiplier {
  display: flex; align-items: baseline;
  font-family: var(--font-display); font-weight: 800;
  color: var(--slate); line-height: 0.85;
}
.multiplier.pos { color: var(--amber); animation: glow 3.5s ease-in-out infinite; }
.multiplier .x { font-size: 26px; opacity: 0.65; margin-right: 2px; }
.multiplier .num { font-size: 64px; letter-spacing: -0.02em; }
.cap .small { font-size: 11.5px; color: var(--text-dim); margin: 5px 0 0; }

.bars { padding: 6px 20px 20px; display: grid; gap: 13px; }
.row { display: grid; grid-template-columns: 92px 1fr 76px; align-items: center; gap: 12px; }
.lbl { display: flex; align-items: center; gap: 8px; font-family: var(--font-mono); font-size: 11px; color: var(--text-dim); }
.swatch { width: 9px; height: 9px; border-radius: 2px; }
.swatch.amber { background: var(--amber); box-shadow: 0 0 8px var(--amber-line); }
.swatch.slate { background: var(--slate); }
.track { height: 16px; background: rgba(0, 0, 0, 0.35); border-radius: 3px; overflow: hidden; border: 1px solid var(--line); }
.fill { height: 100%; transform-origin: left; animation: barGrow 0.7s cubic-bezier(0.16, 1, 0.3, 1) both; }
.fill.amber { background: linear-gradient(90deg, var(--amber), var(--amber-bright)); box-shadow: inset 0 0 12px rgba(255, 198, 58, 0.4); }
.fill.slate { background: repeating-linear-gradient(45deg, var(--slate) 0 8px, #54616b 8px 16px); }
.val { text-align: right; font-size: 12px; color: var(--text); font-weight: 500; }

.plan { border-top: 1px solid var(--line); }
.plan-head { display: flex; align-items: center; justify-content: space-between; padding: 12px 16px 8px; }
.plan pre {
  margin: 0; padding: 4px 16px 18px; max-height: 280px; overflow: auto;
  font-size: 11.5px; line-height: 1.55; color: var(--text-dim);
}
.pln { display: block; white-space: pre; }
.pln.hot {
  color: var(--amber); font-weight: 600;
  background: var(--amber-soft);
  box-shadow: inset 2px 0 0 var(--amber);
}
</style>
