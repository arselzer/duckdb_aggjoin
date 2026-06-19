<script setup lang="ts">
import { computed } from 'vue'
import type { BenchResult } from '../engine'

const props = defineProps<{ bench: BenchResult | null; busy: boolean }>()

const fmtMs = (ms: number) => (ms >= 1000 ? `${(ms / 1000).toFixed(2)} s` : `${ms.toFixed(1)} ms`)

const isBenchmark = computed(() => props.bench?.mode === 'benchmark')
const isOptimizedOnly = computed(() => props.bench?.mode === 'optimized')
const isNativeOnly = computed(() => props.bench?.mode === 'native')
const maxMs = computed(() => {
  if (!props.bench) return 1
  return Math.max(...[props.bench.aggjoinMs, props.bench.nativeMs, 0.01].filter((ms): ms is number => ms !== null))
})
const aggPct = computed(() => (props.bench?.aggjoinMs !== null && props.bench?.aggjoinMs !== undefined ? Math.max((props.bench.aggjoinMs / maxMs.value) * 100, 2) : 0))
const natPct = computed(() => (props.bench?.nativeMs !== null && props.bench?.nativeMs !== undefined ? Math.max((props.bench.nativeMs / maxMs.value) * 100, 2) : 0))

const speedupLabel = computed(() => {
  if (!props.bench || props.bench.speedup === null) return ''
  const s = props.bench.speedup
  if (s >= 100) return s.toFixed(0)
  if (s >= 10) return s.toFixed(1)
  return s.toFixed(2)
})
const faster = computed(() => isBenchmark.value && (props.bench?.speedup ?? 0) >= 1.05)
const slower = computed(() => isBenchmark.value && (props.bench?.speedup ?? 1) < 0.95)
const primaryMs = computed(() => props.bench?.mode === 'native' ? props.bench.nativeMs : props.bench?.aggjoinMs)
const primaryMsLabel = computed(() => typeof primaryMs.value === 'number' ? fmtMs(primaryMs.value) : 'n/a')
const aggMsLabel = computed(() => typeof props.bench?.aggjoinMs === 'number' ? fmtMs(props.bench.aggjoinMs) : '')
const nativeMsLabel = computed(() => typeof props.bench?.nativeMs === 'number' ? fmtMs(props.bench.nativeMs) : '')
const headlineLabel = computed(() => {
  if (!props.bench) return ''
  if (props.bench.mode === 'benchmark') return faster.value ? 'aggjoin speedup' : 'no speedup on this shape'
  if (props.bench.mode === 'optimized') return 'optimized runtime'
  return 'native runtime'
})

const planLines = computed(() => (props.bench?.plan ? props.bench.plan.split('\n') : []))
const nativePlanLines = computed(() => (props.bench?.nativePlan ? props.bench.nativePlan.split('\n') : []))

const rewriteLabel = computed(() => {
  if (props.bench?.mode === 'native') return 'extension optimizer disabled'
  const marker = props.bench?.rewrite
  if (props.bench?.planHasAggjoin || marker === 'fused') return 'fused AGGJOIN operator fired'
  if (marker === 'agg_propagation') return 'aggregate-propagation native rewrite'
  if (marker === 'final_bag') return 'final-bag native rewrite'
  if (marker === 'native_build') return 'build-side native rewrite'
  if (marker === 'native_mixed') return 'mixed-side native rewrite'
  if (marker && marker !== 'none') return `${marker.replace(/_/g, ' ')} rewrite`
  return 'native plan'
})

const decision = computed(() => {
  const bench = props.bench
  if (!bench) return null
  if (bench.mode === 'native') {
    return {
      tone: 'slate',
      title: 'Native-only run',
      text: 'The extension optimizer was disabled. No optimized path or speedup was measured.',
      facts: ['extension off', `rows=${bench.result.rowCount.toLocaleString()}`],
    }
  }
  const marker = bench.rewrite || 'none'
  if (bench.planHasAggjoin || marker === 'fused') {
    return {
      tone: 'amber',
      title: 'Fused operator',
      text: bench.mode === 'optimized'
        ? 'The extension selected a physical AGGJOIN operator. Native baseline was skipped.'
        : 'The extension selected a physical AGGJOIN operator for the optimized run.',
      facts: [`marker=${marker}`, 'optimized plan contains AGGJOIN', `rows=${bench.result.rowCount.toLocaleString()}`],
    }
  }
  if (marker === 'agg_propagation') {
    return {
      tone: 'cyan',
      title: 'Aggregate propagation',
      text: 'The logical rewrite fired and lowered back to native DuckDB operators.',
      facts: [`marker=${marker}`, 'no physical AGGJOIN node expected', `rows=${bench.result.rowCount.toLocaleString()}`],
    }
  }
  if (marker && marker !== 'none') {
    return {
      tone: 'cyan',
      title: 'Native logical rewrite',
      text: 'The extension rewrote the query into an alternative native DuckDB plan.',
      facts: [`marker=${marker}`, `rows=${bench.result.rowCount.toLocaleString()}`],
    }
  }
  return {
    tone: 'slate',
    title: 'Native DuckDB',
    text: bench.mode === 'optimized'
      ? 'No aggjoin rewrite marker was recorded. Native baseline was skipped.'
      : 'No aggjoin rewrite marker was recorded for the optimized run.',
    facts: bench.mode === 'optimized'
      ? ['marker=none', 'native baseline skipped', `rows=${bench.result.rowCount.toLocaleString()}`]
      : ['marker=none', 'extension plan and native baseline are both DuckDB plans', `rows=${bench.result.rowCount.toLocaleString()}`],
  }
})
</script>

<template>
  <section class="panel bench">
    <div class="bar">
      <span class="eyebrow">{{ isBenchmark ? 'benchmark' : 'run result' }}</span>
      <span v-if="bench && isBenchmark" class="tag" :class="bench.rowsMatch ? 'green' : 'slate'">
        <span class="dot" />{{ bench.rowsMatch ? 'results match' : 'row counts differ' }}
      </span>
      <span v-else-if="bench && isOptimizedOnly" class="tag amber">optimized only</span>
      <span v-else-if="bench && isNativeOnly" class="tag slate">native only</span>
    </div>

    <!-- idle / busy placeholder -->
      <div v-if="!bench" class="idle">
        <div class="plain-loader" :class="{ live: busy }">
          <span v-if="busy" class="spin ring" />
        <span class="mono">{{ busy ? 'Running query' : 'Run a query to inspect or compare plans' }}</span>
      </div>
    </div>

    <template v-else>
      <div class="headline">
        <div v-if="isBenchmark" class="multiplier" :class="{ pos: faster, neg: slower }">
          <span class="x">×</span><span class="num">{{ speedupLabel }}</span>
        </div>
        <div v-else class="runtime" :class="{ native: isNativeOnly }">
          <span class="num">{{ primaryMsLabel }}</span>
        </div>
        <div class="cap">
          <p class="eyebrow">{{ headlineLabel }}</p>
          <p class="mono small">
            {{ rewriteLabel }}
          </p>
        </div>
      </div>

      <div class="bars">
        <div v-if="bench.aggjoinMs !== null" class="row">
          <div class="lbl"><span class="swatch amber" /> {{ isBenchmark ? 'AGGJOIN' : 'OPTIMIZED' }}</div>
          <div class="track"><div class="fill amber" :style="{ width: aggPct + '%' }" /></div>
          <div class="val mono">{{ aggMsLabel }}</div>
        </div>
        <div v-if="bench.nativeMs !== null" class="row">
          <div class="lbl"><span class="swatch slate" /> NATIVE</div>
          <div class="track"><div class="fill slate" :style="{ width: natPct + '%' }" /></div>
          <div class="val mono">{{ nativeMsLabel }}</div>
        </div>
      </div>

      <div v-if="decision" class="decision">
        <div class="decision-copy">
          <span class="eyebrow">planner decision</span>
          <strong>{{ decision.title }}</strong>
          <p class="mono">{{ decision.text }}</p>
        </div>
        <div class="decision-facts">
          <span v-for="fact in decision.facts" :key="fact" class="tag" :class="decision.tone">{{ fact }}</span>
        </div>
      </div>

      <div v-if="planLines.length || nativePlanLines.length" class="plans">
        <div v-if="planLines.length" class="plan-pane">
          <div class="plan-head">
            <span class="eyebrow">optimized plan</span>
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

        <div v-if="nativePlanLines.length" class="plan-pane">
          <div class="plan-head">
            <span class="eyebrow">native baseline plan</span>
            <span class="tag slate">extension off</span>
          </div>
          <pre class="mono"><code><span
            v-for="(ln, i) in nativePlanLines"
            :key="i"
            class="pln"
          >{{ ln }}
</span></code></pre>
        </div>
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
.plain-loader {
  display: inline-flex; align-items: center; gap: 10px;
  min-height: 44px; padding: 11px 14px; border: 1px solid var(--line);
  border-radius: var(--r); background: var(--panel-2); color: var(--text-faint);
  font-size: 12px;
}
.plain-loader.live { color: var(--text-dim); border-color: rgba(15, 105, 134, 0.24); background: rgba(15, 105, 134, 0.06); }
.ring {
  width: 13px; height: 13px; border-radius: 99px;
  border: 2px solid rgba(15, 105, 134, 0.22);
  border-top-color: var(--blue);
}

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
.multiplier.neg { color: var(--red); }
.multiplier .x { font-size: 26px; opacity: 0.65; margin-right: 2px; }
.multiplier .num { font-size: 64px; letter-spacing: 0; }
.runtime {
  font-family: var(--font-display);
  font-weight: 800;
  color: var(--amber);
  line-height: 0.95;
}
.runtime.native { color: var(--slate); }
.runtime .num { font-size: 46px; letter-spacing: 0; }
.cap .small { font-size: 11.5px; color: var(--text-dim); margin: 5px 0 0; }

.bars { padding: 6px 20px 20px; display: grid; gap: 13px; }
.row { display: grid; grid-template-columns: 92px 1fr 76px; align-items: center; gap: 12px; }
.lbl { display: flex; align-items: center; gap: 8px; font-family: var(--font-mono); font-size: 11px; color: var(--text-dim); }
.swatch { width: 9px; height: 9px; border-radius: 2px; }
.swatch.amber { background: var(--amber); box-shadow: 0 0 8px var(--amber-line); }
.swatch.slate { background: var(--slate); }
.track { height: 16px; background: rgba(29, 83, 112, 0.08); border-radius: 3px; overflow: hidden; border: 1px solid var(--line); }
.fill { height: 100%; transform-origin: left; animation: barGrow 0.7s cubic-bezier(0.16, 1, 0.3, 1) both; }
.fill.amber { background: linear-gradient(90deg, var(--amber), var(--amber-bright)); box-shadow: inset 0 0 12px rgba(255, 198, 58, 0.4); }
.fill.slate { background: repeating-linear-gradient(45deg, var(--slate) 0 8px, #54616b 8px 16px); }
.val { text-align: right; font-size: 12px; color: var(--text); font-weight: 500; }

.decision {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(220px, auto);
  gap: 14px;
  padding: 16px 20px 18px;
  border-top: 1px solid var(--line);
  background: rgba(15, 105, 134, 0.045);
}
.decision-copy strong {
  display: block;
  margin-top: 4px;
  color: var(--text);
  font-size: 14px;
}
.decision-copy p {
  margin: 4px 0 0;
  color: var(--text-dim);
  font-size: 11.5px;
}
.decision-facts {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 7px;
  flex-wrap: wrap;
}

.plans {
  border-top: 1px solid var(--line);
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
}
.plan-pane { min-width: 0; }
.plan-pane + .plan-pane { border-left: 1px solid var(--line); }
.plan-head { display: flex; align-items: center; justify-content: space-between; padding: 12px 16px 8px; }
.plan-pane pre {
  margin: 0; padding: 4px 16px 18px; max-height: clamp(440px, 58vh, 720px); overflow: auto;
  font-size: 11.5px; line-height: 1.55; color: var(--text-dim);
}
.pln { display: block; white-space: pre; }
.pln.hot {
  color: var(--amber); font-weight: 600;
  background: var(--amber-soft);
  box-shadow: inset 2px 0 0 var(--amber);
}

@media (max-width: 640px) {
  .headline { align-items: flex-start; gap: 10px; padding: 20px 16px 14px; }
  .multiplier .x { font-size: 22px; }
  .multiplier .num { font-size: 52px; }
  .runtime .num { font-size: 36px; }
  .bars { padding: 6px 16px 18px; }
  .row { grid-template-columns: 78px 1fr 66px; gap: 8px; }
  .decision { grid-template-columns: 1fr; padding: 15px 16px 17px; }
  .decision-facts { justify-content: flex-start; }
  .plan-head { align-items: flex-start; gap: 8px; flex-wrap: wrap; }
  .plans { grid-template-columns: 1fr; }
  .plan-pane + .plan-pane { border-left: 0; border-top: 1px solid var(--line); }
}
</style>
