/** @jsxImportSource solid-js */
/**
 * AbsBarrierPropagation
 *
 * Asynchronous Barrier Snapshotting (Chandy-Lamport variant). JobManager
 * инжектит barrier N в source-ов. Барьер распространяется через DAG
 * (Source → KeyBy → Window → Sink). На multi-input операторах включается
 * alignment phase: barrier с одного канала буферизирует записи,
 * пока не придут barrier-ы со всех входов.
 */

import { createSignal } from 'solid-js';
import { DiagramContainer } from '@primitives/DiagramContainer';
import { DiagramTooltip } from '@primitives/Tooltip';

type Stage = 'inject' | 'propagate' | 'align' | 'snapshot' | 'complete';

const STAGES: { id: Stage; label: string; description: string }[] = [
  {
    id: 'inject',
    label: '1. inject',
    description:
      'JobManager.CheckpointCoordinator периодически вызывает triggerCheckpoint() на task-ах source. Source emit-ит CheckpointBarrier(id=N) downstream после каждого partition cursor.',
  },
  {
    id: 'propagate',
    label: '2. propagate',
    description:
      'Barrier плывёт через каналы вместе с обычными записями. Single-input операторы (map, filter): получили barrier → сразу snapshot своего state → emit barrier downstream → продолжают обработку.',
  },
  {
    id: 'align',
    label: '3. align',
    description:
      'Multi-input оператор (KeyBy/Window после shuffle): barrier с одного входа пришёл, с других ещё нет → буферизация записей с "опередившего" канала, пока barriers не пришли со всех каналов. Это и есть "alignment".',
  },
  {
    id: 'snapshot',
    label: '4. snapshot',
    description:
      'Когда все barriers пришли → оператор snapshot-ит свой state в state backend (sync trigger + async upload), emit-ит barrier downstream. С unaligned checkpoints этот шаг происходит сразу при первом barrier, in-flight записи бэкапятся в checkpoint.',
  },
  {
    id: 'complete',
    label: '5. complete',
    description:
      'Sink получает barrier → snapshot + ack JobManager. Когда JobManager собрал ack от ВСЕХ task-ов → checkpoint считается completed, JobManager рассылает notifyCheckpointComplete (нужен для 2PC sink-ов).',
  },
];

const PIPELINE = [
  { id: 'src1', label: 'Source A', kind: 'source', x: 5 },
  { id: 'src2', label: 'Source B', kind: 'source', x: 5 },
  { id: 'kb', label: 'KeyBy', kind: 'shuffle', x: 30 },
  { id: 'win', label: 'Window', kind: 'multi', x: 55 },
  { id: 'snk', label: 'Sink', kind: 'sink', x: 80 },
];

export function AbsBarrierPropagation() {
  const [stage, setStage] = createSignal<Stage>('inject');
  const stageIdx = () => STAGES.findIndex((s) => s.id === stage());

  // barrier progress in pipeline %
  const barrierX: Record<Stage, number> = {
    inject: 8,
    propagate: 28,
    align: 50,
    snapshot: 65,
    complete: 90,
  };

  return (
    <DiagramContainer
      title="ABS (Chandy-Lamport): barrier propagation"
      color="purple"
      description="Алгоритм асинхронного снепшота. Click стадию -- двигается barrier по DAG."
    >
      <div class="flex flex-col gap-4">
        {/* Pipeline visualization */}
        <div class="relative h-40 rounded-md border border-[var(--line-thin)] bg-[var(--bg-surface)] overflow-hidden">
          {/* lane lines */}
          <div class="absolute inset-x-3 top-1/3 h-px bg-[var(--line-thin)]" />
          <div class="absolute inset-x-3 top-2/3 h-px bg-[var(--line-thin)]" />

          {/* nodes */}
          {PIPELINE.map((n) => (
            <div
              class={`absolute -translate-x-1/2 -translate-y-1/2 px-2 py-1 rounded border text-[10px] font-mono ${
                n.kind === 'source'
                  ? 'bg-emerald-500/15 border-emerald-400/50 text-emerald-800'
                  : n.kind === 'shuffle'
                    ? 'bg-blue-500/15 border-blue-400/50 text-blue-800'
                    : n.kind === 'multi'
                      ? 'bg-amber-500/15 border-amber-400/50 text-amber-800'
                      : 'bg-purple-500/15 border-purple-400/50 text-purple-800'
              }`}
              style={{
                left: `${n.x}%`,
                top: n.id === 'src1' ? '33%' : n.id === 'src2' ? '67%' : '50%',
              }}
            >
              {n.label}
            </div>
          ))}

          {/* barrier marker */}
          <div
            class="absolute top-2 bottom-2 w-1 rounded-full bg-rose-500 shadow-[0_0_6px_rgba(244,63,94,0.7)] transition-all duration-500"
            style={{ left: `${barrierX[stage()]}%` }}
          />
          <div
            class="absolute top-0 -translate-x-1/2 text-[10px] font-mono text-rose-700 transition-all duration-500"
            style={{ left: `${barrierX[stage()]}%` }}
          >
            barrier N
          </div>

          {/* alignment buffer indicator */}
          {stage() === 'align' && (
            <div class="absolute right-3 bottom-1 text-[10px] font-mono text-amber-700">
              ⏸ alignment buffer (1 input ahead)
            </div>
          )}
        </div>

        {/* Stage selector */}
        <div class="flex flex-wrap gap-1">
          {STAGES.map((s, i) => (
            <DiagramTooltip content={s.description}>
              <button
                type="button"
                onClick={() => setStage(s.id)}
                class={`text-[11px] font-mono px-2 py-1 rounded border transition-colors ${
                  stage() === s.id
                    ? 'bg-purple-500/30 border-purple-400/60 text-purple-800'
                    : i <= stageIdx()
                      ? 'bg-purple-500/10 border-purple-400/30 text-purple-700'
                      : 'bg-[var(--bg-surface)] border-[var(--line-thin)] text-[var(--ink-muted)] hover:text-[var(--ink-default)]'
                }`}
              >
                {s.label}
              </button>
            </DiagramTooltip>
          ))}
        </div>

        {/* Current stage detail */}
        <div class="rounded-md border border-[var(--line-thin)] bg-[var(--bg-surface)] p-3 text-[11px] text-[var(--ink-default)] leading-relaxed">
          <span class="font-semibold text-[var(--ink-strong)] block mb-1">
            {STAGES[stageIdx()].label}
          </span>
          {STAGES[stageIdx()].description}
        </div>

        {/* Aligned vs unaligned note */}
        <div class="grid grid-cols-1 md:grid-cols-2 gap-2 text-[11px] text-[var(--ink-muted)]">
          <div class="p-2 rounded bg-amber-500/10 border border-amber-400/30 text-amber-800">
            <span class="font-semibold block">Aligned checkpoint</span>
            Стабильно, маленький snapshot. Backpressure → alignment timeout →
            фейл checkpoint.
          </div>
          <div class="p-2 rounded bg-emerald-500/10 border border-emerald-400/30 text-emerald-800">
            <span class="font-semibold block">Unaligned checkpoint</span>
            Barrier перескакивает буферы, in-flight данные пишутся в checkpoint.
            Жирнее, но завершается даже при backpressure.
          </div>
        </div>
      </div>
    </DiagramContainer>
  );
}
