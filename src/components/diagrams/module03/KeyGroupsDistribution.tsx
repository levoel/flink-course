/**
 * KeyGroupsDistribution
 *
 * Flink keyed state shard = "key group". maxParallelism определяет количество
 * key groups (по умолчанию 128). Каждый subtask владеет contiguous range
 * key groups. При rescaling parallelism 4 → 8 диапазоны переделятся пополам --
 * никакого remapping per-key, только per-range.
 */

import { useState } from 'react';
import { DiagramContainer } from '@primitives/DiagramContainer';
import { DiagramTooltip } from '@primitives/Tooltip';

const MAX_PARALLELISM = 128;

function rangesFor(parallelism: number) {
  // mirrors Flink's KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex
  return Array.from({ length: parallelism }, (_, subtask) => {
    const start = Math.floor((subtask * MAX_PARALLELISM) / parallelism);
    const end = Math.floor(((subtask + 1) * MAX_PARALLELISM) / parallelism) - 1;
    return { subtask, start, end, size: end - start + 1 };
  });
}

const PALETTE = [
  'bg-emerald-500/30 border-emerald-400/60',
  'bg-blue-500/30 border-blue-400/60',
  'bg-amber-500/30 border-amber-400/60',
  'bg-rose-500/30 border-rose-400/60',
  'bg-purple-500/30 border-purple-400/60',
  'bg-cyan-500/30 border-cyan-400/60',
  'bg-pink-500/30 border-pink-400/60',
  'bg-lime-500/30 border-lime-400/60',
];

function KeyGroupRow({
  label,
  parallelism,
}: {
  label: string;
  parallelism: number;
}) {
  const ranges = rangesFor(parallelism);
  // mapping key group → subtask
  const ownerOfKg = (kg: number) =>
    ranges.findIndex((r) => kg >= r.start && kg <= r.end);

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <div className="text-xs font-mono text-[var(--ink-strong)]">
          {label}
        </div>
        <div className="text-[10px] font-mono text-[var(--ink-subtle)]">
          parallelism = {parallelism}
        </div>
      </div>

      {/* Subtask legend */}
      <div className="flex flex-wrap gap-1">
        {ranges.map((r) => (
          <DiagramTooltip
            key={r.subtask}
            content={`Subtask ${r.subtask}: key groups [${r.start}..${r.end}] (${r.size} групп). При rescaling этот subtask унаследует state именно из этого диапазона.`}
          >
            <span
              className={`text-[10px] font-mono px-1.5 py-0.5 rounded border ${
                PALETTE[r.subtask % PALETTE.length]
              } text-[var(--ink-strong)]`}
              tabIndex={0}
            >
              s{r.subtask}: [{r.start}..{r.end}]
            </span>
          </DiagramTooltip>
        ))}
      </div>

      {/* 128 key group strip */}
      <div className="flex flex-wrap gap-[2px]">
        {Array.from({ length: MAX_PARALLELISM }, (_, kg) => {
          const owner = ownerOfKg(kg);
          return (
            <div
              key={kg}
              className={`w-[10px] h-[14px] rounded-[2px] border ${PALETTE[owner % PALETTE.length]}`}
              title={`kg=${kg} → subtask ${owner}`}
            />
          );
        })}
      </div>
    </div>
  );
}

export function KeyGroupsDistribution() {
  const [parallelism, setParallelism] = useState<number>(4);

  return (
    <DiagramContainer
      title="Key groups distribution & rescaling (maxParallelism = 128)"
      color="purple"
      description="Один &quot;квадратик&quot; = один key group из 128. Цвет = subtask-владелец. Rescaling — это перерезка соседних range, а не хеш-перешафл."
    >
      <div className="flex flex-col gap-5">
        <KeyGroupRow label="Before (parallelism = 4)" parallelism={4} />
        <KeyGroupRow
          label={`After (parallelism = ${parallelism})`}
          parallelism={parallelism}
        />

        <div className="flex flex-wrap items-center gap-2">
          <span className="text-[11px] font-mono text-[var(--ink-muted)]">
            Try rescaling:
          </span>
          {[2, 4, 8, 16, 32].map((p) => (
            <button
              key={p}
              type="button"
              onClick={() => setParallelism(p)}
              className={`text-[11px] font-mono px-2 py-1 rounded border transition-colors ${
                parallelism === p
                  ? 'bg-purple-500/30 border-purple-400/60 text-purple-800'
                  : 'bg-[var(--bg-surface)] border-[var(--line-thin)] text-[var(--ink-muted)] hover:text-[var(--ink-default)]'
              }`}
            >
              p = {p}
            </button>
          ))}
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-2 text-[11px] text-[var(--ink-muted)]">
          <div className="p-2 rounded bg-[var(--bg-surface)] border border-[var(--line-thin)]">
            <span className="block font-semibold text-[var(--ink-strong)]">
              key → key group
            </span>
            <code>kg = murmurhash(key) % maxParallelism</code>
          </div>
          <div className="p-2 rounded bg-[var(--bg-surface)] border border-[var(--line-thin)]">
            <span className="block font-semibold text-[var(--ink-strong)]">
              key group → subtask
            </span>
            <code>
              subtask = kg * parallelism / maxParallelism
            </code>
          </div>
          <div className="p-2 rounded bg-[var(--bg-surface)] border border-[var(--line-thin)]">
            <span className="block font-semibold text-[var(--ink-strong)]">
              Почему 128 default
            </span>
            128 = верхняя граница parallelism при restore. Выше 128 без resnapshot
            уже нельзя -- maxParallelism замораживается в savepoint.
          </div>
        </div>
      </div>
    </DiagramContainer>
  );
}
