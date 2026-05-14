/** @jsxImportSource solid-js */
/**
 * LsmCompaction
 *
 * RocksDB (и ForSt) -- это leveled LSM-tree. Запись попадает в активный
 * memtable, затем флашится в L0 SSTable, дальше background compaction
 * мёрджит SSTable вниз по уровням (L0 → L1 → L2 → L3) с ростом размера
 * в `target_file_size_multiplier` раз на каждом уровне.
 */

import { DiagramContainer } from '@primitives/DiagramContainer';
import { DiagramTooltip } from '@primitives/Tooltip';

interface Sstable {
  id: string;
  size: string;
}

interface Level {
  name: string;
  capacity: string;
  description: string;
  sstables: Sstable[];
  color: string;
}

const LEVELS: Level[] = [
  {
    name: 'L0',
    capacity: '~256MB · overlapping ranges',
    description:
      'SSTables здесь могут перекрываться по key range. Чтение должно проверить ВСЕ файлы L0. Когда количество L0 файлов > level0_file_num_compaction_trigger -- начинается compaction в L1.',
    color: 'bg-rose-500/10 border-rose-400/40 text-rose-700',
    sstables: [
      { id: 'L0-a', size: '64MB' },
      { id: 'L0-b', size: '64MB' },
      { id: 'L0-c', size: '64MB' },
      { id: 'L0-d', size: '64MB' },
    ],
  },
  {
    name: 'L1',
    capacity: '~256MB · non-overlapping',
    description:
      'SSTables не перекрываются. На уровне action — мердж overlapping L0 files в новый L1 file. После этого чтение по ключу делает binary search per level.',
    color: 'bg-amber-500/10 border-amber-400/40 text-amber-700',
    sstables: [
      { id: 'L1-a', size: '64MB' },
      { id: 'L1-b', size: '64MB' },
      { id: 'L1-c', size: '64MB' },
      { id: 'L1-d', size: '64MB' },
    ],
  },
  {
    name: 'L2',
    capacity: '~2.5GB · non-overlapping',
    description:
      'target_file_size_multiplier=10 → каждый следующий уровень в ~10x больше. Большинство данных живёт тут. Compaction пишет амплифицированные данные (write amp = ~10x).',
    color: 'bg-blue-500/10 border-blue-400/40 text-blue-700',
    sstables: Array.from({ length: 10 }, (_, i) => ({
      id: `L2-${i}`,
      size: '256MB',
    })),
  },
  {
    name: 'L3',
    capacity: '~25GB · non-overlapping',
    description:
      'Самый низкий горячий уровень для типовой конфигурации. Здесь -- &quot;холодный хвост&quot; state. Чтение из L3 требует block cache miss → disk read.',
    color: 'bg-emerald-500/10 border-emerald-400/40 text-emerald-700',
    sstables: Array.from({ length: 18 }, (_, i) => ({
      id: `L3-${i}`,
      size: '1.4GB',
    })),
  },
];

export function LsmCompaction() {
  return (
    <DiagramContainer
      title="LSM-tree leveled compaction (RocksDB / ForSt)"
      color="emerald"
      description="Write path: memtable → immutable memtable → L0 SSTable → background compaction. Read path: memtable + bloom filter per SSTable + level scan."
    >
      <div class="flex flex-col gap-3">
        {/* Write entry point */}
        <DiagramTooltip content="Активный memtable в JVM-heap (writeBufferSize). Записи идут как insert/delete-tombstone. При заполнении → switch на immutable + новый активный.">
          <div
            class="rounded-md border border-purple-400/40 bg-purple-500/10 px-3 py-2 text-xs font-mono text-purple-800 self-start"
            tabindex={0}
          >
            memtable (active, in-memory)
            <span class="ml-2 text-[10px] opacity-70">→ flush on full</span>
          </div>
        </DiagramTooltip>

        <div class="text-[var(--ink-muted)] text-center text-lg leading-none">
          ↓ flush
        </div>

        {/* Levels */}
        <div class="flex flex-col gap-2">
          {LEVELS.map((lvl) => (
            <div class="flex flex-col gap-1">
              <div class="flex items-center justify-between">
                <DiagramTooltip content={lvl.description}>
                  <span
                    class={`text-[11px] font-mono px-2 py-0.5 rounded border ${lvl.color}`}
                    tabindex={0}
                  >
                    {lvl.name}
                  </span>
                </DiagramTooltip>
                <span class="text-[10px] font-mono text-[var(--ink-subtle)]">
                  {lvl.capacity}
                </span>
              </div>
              <div class="flex flex-wrap gap-1">
                {lvl.sstables.map((s) => (
                  <DiagramTooltip
                    content={`SSTable ${s.id} · ${s.size}. Immutable, sorted by key. Содержит block index, bloom filter, optional compression.`}
                  >
                    <div
                      class={`px-1.5 py-1 rounded border text-[10px] font-mono ${lvl.color}`}
                      tabindex={0}
                    >
                      {s.id}
                    </div>
                  </DiagramTooltip>
                ))}
              </div>
            </div>
          ))}
        </div>

        {/* Compaction arrows */}
        <div class="grid grid-cols-1 md:grid-cols-2 gap-2 mt-2">
          <div class="p-2 rounded bg-rose-500/10 border border-rose-400/30 text-[11px] text-rose-800 leading-relaxed">
            <span class="font-semibold">Write amplification</span>
            <br />
            Один логический write в memtable рекомпактируется через все уровни.
            Total bytes written ≈ N × original size (N = глубина дерева).
          </div>
          <div class="p-2 rounded bg-emerald-500/10 border border-emerald-400/30 text-[11px] text-emerald-800 leading-relaxed">
            <span class="font-semibold">Read amplification</span>
            <br />
            Worst case = memtable + 1 SSTable per level. Bloom filter режет
            большинство false-positive lookups → реально читаем 1-2 уровня.
          </div>
        </div>
      </div>
    </DiagramContainer>
  );
}
