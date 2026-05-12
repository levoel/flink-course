/**
 * ForstDisaggregated
 *
 * ForSt = "For Streaming". Disaggregated state storage: primary copy SSTable
 * лежит в remote object store (S3 / OSS / HDFS), на TaskManager-е держится
 * local cache disk + async I/O thread pool, который подгружает блоки в
 * block cache по требованию.
 */

import { DiagramContainer } from '@primitives/DiagramContainer';
import { DiagramTooltip } from '@primitives/Tooltip';

export function ForstDisaggregated() {
  return (
    <DiagramContainer
      title="ForSt: disaggregated state storage"
      color="blue"
      description="Primary data в object store, local disk -- кэш. Async I/O API позволяет не блокировать оператор на cache miss."
    >
      <div className="flex flex-col gap-4">
        {/* TaskManager */}
        <div className="rounded-lg border border-blue-400/40 bg-blue-500/5 p-3">
          <div className="flex items-center justify-between mb-3">
            <div className="text-xs font-mono text-[var(--ink-strong)]">
              TaskManager (operator subtask)
            </div>
            <div className="text-[10px] font-mono text-[var(--ink-subtle)]">
              JVM + native ForSt
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
            <DiagramTooltip content="Block cache in JVM off-heap (Direct Buffer) или native memory. Hottest blocks. Hit rate определяет p99 latency обработки.">
              <div
                className="rounded-md border border-emerald-400/40 bg-emerald-500/10 px-3 py-2 text-[11px] font-mono text-emerald-800"
                tabIndex={0}
              >
                <div className="font-semibold">Block cache</div>
                <div className="text-[10px] opacity-70 mt-0.5">
                  hot SSTable blocks · LRU
                </div>
              </div>
            </DiagramTooltip>

            <DiagramTooltip content="Async I/O executor — пул потоков, обслуживающий cache miss-ы. Оператор кладёт фьючер в Mailbox и продолжает обрабатывать записи; результат вернётся в operator thread.">
              <div
                className="rounded-md border border-amber-400/40 bg-amber-500/10 px-3 py-2 text-[11px] font-mono text-amber-800"
                tabIndex={0}
              >
                <div className="font-semibold">Async I/O executor</div>
                <div className="text-[10px] opacity-70 mt-0.5">
                  thread pool · futures
                </div>
              </div>
            </DiagramTooltip>

            <DiagramTooltip content="Async-style operator API. Запись попадает в очередь, state.get() возвращает CompletableFuture, оператор не блокируется и продолжает шафлить ключи. Гарантия порядка обеспечивается per-key.">
              <div
                className="rounded-md border border-purple-400/40 bg-purple-500/10 px-3 py-2 text-[11px] font-mono text-purple-800"
                tabIndex={0}
              >
                <div className="font-semibold">Async state API</div>
                <div className="text-[10px] opacity-70 mt-0.5">
                  CompletableFuture get/put
                </div>
              </div>
            </DiagramTooltip>
          </div>
        </div>

        {/* Arrows */}
        <div className="flex items-center justify-center gap-4">
          <div className="flex flex-col items-center">
            <div className="text-[10px] font-mono text-[var(--ink-subtle)]">
              cache miss
            </div>
            <div className="text-[var(--ink-muted)]">↓</div>
          </div>
          <div className="flex flex-col items-center">
            <div className="text-[10px] font-mono text-[var(--ink-subtle)]">
              eviction / spill
            </div>
            <div className="text-[var(--ink-muted)]">↑</div>
          </div>
        </div>

        {/* Local cache disk */}
        <DiagramTooltip content="Local NVMe / EBS. Тёплый набор SSTable. ForSt держит только subset relevant key groups, может быть меньше total state — это и есть disaggregation.">
          <div
            className="rounded-md border border-blue-400/40 bg-blue-500/10 px-3 py-2 text-xs font-mono text-blue-800"
            tabIndex={0}
          >
            <div className="font-semibold">
              Local cache disk (TaskManager local)
            </div>
            <div className="text-[10px] opacity-70 mt-1">
              warm SSTables · GC-managed · может быть меньше total state size
            </div>
          </div>
        </DiagramTooltip>

        <div className="flex justify-center text-[var(--ink-muted)]">↓ async fetch / upload</div>

        {/* Remote primary */}
        <DiagramTooltip content="Authoritative source of truth. Все compaction результаты пишутся сюда. Checkpoint = immutable manifest файлов в этом бакете — без copy-out, без duplicate state.">
          <div
            className="rounded-md border border-[var(--line-thin)] bg-[var(--bg-surface)] px-3 py-3 text-xs font-mono text-[var(--ink-strong)]"
            tabIndex={0}
          >
            <div className="flex items-center justify-between">
              <span className="font-semibold">Remote object store (S3 / OSS)</span>
              <span className="text-[10px] font-mono text-[var(--ink-subtle)]">
                authoritative
              </span>
            </div>
            <div className="text-[10px] opacity-70 mt-1">
              s3://flink-state/{'{job-id}'}/db/ · SSTable files · MANIFEST · CURRENT
            </div>
          </div>
        </DiagramTooltip>

        {/* Wins */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-2 text-[11px] text-[var(--ink-muted)]">
          <div className="p-2 rounded bg-[var(--bg-surface)] border border-[var(--line-thin)]">
            <span className="block font-semibold text-[var(--ink-strong)]">
              Fast rescaling
            </span>
            Новый subtask открывает remote SSTable напрямую — не нужен restore
            из 100GB savepoint.
          </div>
          <div className="p-2 rounded bg-[var(--bg-surface)] border border-[var(--line-thin)]">
            <span className="block font-semibold text-[var(--ink-strong)]">
              Cheap checkpoint
            </span>
            Checkpoint = manifest pointers. Нет повторной заливки SSTable.
          </div>
          <div className="p-2 rounded bg-[var(--bg-surface)] border border-[var(--line-thin)]">
            <span className="block font-semibold text-[var(--ink-strong)]">
              Cost
            </span>
            Cache miss latency (десятки ms по сети). Hide-ится async API +
            block cache hit rate.
          </div>
        </div>
      </div>
    </DiagramContainer>
  );
}
