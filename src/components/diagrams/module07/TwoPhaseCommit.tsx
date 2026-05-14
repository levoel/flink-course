/** @jsxImportSource solid-js */
/**
 * TwoPhaseCommit
 *
 * Sink с exactly-once семантикой через 2PC. Sequence:
 *   barrier → preCommit (flush to staging) → notifyCheckpointComplete → commit.
 *
 * Demonstrates три failure scenario:
 *   - crash before preCommit  → rollback in-progress txn
 *   - crash between preCommit & complete → resume + commit on recover
 *   - crash after commit → no-op idempotent
 */

import { createSignal } from 'solid-js';
import { DiagramContainer } from '@primitives/DiagramContainer';
import { DiagramTooltip } from '@primitives/Tooltip';

type Scenario = 'happy' | 'fail-pre' | 'fail-between' | 'fail-after';

interface Step {
  who: 'JM' | 'Op' | 'Sink' | 'Ext';
  label: string;
  tooltip: string;
  variant: 'normal' | 'commit' | 'fail';
}

const SCENARIOS: Record<Scenario, { title: string; steps: Step[] }> = {
  happy: {
    title: 'Happy path',
    steps: [
      {
        who: 'JM',
        label: 'triggerCheckpoint(N)',
        tooltip:
          'CheckpointCoordinator.triggerCheckpoint() инжектит barrier(N) во все source-task-и.',
        variant: 'normal',
      },
      {
        who: 'Op',
        label: 'barrier(N) propagated',
        tooltip:
          'Операторы snapshot-ят state, передают barrier downstream до sink.',
        variant: 'normal',
      },
      {
        who: 'Sink',
        label: 'preCommit(txn=N)',
        tooltip:
          'Sink.snapshotState() → flush буфера в staging transaction (Kafka transactional producer / Iceberg uncommitted snapshot). Транзакция открыта, но НЕ закоммичена.',
        variant: 'normal',
      },
      {
        who: 'Ext',
        label: 'staging txn=N visible only to producer',
        tooltip:
          'External system держит pending transaction. Для Kafka — read_committed consumers её не видят. Для Iceberg — snapshot UUID известен, но не в HEAD.',
        variant: 'normal',
      },
      {
        who: 'JM',
        label: 'all ACKed → notifyCheckpointComplete(N)',
        tooltip:
          'CheckpointCoordinator получил ack от всех task-ов, записал manifest в storage. Шлёт callback notifyCheckpointComplete(N) во все task-и.',
        variant: 'normal',
      },
      {
        who: 'Sink',
        label: 'commit(txn=N)',
        tooltip:
          'Sink.notifyCheckpointComplete() → commits staging txn. Данные становятся видимы downstream consumer-ам. Эта точка определяет EOS-границу.',
        variant: 'commit',
      },
    ],
  },
  'fail-pre': {
    title: 'Crash before preCommit',
    steps: [
      {
        who: 'JM',
        label: 'triggerCheckpoint(N)',
        tooltip: 'Barrier injected.',
        variant: 'normal',
      },
      {
        who: 'Op',
        label: 'barrier propagating...',
        tooltip: 'Часть DAG прошёл barrier.',
        variant: 'normal',
      },
      {
        who: 'Sink',
        label: '✗ task crash before preCommit',
        tooltip:
          'Никакой staging txn не открыт. JobManager отменяет checkpoint N.',
        variant: 'fail',
      },
      {
        who: 'JM',
        label: 'restart from last completed CP (N-1)',
        tooltip:
          'Recover state из checkpoint N-1. В external system нет повисших txn → ничего откатывать не нужно.',
        variant: 'normal',
      },
    ],
  },
  'fail-between': {
    title: 'Crash between preCommit and complete',
    steps: [
      {
        who: 'JM',
        label: 'triggerCheckpoint(N)',
        tooltip: '',
        variant: 'normal',
      },
      {
        who: 'Sink',
        label: 'preCommit(txn=N) OK',
        tooltip:
          'Staging txn открыта. Producer ID и txn id персиснуты в Flink state.',
        variant: 'normal',
      },
      {
        who: 'JM',
        label: 'CP=N completed, sending notify...',
        tooltip:
          'CheckpointCoordinator записал manifest, но не успел доставить notifyCheckpointComplete во все task-и.',
        variant: 'normal',
      },
      {
        who: 'Sink',
        label: '✗ task crash before commit',
        tooltip:
          'Staging txn повисает в external system. Если ничего не делать — duplicates при retry.',
        variant: 'fail',
      },
      {
        who: 'Sink',
        label: 'on recover: recoverAndCommit(stored txn id)',
        tooltip:
          'TwoPhaseCommitSinkFunction в initializeState() читает pending txn list из state и доcommit-ит. Producer ID тот же → Kafka выполнит txn без duplicates.',
        variant: 'commit',
      },
    ],
  },
  'fail-after': {
    title: 'Crash after commit',
    steps: [
      {
        who: 'JM',
        label: 'triggerCheckpoint(N)',
        tooltip: '',
        variant: 'normal',
      },
      {
        who: 'Sink',
        label: 'preCommit(txn=N) OK',
        tooltip: '',
        variant: 'normal',
      },
      {
        who: 'JM',
        label: 'notifyCheckpointComplete(N) sent',
        tooltip: '',
        variant: 'normal',
      },
      {
        who: 'Sink',
        label: 'commit(txn=N) OK',
        tooltip: 'Данные опубликованы external system.',
        variant: 'commit',
      },
      {
        who: 'Sink',
        label: '✗ task crash after commit',
        tooltip:
          'Recover из CP N. recoverAndCommit для txn=N — no-op (idempotent), потому что Kafka producer txn=N уже в Committed state.',
        variant: 'fail',
      },
    ],
  },
};

const WHO_STYLE = {
  JM: 'bg-purple-500/10 border-purple-400/40 text-purple-800',
  Op: 'bg-blue-500/10 border-blue-400/40 text-blue-800',
  Sink: 'bg-emerald-500/10 border-emerald-400/40 text-emerald-800',
  Ext: 'bg-amber-500/10 border-amber-400/40 text-amber-800',
};

const WHO_LABEL: Record<Step['who'], string> = {
  JM: 'JobManager',
  Op: 'Operators',
  Sink: 'Sink task',
  Ext: 'External system',
};

export function TwoPhaseCommit() {
  const [scenario, setScenario] = createSignal<Scenario>('happy');
  const steps = () => SCENARIOS[scenario()].steps;

  return (
    <DiagramContainer
      title="2PC sink: exactly-once via two-phase commit"
      color="emerald"
      description="barrier → preCommit → notifyCheckpointComplete → commit. Failure recovery строится на хранении pending txn id в Flink state."
    >
      <div class="flex flex-col gap-3">
        {/* Scenario tabs */}
        <div class="flex flex-wrap gap-1">
          {(Object.keys(SCENARIOS) as Scenario[]).map((s) => (
            <button
              type="button"
              onClick={() => setScenario(s)}
              class={`text-[11px] font-mono px-2 py-1 rounded border transition-colors ${
                scenario() === s
                  ? 'bg-emerald-500/30 border-emerald-400/60 text-emerald-800'
                  : 'bg-[var(--bg-surface)] border-[var(--line-thin)] text-[var(--ink-muted)] hover:text-[var(--ink-default)]'
              }`}
            >
              {SCENARIOS[s].title}
            </button>
          ))}
        </div>

        {/* Lane headers */}
        <div class="grid grid-cols-4 gap-2 text-[10px] font-mono uppercase tracking-wide text-[var(--ink-subtle)]">
          {(Object.keys(WHO_LABEL) as Step['who'][]).map((w) => (
            <span class="text-center">
              {WHO_LABEL[w]}
            </span>
          ))}
        </div>

        {/* Sequence steps */}
        <div class="flex flex-col gap-1.5">
          {steps().map((s, i) => {
            const col = (Object.keys(WHO_LABEL) as Step['who'][]).indexOf(s.who);
            return (
              <div
                class="grid grid-cols-4 gap-2 items-center"
              >
                {Array.from({ length: 4 }, (_, c) => {
                  if (c !== col) {
                    return (
                      <div
                        class="border-l border-dashed border-[var(--line-thin)] h-full min-h-[36px]"
                      />
                    );
                  }
                  return (
                    <DiagramTooltip content={s.tooltip || s.label}>
                      <div
                        class={`rounded border px-2 py-1.5 text-[11px] font-mono ${WHO_STYLE[s.who]} ${
                          s.variant === 'fail'
                            ? 'ring-1 ring-rose-400/60 bg-rose-500/10 border-rose-400/50 text-rose-800'
                            : s.variant === 'commit'
                              ? 'ring-1 ring-emerald-400/60'
                              : ''
                        }`}
                        tabindex={0}
                      >
                        <span class="opacity-60">{i + 1}. </span>
                        {s.label}
                      </div>
                    </DiagramTooltip>
                  );
                })}
              </div>
            );
          })}
        </div>

        <div class="text-[11px] text-[var(--ink-muted)] mt-1">
          <span class="font-semibold text-[var(--ink-strong)]">
            Ключевая инвариантa:
          </span>{' '}
          commit может произойти только после того, как CheckpointCoordinator
          записал manifest. Если crash после ack-а на manifest, но до commit ---
          recoverAndCommit добивает транзакцию по сохранённому txn id.
        </div>
      </div>
    </DiagramContainer>
  );
}
