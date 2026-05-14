/** @jsxImportSource solid-js */
/**
 * FlinkK8sOperator
 *
 * Apache Flink Kubernetes Operator: control loop вокруг FlinkDeployment CRD.
 * Каждый reconcile cycle: observe (запросить текущее состояние JM/TM/JobStatus)
 * → plan (diff spec vs status, решить какой action) → execute (apply patch,
 * trigger savepoint, restart cluster и т.д.).
 */

import { createSignal } from 'solid-js';
import { DiagramContainer } from '@primitives/DiagramContainer';
import { DiagramTooltip } from '@primitives/Tooltip';

type Phase = 'observe' | 'plan' | 'execute';

const PHASES: { id: Phase; label: string; description: string }[] = [
  {
    id: 'observe',
    label: 'Observe',
    description:
      'Operator опрашивает реальное состояние: FlinkDeployment.status (job state, savepoint info), kube-apiserver (pods/deployments), Flink REST API JobManager (/jobs, /jobs/:id/status, /checkpoints). Обновляет .status.jobStatus + .status.clusterInfo.',
  },
  {
    id: 'plan',
    label: 'Plan',
    description:
      'Diff между .spec (желаемое) и .status (текущее). Возможные actions: scale TaskManagers (replicas), upgrade image (savepoint + redeploy), suspend/resume, restart on FAILED job. План -- conflict-free набор операций.',
  },
  {
    id: 'execute',
    label: 'Execute',
    description:
      'Применяет операции: kubectl-style PATCH на Deployment, REST call для savepoint на JobManager, удаление подов. После execute -- НЕ возвращает результат; пишет события в .status и завершает цикл. Следующий observe увидит результат.',
  },
];

const ACTIONS = [
  {
    spec: 'spec.image: 1.19.0 → 1.19.1',
    plan: 'upgrade job (stateful)',
    flow: [
      'observe: jobStatus = RUNNING',
      'plan: stateful upgrade required',
      'execute: trigger savepoint',
      'execute: delete cluster',
      'execute: deploy 1.19.1, restore from savepoint',
    ],
  },
  {
    spec: 'spec.taskManager.replicas: 2 → 4',
    plan: 'rescale',
    flow: [
      'observe: tm replicas=2, parallelism=4',
      'plan: scale up tm replicas',
      'execute: trigger savepoint (если standalone)',
      'execute: redeploy with replicas=4',
      'execute: resume job from savepoint',
    ],
  },
  {
    spec: 'job FAILED in REST API',
    plan: 'auto-recover',
    flow: [
      'observe: jobStatus = FAILED',
      'plan: check spec.job.restartNonce policy',
      'execute: redeploy from latest checkpoint',
    ],
  },
];

export function FlinkK8sOperator() {
  const [phase, setPhase] = createSignal<Phase>('observe');

  return (
    <DiagramContainer
      title="Flink Kubernetes Operator: reconcile loop"
      color="blue"
      description="Operator pattern из k8s. CRD FlinkDeployment / FlinkSessionJob. Loop работает event-driven (watch CRD) + periodic resync."
    >
      <div class="flex flex-col gap-4">
        {/* Top: CRD + operator */}
        <div class="grid grid-cols-1 md:grid-cols-3 gap-2">
          <DiagramTooltip content="CRD FlinkDeployment. Spec описывает желаемый кластер: image, replicas, jobManager.resource.memory, flinkConfiguration, job.upgradeMode (stateless / savepoint / last-state).">
            <div
              class="rounded-md border border-amber-400/40 bg-amber-500/10 px-3 py-2 text-[11px] font-mono text-amber-800"
              tabindex={0}
            >
              <div class="font-semibold">FlinkDeployment CRD</div>
              <div class="text-[10px] opacity-70 mt-1">
                .spec · .status<br />
                + FlinkSessionJob CRD
              </div>
            </div>
          </DiagramTooltip>

          <DiagramTooltip content="Operator pod в namespace. Watch endpoints на FlinkDeployment, FlinkSessionJob, Deployment, ConfigMap. Каждое событие → enqueue в workqueue → reconcile.">
            <div
              class="rounded-md border border-blue-400/40 bg-blue-500/10 px-3 py-2 text-[11px] font-mono text-blue-800"
              tabindex={0}
            >
              <div class="font-semibold">flink-kubernetes-operator</div>
              <div class="text-[10px] opacity-70 mt-1">
                · informer<br />
                · workqueue<br />
                · reconciler
              </div>
            </div>
          </DiagramTooltip>

          <DiagramTooltip content="Управляемые ресурсы: JobManager Deployment, TaskManager Deployment, ConfigMap, Service, опционально Ingress. Owner reference указывает на FlinkDeployment, поэтому GC чистит всё при удалении CR.">
            <div
              class="rounded-md border border-emerald-400/40 bg-emerald-500/10 px-3 py-2 text-[11px] font-mono text-emerald-800"
              tabindex={0}
            >
              <div class="font-semibold">managed resources</div>
              <div class="text-[10px] opacity-70 mt-1">
                JM Deployment · TM Deployment<br />
                ConfigMap · Service
              </div>
            </div>
          </DiagramTooltip>
        </div>

        {/* Reconcile loop */}
        <div class="rounded-lg border border-[var(--line-thin)] bg-[var(--bg-surface)] p-3">
          <div class="text-xs font-mono text-[var(--ink-strong)] mb-2">
            reconcile() loop
          </div>
          <div class="grid grid-cols-3 gap-2">
            {PHASES.map((p) => (
              <DiagramTooltip content={p.description}>
                <button
                  type="button"
                  onClick={() => setPhase(p.id)}
                  class={`rounded-md border px-3 py-2 text-[11px] font-mono w-full text-left transition-colors ${
                    phase() === p.id
                      ? 'bg-blue-500/30 border-blue-400/60 text-blue-800'
                      : 'bg-[var(--bg-surface)] border-[var(--line-thin)] text-[var(--ink-default)] hover:bg-[var(--bg-deep)]'
                  }`}
                >
                  <div class="font-semibold">{p.label}</div>
                </button>
              </DiagramTooltip>
            ))}
          </div>

          {/* Phase detail */}
          <div class="mt-3 text-[11px] text-[var(--ink-default)] leading-relaxed">
            {PHASES.find((p) => p.id === phase())!.description}
          </div>
        </div>

        {/* Action examples */}
        <div class="rounded-lg border border-[var(--line-thin)] bg-[var(--bg-surface)] p-3">
          <div class="text-xs font-mono text-[var(--ink-strong)] mb-2">
            Common spec changes → reconcile actions
          </div>
          <div class="grid grid-cols-1 md:grid-cols-3 gap-2">
            {ACTIONS.map((a) => (
              <DiagramTooltip
                content={`Plan: ${a.plan}. Steps:\n${a.flow.join('\n')}`}
              >
                <div
                  class="rounded-md border border-[var(--line-thin)] bg-[var(--bg-deep)] p-2 text-[10px] font-mono text-[var(--ink-default)]"
                  tabindex={0}
                >
                  <div class="font-semibold text-[var(--ink-strong)] mb-1">
                    {a.spec}
                  </div>
                  <div class="opacity-70">→ {a.plan}</div>
                </div>
              </DiagramTooltip>
            ))}
          </div>
        </div>

        <div class="text-[11px] text-[var(--ink-muted)] leading-relaxed">
          <span class="font-semibold text-[var(--ink-strong)]">
            Idempotency:
          </span>{' '}
          reconcile вызывается много раз с тем же спеком -- каждый раз приводит
          систему к одному состоянию. Stateful upgrade использует{' '}
          <code>spec.job.savepointTriggerNonce</code> чтобы гарантировать ровно
          один новый savepoint, даже если loop повторил наблюдение.
        </div>
      </div>
    </DiagramContainer>
  );
}
