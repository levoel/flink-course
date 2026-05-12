/**
 * FlinkK8sOperator
 *
 * Apache Flink Kubernetes Operator: control loop вокруг FlinkDeployment CRD.
 * Каждый reconcile cycle: observe (запросить текущее состояние JM/TM/JobStatus)
 * → plan (diff spec vs status, решить какой action) → execute (apply patch,
 * trigger savepoint, restart cluster и т.д.).
 */

import { useState } from 'react';
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
  const [phase, setPhase] = useState<Phase>('observe');

  return (
    <DiagramContainer
      title="Flink Kubernetes Operator: reconcile loop"
      color="blue"
      description="Operator pattern из k8s. CRD FlinkDeployment / FlinkSessionJob. Loop работает event-driven (watch CRD) + periodic resync."
    >
      <div className="flex flex-col gap-4">
        {/* Top: CRD + operator */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
          <DiagramTooltip content="CRD FlinkDeployment. Spec описывает желаемый кластер: image, replicas, jobManager.resource.memory, flinkConfiguration, job.upgradeMode (stateless / savepoint / last-state).">
            <div
              className="rounded-md border border-amber-400/40 bg-amber-500/10 px-3 py-2 text-[11px] font-mono text-amber-800"
              tabIndex={0}
            >
              <div className="font-semibold">FlinkDeployment CRD</div>
              <div className="text-[10px] opacity-70 mt-1">
                .spec · .status<br />
                + FlinkSessionJob CRD
              </div>
            </div>
          </DiagramTooltip>

          <DiagramTooltip content="Operator pod в namespace. Watch endpoints на FlinkDeployment, FlinkSessionJob, Deployment, ConfigMap. Каждое событие → enqueue в workqueue → reconcile.">
            <div
              className="rounded-md border border-blue-400/40 bg-blue-500/10 px-3 py-2 text-[11px] font-mono text-blue-800"
              tabIndex={0}
            >
              <div className="font-semibold">flink-kubernetes-operator</div>
              <div className="text-[10px] opacity-70 mt-1">
                · informer<br />
                · workqueue<br />
                · reconciler
              </div>
            </div>
          </DiagramTooltip>

          <DiagramTooltip content="Управляемые ресурсы: JobManager Deployment, TaskManager Deployment, ConfigMap, Service, опционально Ingress. Owner reference указывает на FlinkDeployment, поэтому GC чистит всё при удалении CR.">
            <div
              className="rounded-md border border-emerald-400/40 bg-emerald-500/10 px-3 py-2 text-[11px] font-mono text-emerald-800"
              tabIndex={0}
            >
              <div className="font-semibold">managed resources</div>
              <div className="text-[10px] opacity-70 mt-1">
                JM Deployment · TM Deployment<br />
                ConfigMap · Service
              </div>
            </div>
          </DiagramTooltip>
        </div>

        {/* Reconcile loop */}
        <div className="rounded-lg border border-[var(--line-thin)] bg-[var(--bg-surface)] p-3">
          <div className="text-xs font-mono text-[var(--ink-strong)] mb-2">
            reconcile() loop
          </div>
          <div className="grid grid-cols-3 gap-2">
            {PHASES.map((p) => (
              <DiagramTooltip key={p.id} content={p.description}>
                <button
                  type="button"
                  onClick={() => setPhase(p.id)}
                  className={`rounded-md border px-3 py-2 text-[11px] font-mono w-full text-left transition-colors ${
                    phase === p.id
                      ? 'bg-blue-500/30 border-blue-400/60 text-blue-800'
                      : 'bg-[var(--bg-surface)] border-[var(--line-thin)] text-[var(--ink-default)] hover:bg-[var(--bg-deep)]'
                  }`}
                >
                  <div className="font-semibold">{p.label}</div>
                </button>
              </DiagramTooltip>
            ))}
          </div>

          {/* Phase detail */}
          <div className="mt-3 text-[11px] text-[var(--ink-default)] leading-relaxed">
            {PHASES.find((p) => p.id === phase)!.description}
          </div>
        </div>

        {/* Action examples */}
        <div className="rounded-lg border border-[var(--line-thin)] bg-[var(--bg-surface)] p-3">
          <div className="text-xs font-mono text-[var(--ink-strong)] mb-2">
            Common spec changes → reconcile actions
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
            {ACTIONS.map((a, i) => (
              <DiagramTooltip
                key={i}
                content={`Plan: ${a.plan}. Steps:\n${a.flow.join('\n')}`}
              >
                <div
                  className="rounded-md border border-[var(--line-thin)] bg-[var(--bg-deep)] p-2 text-[10px] font-mono text-[var(--ink-default)]"
                  tabIndex={0}
                >
                  <div className="font-semibold text-[var(--ink-strong)] mb-1">
                    {a.spec}
                  </div>
                  <div className="opacity-70">→ {a.plan}</div>
                </div>
              </DiagramTooltip>
            ))}
          </div>
        </div>

        <div className="text-[11px] text-[var(--ink-muted)] leading-relaxed">
          <span className="font-semibold text-[var(--ink-strong)]">
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
