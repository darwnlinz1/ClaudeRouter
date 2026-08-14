export const PHASES = [
  'PLANNING',
  'MANAGING',
  'CODING',
  'REVIEWING',
  'REVISION',
  'COMPLETED',
] as const;

export type Phase = (typeof PHASES)[number];

export const STEPPER_LABELS = ['Plan', 'Manage', 'Code', 'Review', 'Done'] as const;

const STATUS_PHASE: Record<string, Phase> = {
  QUEUED: 'PLANNING',
  RUNNING: 'PLANNING',
  RESUMING: 'PLANNING',
  PLANNING: 'PLANNING',
  MANAGING: 'MANAGING',
  CODING: 'CODING',
  REVIEWING: 'REVIEWING',
  REVISION: 'REVISION',
  WAITING_INPUT: 'REVISION',
  STOPPING: 'REVISION',
  COMPLETED: 'COMPLETED',
  DONE: 'COMPLETED',
  PARTIAL: 'COMPLETED',
  ABANDONED: 'COMPLETED',
  SKIPPED: 'COMPLETED',
  FAILED: 'COMPLETED',
  STOPPED: 'COMPLETED',
  CANCELLED: 'COMPLETED',
};

const ACTIVE_STATUSES = new Set([
  'QUEUED',
  'RUNNING',
  'PLANNING',
  'MANAGING',
  'CODING',
  'REVIEWING',
  'REVISION',
  'WAITING_INPUT',
  'STOPPING',
  'RESUMING',
]);

export function phaseIndex(status: string): number {
  const phase = STATUS_PHASE[status.toUpperCase()] ?? 'PLANNING';
  if (phase === 'REVISION') return 3;
  if (phase === 'COMPLETED') return STEPPER_LABELS.length - 1;
  const idx = PHASES.indexOf(phase);
  return idx >= 0 ? Math.min(idx, STEPPER_LABELS.length - 1) : 0;
}

export function isActiveStatus(status: string): boolean {
  return ACTIVE_STATUSES.has(status.toUpperCase());
}

export function statusTone(status: string): 'active' | 'success' | 'danger' | 'warn' | 'idle' {
  const normalized = status.toUpperCase();
  if (['COMPLETED', 'DONE', 'PASSED'].includes(normalized)) return 'success';
  if (['FAILED', 'ERROR', 'CANCELLED', 'ABANDONED'].includes(normalized)) return 'danger';
  if (['WAITING_INPUT', 'STOPPING', 'REVISION', 'PARTIAL', 'SKIPPED'].includes(normalized)) {
    return 'warn';
  }
  if (isActiveStatus(normalized)) return 'active';
  return 'idle';
}
