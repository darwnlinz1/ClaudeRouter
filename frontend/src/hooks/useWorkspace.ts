import { useCallback, useEffect, useReducer, useRef, useState } from 'react';
import { api } from '../api/client';
import { initialWorkspaceState, normalizeEvent, workspaceReducer } from '../store/workspaceReducer';
import type { ApprovalItem, TaskSummary } from '../types';

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

const MAX_EVENTS_PER_FRAME = 1000;

const approvalStatus = (value: unknown): ApprovalItem['status'] =>
  value === 'approved' || value === 'rejected' ? value : 'pending';

const updateApprovalsFromEvent = (
  current: ApprovalItem[],
  raw: Record<string, unknown>,
  taskId: string,
): ApprovalItem[] => {
  const event = normalizeEvent(raw, taskId);
  if (event.type !== 'approval_requested' && event.type !== 'approval_decided') return current;
  const payload = event.payload;
  const approvalId = String(payload.approval_id ?? payload.approvalId ?? '').trim();
  if (!approvalId) return current;
  const index = current.findIndex((approval) => approval.approvalId === approvalId);
  if (event.type === 'approval_decided') {
    if (index < 0) return current;
    const status = approvalStatus(payload.decision ?? payload.status);
    const next = [...current];
    next[index] = {
      ...next[index],
      status,
      decidedAt: String(payload.decided_at ?? payload.decidedAt ?? event.timestamp),
    };
    return next;
  }

  const existing = index >= 0 ? current[index] : undefined;
  const requested: ApprovalItem = {
    approvalId,
    taskId: String(payload.task_id ?? payload.taskId ?? taskId),
    workstreamId:
      payload.workstream_id || payload.workstreamId
        ? String(payload.workstream_id ?? payload.workstreamId)
        : existing?.workstreamId,
    kind: String(payload.kind ?? existing?.kind ?? 'action'),
    target: String(payload.target ?? existing?.target ?? ''),
    reason: String(payload.reason ?? existing?.reason ?? ''),
    status: approvalStatus(payload.status ?? existing?.status),
    createdAt: String(
      payload.created_at ?? payload.createdAt ?? existing?.createdAt ?? event.timestamp,
    ),
    decidedAt: existing?.decidedAt,
  };
  if (index < 0) return [...current, requested];
  const next = [...current];
  next[index] = requested;
  return next;
};

export function useWorkspace(task?: TaskSummary) {
  const [state, dispatch] = useReducer(workspaceReducer, initialWorkspaceState);
  const [approvals, setApprovals] = useState<ApprovalItem[]>([]);
  const sequenceRef = useRef(0);
  const streamOpenedTaskRef = useRef<string | undefined>(undefined);
  const [hydratedTaskId, setHydratedTaskId] = useState<string>();
  const taskId = task?.id;
  const taskStatus = task?.status;

  useEffect(() => {
    sequenceRef.current = Math.max(sequenceRef.current, state.sequence);
  }, [state.sequence]);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();
    if (!task) {
      sequenceRef.current = 0;
      setHydratedTaskId(undefined);
      setApprovals([]);
      streamOpenedTaskRef.current = undefined;
      dispatch({ type: 'reset' });
      return;
    }

    sequenceRef.current = 0;
    setHydratedTaskId(undefined);
    setApprovals([]);
    streamOpenedTaskRef.current = undefined;
    dispatch({ type: 'reset', task });
    if (task.id === 'preview-workspace') {
      dispatch({ type: 'load-task', task });
      setHydratedTaskId(task.id);
      return;
    }

    void (async () => {
      const [detail, approvalItems] = await Promise.all([
        api.getTask(task.id, controller.signal),
        api.listApprovals(task.id, controller.signal).catch((error) => {
          if (controller.signal.aborted) throw error;
          return [];
        }),
      ]);
      if (cancelled) return;
      let events: Record<string, unknown>[];
      let timeline:
        | {
            latestSequence: number;
            retainedFromSequence: number;
            historyIncomplete: boolean;
          }
        | undefined;
      try {
        const result = await api.getTimeline(task.id, 0, controller.signal);
        events = result.events;
        timeline = {
          latestSequence: result.latestSequence,
          retainedFromSequence: result.retainedFromSequence,
          historyIncomplete: result.historyIncomplete,
        };
        if (cancelled) return;
      } catch {
        if (controller.signal.aborted) return;
        // Legacy tasks may not have a durable hierarchy timeline.
        events = detail.events ?? [];
      }
      if (cancelled) return;
      // Durable timeline is the source of truth. Replaying detail.events
      // first would jump sequence to the end and discard older token/thinking
      // frames as duplicates.
      const hierarchy = { ...(detail.hierarchy ?? {}) };
      delete hierarchy.execution;
      dispatch({
        type: 'load-task',
        task: { ...detail, events: [], hierarchy },
      });
      const ordered = [...events].sort(
        (left, right) => Number(left.sequence ?? 0) - Number(right.sequence ?? 0),
      );
      if (ordered.length) {
        dispatch({ type: 'events', events: ordered, taskId: task.id });
        sequenceRef.current = Math.max(
          sequenceRef.current,
          ...ordered.map((event) => Number(event.sequence ?? 0)).filter(Number.isFinite),
        );
      }
      if (timeline) {
        sequenceRef.current = Math.max(sequenceRef.current, timeline.latestSequence);
      }
      dispatch({ type: 'apply-snapshot', task: detail, timeline });
      setApprovals(approvalItems);
      setHydratedTaskId(task.id);
    })().catch(() => {
      // Missing/deleted tasks must not keep a ghost live stream open.
      if (!cancelled && !controller.signal.aborted) dispatch({ type: 'reset' });
    });

    return () => {
      cancelled = true;
      controller.abort();
    };
    // Polling replaces the summary object; identity alone controls hydration.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [taskId]);

  useEffect(() => {
    if (
      !taskId ||
      hydratedTaskId !== taskId ||
      taskId === 'preview-workspace' ||
      !taskStatus ||
      !ACTIVE_STATUSES.has(taskStatus)
    )
      return;

    let source: EventSource | undefined;
    let retryTimer: number | undefined;
    let stopped = false;
    let retries = 0;
    const queuedEvents: Record<string, unknown>[] = [];
    let frameHandle: number | undefined;
    let frameUsesTimeout = false;
    let approvalController: AbortController | undefined;

    const cancelScheduledFrame = () => {
      if (frameHandle == null) return;
      if (frameUsesTimeout) window.clearTimeout(frameHandle);
      else window.cancelAnimationFrame(frameHandle);
      frameHandle = undefined;
    };
    const scheduleFlush = () => {
      if (stopped || frameHandle != null) return;
      if (typeof window.requestAnimationFrame === 'function') {
        frameUsesTimeout = false;
        frameHandle = window.requestAnimationFrame(() => {
          frameHandle = undefined;
          flushQueuedEvents();
        });
      } else {
        frameUsesTimeout = true;
        frameHandle = window.setTimeout(() => {
          frameHandle = undefined;
          flushQueuedEvents();
        }, 16);
      }
    };
    const flushQueuedEvents = (all = false) => {
      if (!queuedEvents.length) return;
      const count = all ? queuedEvents.length : MAX_EVENTS_PER_FRAME;
      const events = queuedEvents.splice(0, count);
      dispatch({ type: 'events', events, taskId });
      if (queuedEvents.length) scheduleFlush();
    };
    const queueEvent = (event: Record<string, unknown>) => {
      queuedEvents.push(event);
      scheduleFlush();
    };
    const refreshApprovals = () => {
      approvalController?.abort();
      approvalController = new AbortController();
      const activeController = approvalController;
      void api
        .listApprovals(taskId, activeController.signal)
        .then((items) => {
          if (!stopped && !activeController.signal.aborted) setApprovals(items);
        })
        .catch(() => {
          // Keep the last known approvals if a reconnect refresh is cancelled or unavailable.
        });
    };

    const teardown = () => {
      cancelScheduledFrame();
      flushQueuedEvents(true);
      stopped = true;
      source?.close();
      source = undefined;
      if (retryTimer) window.clearTimeout(retryTimer);
      approvalController?.abort();
    };

    const connect = () => {
      if (stopped || document.visibilityState === 'hidden' || !navigator.onLine) return;
      if (retryTimer) {
        window.clearTimeout(retryTimer);
        retryTimer = undefined;
      }
      source?.close();
      dispatch({
        type: 'connection',
        connection: retries > 0 ? 'reconnecting' : 'connecting',
      });
      source = new EventSource(api.streamUrl(taskId, sequenceRef.current));
      source.onopen = () => {
        const reconnected = streamOpenedTaskRef.current === taskId;
        streamOpenedTaskRef.current = taskId;
        retries = 0;
        dispatch({ type: 'connection', connection: 'live' });
        if (reconnected) refreshApprovals();
      };
      source.onmessage = (message) => {
        try {
          const event = JSON.parse(message.data) as Record<string, unknown>;
          const normalized = normalizeEvent(event, taskId, sequenceRef.current + 1);
          sequenceRef.current = Math.max(sequenceRef.current, normalized.sequence);
          if (normalized.type === 'approval_requested' || normalized.type === 'approval_decided') {
            setApprovals((current) => updateApprovalsFromEvent(current, event, taskId));
          }
          if (
            normalized.type === 'done' ||
            event.fatal === true ||
            normalized.payload.fatal === true
          ) {
            queueEvent(event);
            cancelScheduledFrame();
            flushQueuedEvents(true);
            dispatch({ type: 'connection', connection: 'idle' });
            teardown();
            return;
          }
          if (normalized.type === 'error') {
            const detail = String(normalized.payload.data ?? normalized.payload.detail ?? '');
            queueEvent(event);
            if (/không tồn tại|closed|not found/i.test(detail)) {
              cancelScheduledFrame();
              flushQueuedEvents(true);
              dispatch({ type: 'connection', connection: 'idle' });
              teardown();
              return;
            }
            return;
          }
          queueEvent(event);
        } catch {
          // A malformed frame should not terminate an otherwise healthy stream.
        }
      };
      source.onerror = () => {
        source?.close();
        source = undefined;
        if (stopped) return;
        retries += 1;
        dispatch({ type: 'connection', connection: 'reconnecting' });
        const delay = Math.min(30_000, 800 * 2 ** Math.min(retries, 6));
        retryTimer = window.setTimeout(connect, delay);
      };
    };

    const resume = () => {
      if (stopped || document.visibilityState === 'hidden' || !navigator.onLine) {
        return;
      }
      retries = 0;
      connect();
    };
    const pause = () => {
      if (document.visibilityState !== 'hidden') return;
      cancelScheduledFrame();
      flushQueuedEvents(true);
      source?.close();
      source = undefined;
      if (retryTimer) window.clearTimeout(retryTimer);
      retryTimer = undefined;
      dispatch({ type: 'connection', connection: 'reconnecting' });
    };

    window.addEventListener('online', resume);
    document.addEventListener('visibilitychange', pause);
    document.addEventListener('visibilitychange', resume);
    connect();
    return () => {
      window.removeEventListener('online', resume);
      document.removeEventListener('visibilitychange', pause);
      document.removeEventListener('visibilitychange', resume);
      teardown();
    };
  }, [hydratedTaskId, taskId, taskStatus]);

  const markApprovalDecision = useCallback((approvalId: string, status: ApprovalItem['status']) => {
    setApprovals((current) =>
      current.map((approval) =>
        approval.approvalId === approvalId
          ? { ...approval, status, decidedAt: new Date().toISOString() }
          : approval,
      ),
    );
  }, []);

  return { state, dispatch, approvals, markApprovalDecision };
}
