import { useEffect, useReducer, useRef } from 'react';
import { api } from '../api/client';
import { initialWorkspaceState, workspaceReducer } from '../store/workspaceReducer';
import type { TaskSummary } from '../types';

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

export function useWorkspace(task?: TaskSummary) {
  const [state, dispatch] = useReducer(workspaceReducer, initialWorkspaceState);
  const sequenceRef = useRef(0);

  useEffect(() => {
    sequenceRef.current = state.sequence;
  }, [state.sequence]);

  useEffect(() => {
    let cancelled = false;
    if (!task) {
      dispatch({ type: 'reset' });
      return;
    }

    dispatch({ type: 'reset', task });
    if (task.id === 'preview-workspace') {
      dispatch({ type: 'load-task', task });
      return;
    }

    api
      .getTask(task.id)
      .then(async (detail) => {
        if (cancelled) return;
        dispatch({ type: 'load-task', task: detail });
        try {
          const events = await api.getTimeline(task.id);
          if (cancelled) return;
          for (const event of events) {
            dispatch({ type: 'event', event, taskId: task.id });
          }
        } catch {
          // Legacy tasks may not have a durable hierarchy timeline.
        }
      })
      .catch(() => {
        // Missing/deleted tasks must not keep a ghost live stream open.
        if (!cancelled) dispatch({ type: 'reset' });
      });

    return () => {
      cancelled = true;
    };
  }, [task?.id]);

  useEffect(() => {
    if (!task || task.id === 'preview-workspace' || !ACTIVE_STATUSES.has(task.status)) return;

    let source: EventSource | undefined;
    let retryTimer: number | undefined;
    let stopped = false;
    let retries = 0;
    const maxRetries = 8;

    const teardown = () => {
      stopped = true;
      source?.close();
      source = undefined;
      if (retryTimer) window.clearTimeout(retryTimer);
    };

    const connect = () => {
      if (stopped) return;
      dispatch({
        type: 'connection',
        connection: retries > 0 ? 'reconnecting' : 'connecting',
      });
      source = new EventSource(api.streamUrl(task.id, sequenceRef.current));
      source.onopen = () => {
        retries = 0;
        dispatch({ type: 'connection', connection: 'live' });
      };
      source.onmessage = (message) => {
        try {
          const event = JSON.parse(message.data) as Record<string, unknown>;
          if (event.type === 'done' || event.fatal === true) {
            dispatch({ type: 'event', event, taskId: task.id });
            dispatch({ type: 'connection', connection: 'idle' });
            teardown();
            return;
          }
          if (event.type === 'error') {
            const detail = String(event.data ?? '');
            dispatch({ type: 'event', event, taskId: task.id });
            if (/không tồn tại|closed|not found/i.test(detail)) {
              dispatch({ type: 'connection', connection: 'idle' });
              teardown();
              return;
            }
            return;
          }
          dispatch({ type: 'event', event, taskId: task.id });
        } catch {
          // A malformed frame should not terminate an otherwise healthy stream.
        }
      };
      source.onerror = () => {
        source?.close();
        if (stopped) return;
        retries += 1;
        if (retries > maxRetries) {
          dispatch({ type: 'connection', connection: 'idle' });
          teardown();
          return;
        }
        dispatch({ type: 'connection', connection: 'reconnecting' });
        const delay = Math.min(12_000, 800 * 2 ** Math.min(retries, 4));
        retryTimer = window.setTimeout(connect, delay);
      };
    };

    connect();
    return () => {
      teardown();
    };
  }, [task?.id, task?.status]);

  return { state, dispatch };
}
