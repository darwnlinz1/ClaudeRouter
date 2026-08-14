// @vitest-environment jsdom

import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { TaskSummary } from '../types';
import { useWorkspace } from './useWorkspace';

const apiMock = vi.hoisted(() => ({
  getTask: vi.fn(),
  getTimeline: vi.fn(),
  listApprovals: vi.fn(),
  streamUrl: vi.fn(),
}));

vi.mock('../api/client', () => ({ api: apiMock }));

class FakeEventSource {
  static instances: FakeEventSource[] = [];

  onopen: (() => void) | null = null;
  onmessage: ((message: MessageEvent<string>) => void) | null = null;
  onerror: (() => void) | null = null;
  close = vi.fn();

  constructor(readonly url: string) {
    FakeEventSource.instances.push(this);
  }
}

const task: TaskSummary = {
  id: 'task-live',
  name: 'Live task',
  status: 'RUNNING',
};

let animationFrames: Array<FrameRequestCallback>;

const flushAnimationFrame = () => {
  const callbacks = animationFrames;
  animationFrames = [];
  act(() => {
    for (const callback of callbacks) callback(performance.now());
  });
};

const sendEvent = (source: FakeEventSource, event: Record<string, unknown>) => {
  source.onmessage?.(new MessageEvent('message', { data: JSON.stringify(event) }));
};

beforeEach(() => {
  vi.clearAllMocks();
  FakeEventSource.instances = [];
  animationFrames = [];
  vi.stubGlobal('EventSource', FakeEventSource);
  vi.stubGlobal(
    'requestAnimationFrame',
    vi.fn((callback: FrameRequestCallback) => {
      animationFrames.push(callback);
      return animationFrames.length;
    }),
  );
  vi.stubGlobal('cancelAnimationFrame', vi.fn());
  apiMock.getTask.mockResolvedValue({ ...task, events: [] });
  apiMock.getTimeline.mockResolvedValue([
    {
      sequence: 1,
      type: 'agent_started',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { stage: 'running' },
    },
  ]);
  apiMock.listApprovals.mockResolvedValue([]);
  apiMock.streamUrl.mockReturnValue('/api/stream/task-live?after=1');
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('useWorkspace live synchronization', () => {
  it('fetches approvals once per hydrate/reconnect and applies approval events locally', async () => {
    const { result, unmount } = renderHook(() => useWorkspace(task));

    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(1));
    expect(apiMock.listApprovals).toHaveBeenCalledTimes(1);

    act(() => FakeEventSource.instances[0].onopen?.());
    expect(apiMock.listApprovals).toHaveBeenCalledTimes(1);

    act(() => {
      sendEvent(FakeEventSource.instances[0], {
        sequence: 2,
        type: 'approval_requested',
        approval_id: 'approval-1',
        kind: 'write',
        target: 'frontend/src/App.tsx',
        reason: 'High-risk write',
        status: 'pending',
      });
    });
    expect(result.current.approvals).toEqual([
      expect.objectContaining({ approvalId: 'approval-1', status: 'pending' }),
    ]);
    expect(apiMock.listApprovals).toHaveBeenCalledTimes(1);

    act(() => {
      sendEvent(FakeEventSource.instances[0], {
        sequence: 3,
        type: 'approval_decided',
        approval_id: 'approval-1',
        decision: 'approved',
      });
    });
    expect(result.current.approvals[0].status).toBe('approved');
    expect(apiMock.listApprovals).toHaveBeenCalledTimes(1);

    act(() => window.dispatchEvent(new Event('online')));
    expect(FakeEventSource.instances).toHaveLength(2);
    await act(async () => {
      FakeEventSource.instances[1].onopen?.();
      await Promise.resolve();
    });
    expect(apiMock.listApprovals).toHaveBeenCalledTimes(2);

    unmount();
  });

  it('renders at most once for 1000 token events in one animation frame', async () => {
    let renderCount = 0;
    const { result, unmount } = renderHook(() => {
      renderCount += 1;
      return useWorkspace(task);
    });
    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(1));
    const source = FakeEventSource.instances[0];
    const rendersBeforeTokens = renderCount;
    const graphRevision = result.current.state.graphRevision;

    act(() => {
      for (let index = 0; index < 1000; index += 1) {
        sendEvent(source, {
          sequence: index + 2,
          type: 'token',
          agent_instance_id: 'worker-a',
          role: 'worker',
          payload: { text: 'x' },
        });
      }
    });

    expect(requestAnimationFrame).toHaveBeenCalledTimes(1);
    expect(renderCount).toBe(rendersBeforeTokens);
    flushAnimationFrame();
    expect(renderCount - rendersBeforeTokens).toBeLessThanOrEqual(1);
    expect(result.current.state.eventCount).toBe(1001);
    expect(result.current.state.agents['worker-a'].output).toHaveLength(1000);
    expect(result.current.state.graphRevision).toBe(graphRevision);

    unmount();
  });
});
