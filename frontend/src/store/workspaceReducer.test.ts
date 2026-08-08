import { describe, expect, it } from 'vitest';
import {
  initialWorkspaceState,
  normalizeEvent,
  workspaceReducer,
} from './workspaceReducer';

describe('normalizeEvent', () => {
  it('normalizes legacy flat SSE frames', () => {
    const event = normalizeEvent(
      { _seq: 7, type: 'agent_progress', role: 'worker', stage: 'coding' },
      'task-1',
    );

    expect(event.version).toBe(0);
    expect(event.sequence).toBe(7);
    expect(event.taskId).toBe('task-1');
    expect(event.agentInstanceId).toBeUndefined();
    expect(event.payload.stage).toBe('coding');
  });

  it('preserves versioned hierarchy identifiers', () => {
    const event = normalizeEvent(
      {
        version: 2,
        sequence: 42,
        type: 'agent_started',
        task_id: 'task-2',
        workstream_id: 'ws-ui',
        work_item_id: 'item-dock',
        agent_instance_id: 'worker-17',
        payload: { role: 'worker', manager_id: 'manager-ui' },
      },
      'fallback',
    );

    expect(event.version).toBe(2);
    expect(event.workstreamId).toBe('ws-ui');
    expect(event.workItemId).toBe('item-dock');
    expect(event.managerId).toBe('manager-ui');
    expect(event.agentInstanceId).toBe('worker-17');
  });
});

describe('workspaceReducer', () => {
  const reduceEvent = (
    state: typeof initialWorkspaceState,
    event: Record<string, unknown>,
  ) => workspaceReducer(state, { type: 'event', event, taskId: 'task-1' });

  it('creates dynamic agent instances keyed by agent_instance_id', () => {
    const state = reduceEvent(initialWorkspaceState, {
      version: 1,
      sequence: 1,
      type: 'agent_started',
      agent_instance_id: 'director-a',
      role: 'director',
      payload: { name: 'Director A', stage: 'planning' },
    });

    expect(state.directorId).toBe('director-a');
    expect(state.agents['director-a']).toMatchObject({
      title: 'Director A',
      status: 'running',
      phase: 'planning',
    });
  });

  it('groups multiple workers beneath one manager and work item', () => {
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'agent_started',
      agent_instance_id: 'manager-a',
      role: 'manager',
      payload: { stage: 'running' },
    });
    state = reduceEvent(state, {
      sequence: 2,
      type: 'agent_started',
      manager_id: 'manager-a',
      work_item_id: 'item-a',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { work_item_title: 'Build reducer', stage: 'running' },
    });
    state = reduceEvent(state, {
      sequence: 3,
      type: 'agent_started',
      manager_id: 'manager-a',
      work_item_id: 'item-a',
      agent_instance_id: 'worker-b',
      role: 'worker',
      payload: { work_item_title: 'Build reducer', stage: 'running' },
    });

    expect(state.managers).toHaveLength(1);
    expect(state.managers[0].items[0].agentIds).toEqual(['worker-a', 'worker-b']);
  });

  it('attaches testers under the same manager as workers', () => {
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'agent_started',
      agent_instance_id: 'manager-a',
      workstream_id: 'ws-1',
      role: 'manager',
      payload: { stage: 'running' },
    });
    state = reduceEvent(state, {
      sequence: 2,
      type: 'agent_started',
      manager_id: 'manager-a',
      workstream_id: 'ws-1',
      work_item_id: 'item-a',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { stage: 'running' },
    });
    state = reduceEvent(state, {
      sequence: 3,
      type: 'agent_started',
      manager_id: 'manager-a',
      workstream_id: 'ws-1',
      work_item_id: 'item-a',
      agent_instance_id: 'tester-a',
      role: 'tester',
      payload: { stage: 'running' },
    });

    expect(state.agents['tester-a'].managerId).toBe('manager-a');
    expect(state.managers[0].items[0].agentIds).toEqual(
      expect.arrayContaining(['worker-a', 'tester-a']),
    );
  });

  it('backfills managerId onto token streams from prior agent state', () => {
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'agent_started',
      manager_id: 'manager-a',
      workstream_id: 'ws-1',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { stage: 'running' },
    });
    state = reduceEvent(state, {
      sequence: 2,
      type: 'token',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { text: 'thinking aloud' },
    });

    expect(state.agents['worker-a'].managerId).toBe('manager-a');
    expect(state.agents['worker-a'].workstreamId).toBe('ws-1');
  });

  it('ignores replayed sequence numbers after reconnect', () => {
    const event = {
      sequence: 9,
      type: 'token',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { text: 'hello' },
    };
    const once = reduceEvent(initialWorkspaceState, event);
    const replayed = reduceEvent(once, event);

    expect(replayed.eventCount).toBe(1);
    expect(replayed.agents['worker-a'].output).toBe('hello');
  });

  it('collects context, diff, tests, and failure status', () => {
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'agent_action',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { action: 'submit_patch', file_path: 'src/App.tsx', diff: '+hello' },
    });
    state = reduceEvent(state, {
      sequence: 2,
      type: 'test_result',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { name: 'unit', accepted: false, detail: '1 failed' },
    });
    state = reduceEvent(state, {
      sequence: 3,
      type: 'error',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { message: 'Compilation failed' },
    });

    expect(state.agents['worker-a'].contextFiles).toEqual(['src/App.tsx']);
    expect(state.agents['worker-a'].diff).toBe('+hello');
    expect(state.agents['worker-a'].tests[0].status).toBe('failed');
    expect(state.agents['worker-a'].status).toBe('failed');
    expect(state.agents['worker-a'].error).toBe('Compilation failed');
  });

  it('records source-target agent signals for the animated graph', () => {
    const state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'agent_message',
      agent_instance_id: 'manager-a',
      role: 'agent',
      payload: {
        source_agent_id: 'manager-a',
        target_agent_id: 'worker-a',
        signal_type: 'delegate_work_item',
        summary: 'Implement the API.',
      },
    });

    expect(state.signals).toEqual([
      expect.objectContaining({
        sourceAgentId: 'manager-a',
        targetAgentId: 'worker-a',
        signalType: 'delegate_work_item',
        summary: 'Implement the API.',
      }),
    ]);
  });

  it('applies a model and effort override to one agent only', () => {
    const started = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'agent_started',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { model: 'claude-sonnet-5', effort: 'max' },
    });
    const configured = workspaceReducer(started, {
      type: 'agent-configured',
      agentId: 'worker-a',
      model: 'claude-sonnet-4-6',
      effort: 'high',
    });

    expect(configured.agents['worker-a']).toMatchObject({
      model: 'claude-sonnet-4-6',
      effort: 'high',
    });
  });

  it('keeps workers linked under their manager after later token events', () => {
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'agent_started',
      agent_instance_id: 'manager-a',
      workstream_id: 'ws-1',
      role: 'manager',
      payload: { stage: 'running' },
    });
    state = reduceEvent(state, {
      sequence: 2,
      type: 'agent_started',
      manager_id: 'manager-a',
      workstream_id: 'ws-1',
      work_item_id: 'item-a',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { stage: 'running' },
    });
    state = reduceEvent(state, {
      sequence: 3,
      type: 'token',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { text: 'coding' },
    });

    expect(state.agents['worker-a'].role).toBe('worker');
    expect(state.agents['worker-a'].managerId).toBe('manager-a');
    expect(state.managers[0].agentId).toBe('manager-a');
    expect(state.managers[0].items[0].agentIds).toContain('worker-a');
  });

  it('ignores protocol noise without agent_instance_id (no ghost agents)', () => {
    const state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'protocol_retry',
      role: 'director',
      payload: { reason: 'protocol_error' },
    });
    expect(Object.keys(state.agents)).toEqual([]);
    expect(state.eventCount).toBe(1);
  });
});
