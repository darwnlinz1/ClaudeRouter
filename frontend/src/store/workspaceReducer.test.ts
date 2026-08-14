import { describe, expect, it } from 'vitest';
import {
  initialWorkspaceState,
  normalizeEvent,
  WORKSPACE_LIMITS,
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
        call_id: 'llmreq-17',
        payload: { role: 'worker', manager_id: 'manager-ui' },
      },
      'fallback',
    );

    expect(event.version).toBe(2);
    expect(event.workstreamId).toBe('ws-ui');
    expect(event.workItemId).toBe('item-dock');
    expect(event.managerId).toBe('manager-ui');
    expect(event.agentInstanceId).toBe('worker-17');
    expect(event.callId).toBe('llmreq-17');
  });
});

describe('workspaceReducer', () => {
  const reduceEvent = (state: typeof initialWorkspaceState, event: Record<string, unknown>) =>
    workspaceReducer(state, { type: 'event', event, taskId: 'task-1' });

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

  it('keeps planned agents separate from actual starts', () => {
    const planned = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'agent_planned',
      agent_instance_id: 'worker-planned',
      manager_id: 'manager-a',
      role: 'worker',
      payload: { status: 'planned', title: 'Worker 1.1' },
    });

    expect(planned.agents['worker-planned']).toMatchObject({
      status: 'queued',
      phase: 'planned',
      executionState: 'planned',
    });
    expect(planned.agents['worker-planned'].startedAt).toBeUndefined();

    const started = reduceEvent(planned, {
      sequence: 2,
      timestamp: '2026-08-13T04:00:00Z',
      type: 'agent_started',
      agent_instance_id: 'worker-planned',
      manager_id: 'manager-a',
      role: 'worker',
      payload: { status: 'running' },
    });

    expect(started.agents['worker-planned']).toMatchObject({
      status: 'running',
      executionState: 'in_flight',
      startedAt: '2026-08-13T04:00:00Z',
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

  it('batches 1000 token frames without rebuilding the graph and bounds recent output', () => {
    const started = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'agent_started',
      manager_id: 'manager-a',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { stage: 'running' },
    });
    const graphRevision = started.graphRevision;
    const managers = started.managers;
    const events = Array.from({ length: 1000 }, (_, index) => ({
      sequence: index + 2,
      type: 'token',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { text: `${String(index).padStart(4, '0')}:${'x'.repeat(75)}\n` },
    }));

    const state = workspaceReducer(started, {
      type: 'events',
      events,
      taskId: 'task-1',
    });

    expect(state.eventCount).toBe(1001);
    expect(state.sequence).toBe(1001);
    expect(state.graphRevision).toBe(graphRevision);
    expect(state.managers).toBe(managers);
    expect(state.agents['worker-a'].output.length).toBe(WORKSPACE_LIMITS.outputCharacters);
    expect(state.agents['worker-a'].output).toContain('0999:');
  });

  it('creates graph topology when the first frame for an agent is streaming', () => {
    const state = workspaceReducer(initialWorkspaceState, {
      type: 'events',
      taskId: 'task-1',
      events: [
        {
          sequence: 1,
          type: 'thinking',
          agent_instance_id: 'director-stream',
          role: 'director',
          payload: { name: 'Director stream', text: 'Planning.' },
        },
        {
          sequence: 2,
          type: 'token',
          agent_instance_id: 'worker-stream',
          manager_id: 'manager-stream',
          role: 'worker',
          payload: { name: 'Worker stream', text: 'Working.' },
        },
      ],
    });

    expect(state.graphRevision).toBe(1);
    expect(state.directorId).toBe('director-stream');
    expect(Object.keys(state.agents)).toEqual(['director-stream', 'worker-stream']);
    expect(state.agents['worker-stream'].managerId).toBe('manager-stream');
  });

  it('keeps activity, signal, and call-attempt histories bounded to recent entries', () => {
    const activityEvents = Array.from({ length: 250 }, (_, index) => ({
      sequence: index + 1,
      type: 'agent_action',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { action: `step-${index}` },
    }));
    const signalEvents = Array.from({ length: 150 }, (_, index) => ({
      sequence: index + 251,
      type: 'agent_message',
      payload: {
        source_agent_id: 'manager-a',
        target_agent_id: 'worker-a',
        summary: `message-${index}`,
      },
    }));
    const attemptEvents = Array.from({ length: 80 }, (_, index) => ({
      sequence: index + 401,
      type: 'model_request_started',
      agent_instance_id: 'worker-a',
      role: 'worker',
      attempt_id: `attempt-${index}`,
      attempt: index + 1,
    }));
    const state = workspaceReducer(initialWorkspaceState, {
      type: 'events',
      events: [...activityEvents, ...signalEvents, ...attemptEvents],
      taskId: 'task-1',
    });

    expect(state.agents['worker-a'].activity).toHaveLength(WORKSPACE_LIMITS.activity);
    expect(state.agents['worker-a'].activity[0].message).toContain('step-130');
    expect(state.signals).toHaveLength(WORKSPACE_LIMITS.signals);
    expect(state.signals[0].summary).toBe('message-50');
    expect(state.agents['worker-a'].callAttempts).toHaveLength(WORKSPACE_LIMITS.attempts);
    expect(state.agents['worker-a'].callAttempts[0].id).toBe('attempt-30');
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

  it('restores thinking and explains worker preflight failures', () => {
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'agent_started',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { status: 'queued' },
    });
    state = reduceEvent(state, {
      sequence: 2,
      type: 'thinking',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: { text: 'Inspecting the target file.' },
    });
    state = reduceEvent(state, {
      sequence: 3,
      type: 'agent_failed',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: {
        status: 'preflight_failed',
        error: 'FileNotFoundError: requirements.txt',
      },
    });

    expect(state.agents['worker-a'].thinking).toContain('Inspecting the target file.');
    expect(state.agents['worker-a'].status).toBe('failed');
    expect(state.agents['worker-a'].error).toContain('requirements.txt');
  });

  it('shows dependency-gated agents as blocked instead of failed', () => {
    const state = workspaceReducer(initialWorkspaceState, {
      type: 'events',
      taskId: 'task-blocked',
      events: [
        {
          sequence: 1,
          type: 'agent_started',
          agent_instance_id: 'worker-blocked',
          manager_id: 'manager-upstream',
          role: 'worker',
          payload: { status: 'queued' },
        },
        {
          sequence: 2,
          type: 'agent_blocked',
          agent_instance_id: 'worker-blocked',
          manager_id: 'manager-upstream',
          role: 'worker',
          payload: {
            status: 'blocked',
            failure_kind: 'dependency',
            blocked_by: ['foundation'],
          },
        },
      ],
    });

    expect(state.agents['worker-blocked']).toMatchObject({
      status: 'blocked',
      executionState: 'blocked',
    });
    expect(state.agents['worker-blocked'].failures).toEqual([]);
    expect(state.managers[0]).toMatchObject({
      status: 'blocked',
      items: [expect.objectContaining({ status: 'blocked' })],
    });
  });

  it('shows provisional thinking live and replaces it with committed thinking once', () => {
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'thinking',
      agent_instance_id: 'worker-live',
      role: 'worker',
      attempt_id: 'worker-live:a1',
      chunk_index: 0,
      provisional: true,
      committed: false,
      payload: { text: 'Inspecting live.' },
    });

    expect(state.agents['worker-live'].provisionalThinking).toBe('Inspecting live.');
    expect(state.agents['worker-live'].thinking).toBe('');

    state = reduceEvent(state, {
      sequence: 2,
      type: 'thinking',
      agent_instance_id: 'worker-live',
      role: 'worker',
      attempt_id: 'worker-live:a1',
      chunk_index: 0,
      provisional: false,
      committed: true,
      payload: { text: 'Inspecting live.' },
    });

    expect(state.agents['worker-live'].thinking).toBe('Inspecting live.');
    expect(state.agents['worker-live'].provisionalThinking).toBe('');
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

  it('separates planned fan-out from real calls and preserves agent identity on 429', () => {
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'hierarchy_fanout_planned',
      agent_instance_id: 'director-a',
      role: 'director',
      payload: {
        manager_count: 4,
        coder_count: 16,
        tester_count: 4,
        child_agent_count: 20,
        max_parallel_managers: 4,
        max_parallel_workers: 8,
      },
    });
    state = reduceEvent(state, {
      sequence: 2,
      type: 'model_request_started',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: {
        account: 'a.txt',
        logical_request_id: 'llmreq-1',
        request_fingerprint: 'same-prompt',
        replayed: false,
      },
    });
    state = reduceEvent(state, {
      sequence: 3,
      type: 'account_rate_limited',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: {
        from_account: 'a.txt',
        logical_request_id: 'llmreq-1',
        request_fingerprint: 'same-prompt',
      },
    });
    state = reduceEvent(state, {
      sequence: 4,
      type: 'account_switch',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: {
        from_account: 'a.txt',
        to_account: 'b.txt',
        reason: 'rate_limit',
        logical_request_id: 'llmreq-1',
        request_fingerprint: 'same-prompt',
        replayed: true,
      },
    });
    state = reduceEvent(state, {
      sequence: 5,
      type: 'model_request_started',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: {
        account: 'b.txt',
        logical_request_id: 'llmreq-1',
        request_fingerprint: 'same-prompt',
        replayed: true,
      },
    });
    state = reduceEvent(state, {
      sequence: 6,
      type: 'model_request_completed',
      agent_instance_id: 'worker-a',
      role: 'worker',
      payload: {
        account: 'b.txt',
        logical_request_id: 'llmreq-1',
        request_fingerprint: 'same-prompt',
        replayed: true,
      },
    });

    expect(state.fanout).toMatchObject({
      plannedCoders: 16,
      plannedTesters: 4,
      plannedChildren: 20,
      requestAttempts: 2,
      completedRequests: 1,
      replayedRequests: 1,
      accountSwitches: 1,
      calledAgentIds: ['worker-a'],
    });
    expect(state.agents['worker-a']).toMatchObject({
      account: 'b.txt',
      previousAccount: 'a.txt',
      logicalRequestId: 'llmreq-1',
      requestFingerprint: 'same-prompt',
      requestAttempts: 2,
      replayCount: 1,
      accountSwitchCount: 1,
    });
  });

  it('stores work contracts on plans, work items, and their assigned agents', () => {
    const contract = {
      contract_id: 'contract-ui',
      version: 2,
      input_artifacts: ['design.md'],
      expected_outputs: ['frontend/src/App.tsx'],
      read_scopes: ['frontend/src'],
      write_scopes: ['frontend/src/App.tsx'],
      acceptance_criteria: ['Dashboard renders'],
      evidence_requirements: ['npm test'],
      consumers: ['tester-ui'],
      risk_level: 'medium',
      priority: 3,
    };
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'plan_created',
      payload: {
        workstreams: [{ id: 'ws-ui', title: 'User interface', dependencies: [], contract }],
      },
    });
    state = reduceEvent(state, {
      sequence: 2,
      type: 'manager_plan_created',
      agent_instance_id: 'manager-ui',
      manager_id: 'manager-ui',
      workstream_id: 'ws-ui',
      role: 'manager',
      payload: {
        work_items: [{ id: 'item-dashboard', title: 'Dashboard', dependencies: [], contract }],
      },
    });
    state = reduceEvent(state, {
      sequence: 3,
      type: 'agent_started',
      agent_instance_id: 'worker-ui',
      manager_id: 'manager-ui',
      workstream_id: 'ws-ui',
      work_item_id: 'item-dashboard',
      role: 'worker',
      payload: { status: 'running' },
    });

    expect(state.managers[0].contract).toMatchObject({
      id: 'contract-ui',
      version: 2,
      riskLevel: 'medium',
    });
    expect(state.managers[0].items[0].contract?.acceptanceCriteria).toEqual(['Dashboard renders']);
    expect(state.agents['worker-ui'].workContract?.writeScopes).toEqual(['frontend/src/App.tsx']);
  });

  it('records selected fan-out separately from planning maxima and execution slots', () => {
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'hierarchy_fanout_planned',
      payload: {
        manager_count: 2,
        coder_count: 3,
        tester_count: 2,
        child_agent_count: 5,
        max_manager_count: 6,
        max_parallel_managers: 2,
        max_coders_per_manager: 4,
        max_parallel_workers_per_manager: 2,
        max_parallel_workers: 3,
      },
    });
    state = reduceEvent(state, {
      sequence: 2,
      type: 'fanout_selected',
      payload: {
        level: 'manager',
        maximum: 6,
        selected: 2,
        unused_capacity: 4,
        reason: 'Two independent workstreams',
      },
    });
    state = reduceEvent(state, {
      sequence: 3,
      type: 'fanout_selected',
      payload: {
        level: 'worker',
        max_count: 4,
        selected_count: 3,
        manager_id: 'manager-ui',
        workstream_id: 'ws-ui',
      },
    });

    expect(state.fanout).toMatchObject({
      maxManagers: 6,
      maxParallelManagers: 2,
      maxWorkersPerManager: 5,
      maxParallelWorkersPerManager: 2,
      maxParallelWorkers: 3,
      directorSelection: { selected: 2, maximum: 6, unused: 4 },
    });
    expect(state.fanout.managerSelections['ws-ui']).toMatchObject({
      selected: 3,
      maximum: 4,
      unused: 1,
    });
  });

  it('tracks reconciliation, durable effects, and project lease lifecycle without ghost agents', () => {
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'project_lease_acquired',
      project_key: 'c:/workspace',
      fencing_token: 17,
      isolation_level: 'fenced-local-workspace',
    });
    state = reduceEvent(state, {
      sequence: 2,
      type: 'effect_applied',
      effect_id: 'effect-1',
      effect_kind: 'file_patch',
      idempotency_key: 'patch:item-1',
      file_path: 'frontend/src/App.tsx',
      before_sha256: 'before',
      after_sha256: 'after',
    });
    state = reduceEvent(state, {
      sequence: 3,
      type: 'completion_reconciliation',
      balanced: false,
      errors: ['calls are not terminal'],
      workstreams: { planned: 1, terminal: 1, balanced: true, ids: {} },
      work_items: {
        planned: 2,
        completed: 1,
        failed: 1,
        terminal: 2,
        balanced: true,
        ids: { completed: ['item-a'], failed: ['item-b'] },
      },
      agents: { planned: 2, terminal: 2, balanced: true, ids: {} },
      calls: {
        planned: 2,
        completed: 1,
        terminal: 1,
        balanced: false,
        ids: {},
        invalid_ids: ['call-b'],
      },
    });
    state = reduceEvent(state, {
      sequence: 4,
      type: 'project_lease_lost',
      error: 'Fencing token expired',
    });

    expect(state.projectLease).toMatchObject({
      status: 'lost',
      projectKey: 'c:/workspace',
      fencingToken: 17,
      error: 'Fencing token expired',
    });
    expect(state.effects[0]).toMatchObject({
      id: 'effect-1',
      target: 'frontend/src/App.tsx',
      idempotencyKey: 'patch:item-1',
    });
    expect(state.reconciliation?.workItems).toMatchObject({
      planned: 2,
      terminal: 2,
      failed: 1,
    });
    expect(state.reconciliation?.calls.invalidIds).toEqual(['call-b']);
    expect(Object.keys(state.agents)).toEqual([]);
  });

  it('keeps test isolation evidence and recognizes failed status without accepted', () => {
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'test_result',
      agent_instance_id: 'tester-a',
      role: 'tester',
      payload: {
        test_run_id: 'gate-1',
        command: 'npm test',
        status: 'failed',
        requested_isolation: 'container',
        actual_isolation: 'restricted-process',
        isolation_details: 'No container runtime was available',
        output_truncated: true,
      },
    });
    state = reduceEvent(state, {
      sequence: 2,
      type: 'test_result',
      agent_instance_id: 'tester-a',
      role: 'tester',
      payload: {
        testRunId: 'gate-2',
        command: 'npm run typecheck',
        status: 'passed',
      },
    });

    expect(state.agents['tester-a'].tests).toEqual([
      expect.objectContaining({
        id: 'gate-1',
        status: 'failed',
        actualIsolation: 'restricted-process',
        outputTruncated: true,
      }),
      expect.objectContaining({ id: 'gate-2', status: 'passed' }),
    ]);
    expect(state.sandbox).toMatchObject({
      requestedIsolation: 'container',
      actualIsolation: 'restricted-process',
      outputTruncated: true,
    });
  });

  it('isolates model failures, aborts, explicit replays, and account switches by attempt', () => {
    let state = reduceEvent(initialWorkspaceState, {
      sequence: 1,
      type: 'model_request_started',
      agent_instance_id: 'worker-a',
      role: 'worker',
      logical_request_id: 'request-1',
      attempt_id: 'request-1:a1',
      attempt: 1,
      provider: 'provider-a',
      account: 'account-a',
    });
    state = reduceEvent(state, {
      sequence: 2,
      type: 'model_request_failed',
      agent_instance_id: 'worker-a',
      role: 'worker',
      logical_request_id: 'request-1',
      attempt_id: 'request-1:a1',
      attempt: 1,
      provider: 'provider-a',
      account: 'account-a',
      error: 'Rate limited',
      error_type: 'rate_limit',
    });
    state = reduceEvent(state, {
      sequence: 3,
      type: 'account_switch',
      agent_instance_id: 'worker-a',
      role: 'worker',
      logicalRequestId: 'request-1',
      fromAccount: 'account-a',
      toAccount: 'account-b',
      reason: 'rate_limit',
    });
    state = reduceEvent(state, {
      sequence: 4,
      type: 'model_request_replayed',
      agent_instance_id: 'worker-a',
      role: 'worker',
      logicalRequestId: 'request-1',
      attemptId: 'request-1:a2',
      attempt: 2,
      requestRevision: 1,
      provider: 'provider-a',
      accountId: 'account-b',
    });
    state = reduceEvent(state, {
      sequence: 5,
      type: 'model_request_aborted',
      agent_instance_id: 'worker-a',
      role: 'worker',
      logical_request_id: 'request-1',
      attempt_id: 'request-1:a2',
      attempt: 2,
      provider: 'provider-a',
      account: 'account-b',
      error: 'Task stopped',
    });

    expect(state.fanout).toMatchObject({
      requestAttempts: 2,
      replayedRequests: 1,
      failedRequests: 1,
      abortedRequests: 1,
      accountSwitches: 1,
    });
    expect(state.agents['worker-a']).toMatchObject({
      logicalRequestId: 'request-1',
      previousAccount: 'account-a',
      account: 'account-b',
      requestAttempts: 2,
      replayCount: 1,
      accountSwitchCount: 1,
      status: 'stopped',
    });
    expect(state.agents['worker-a'].callAttempts).toEqual([
      expect.objectContaining({
        id: 'request-1:a1',
        status: 'failed',
        errorType: 'rate_limit',
      }),
      expect.objectContaining({
        id: 'request-1:a2',
        status: 'aborted',
        replayed: true,
        account: 'account-b',
      }),
    ]);
    expect(state.usedAccounts).toEqual(['account-a', 'account-b']);
  });

  it('does not count redaction placeholders as provider accounts', () => {
    const state = workspaceReducer(initialWorkspaceState, {
      type: 'events',
      taskId: 'task-redacted',
      events: [
        {
          sequence: 1,
          type: 'model_request_started',
          agent_instance_id: 'worker-redacted',
          role: 'worker',
          account: '[REDACTED]',
          account_ref: 'acct-1234-abcd-5678',
          payload: { stage: 'calling_model' },
        },
      ],
    });

    expect(state.usedAccounts).toEqual(['acct-1234-abcd-5678']);
  });

  it('counts called logical agents by role without counting retries twice', () => {
    const roles = [
      ['director-main', 'director'],
      ['manager-api', 'manager'],
      ['worker-api', 'worker'],
      ['tester-api', 'tester'],
      ['worker-api', 'worker'],
    ] as const;
    const state = workspaceReducer(initialWorkspaceState, {
      type: 'events',
      taskId: 'task-accounting',
      events: roles.map(([agentId, role], index) => ({
        sequence: index + 1,
        type: 'model_request_started',
        agent_instance_id: agentId,
        role,
        attempt_id: `${agentId}:a${index + 1}`,
        logical_request_id: `${agentId}:request`,
        replayed: index === roles.length - 1,
        payload: { stage: 'calling_model' },
      })),
    });

    expect(state.fanout.requestAttempts).toBe(5);
    expect(state.fanout.calledAgentIds).toEqual([
      'director-main',
      'manager-api',
      'worker-api',
      'tester-api',
    ]);
    expect(state.fanout.calledByRole).toEqual({
      director: 1,
      manager: 1,
      worker: 1,
      tester: 1,
    });
  });

  it('hydrates planning capacity from legacy hierarchy snapshots', () => {
    const state = workspaceReducer(initialWorkspaceState, {
      type: 'load-task',
      task: {
        id: 'legacy-task',
        name: 'Legacy hierarchy',
        status: 'INTERRUPTED',
        settings: {
          max_parallel_managers: 4,
          max_workers_per_manager: 6,
          max_parallel_workers: 8,
        },
        hierarchy: {
          requested_manager_count: 2,
          agents: {
            'director-stable': {
              id: 'director-stable',
              role: 'director',
              status: 'completed',
            },
            'manager-api': {
              id: 'manager-api',
              role: 'manager',
              workstream_id: 'api',
              status: 'completed',
            },
            'worker-api': {
              id: 'worker-api',
              role: 'worker',
              manager_id: 'manager-api',
              workstream_id: 'api',
              work_item_id: 'api-contract',
              status: 'completed',
            },
            'tester-api': {
              id: 'tester-api',
              role: 'tester',
              manager_id: 'manager-api',
              workstream_id: 'api',
              status: 'blocked',
            },
          },
          workstreams: {
            api: {
              title: 'API stream',
              status: 'ready',
              requested_worker_count: 3,
              work_items: [{ id: 'api-contract', title: 'API contract', status: 'planned' }],
            },
            ui: { title: 'UI stream', requested_worker_count: 2 },
          },
        },
        events: [
          {
            type: 'hierarchy_fanout_planned',
            sequence: 1,
            manager_count: 2,
            coder_count: 5,
            tester_count: 2,
            child_agent_count: 7,
            max_manager_count: null,
            max_parallel_managers: 4,
            max_parallel_workers: 8,
          },
        ],
      },
    });

    expect(state.fanout).toMatchObject({
      plannedManagers: 2,
      plannedCoders: 5,
      plannedTesters: 2,
      plannedChildren: 7,
      maxManagers: 4,
      maxWorkersPerManager: 6,
      maxParallelWorkers: 8,
    });
    expect(state.graphRevision).toBeGreaterThan(0);
    expect(Object.keys(state.agents)).toEqual([
      'director-stable',
      'manager-api',
      'worker-api',
      'tester-api',
    ]);
    expect(state.directorId).toBe('director-stable');
    expect(state.agents['tester-api'].workItemId).toBeUndefined();
    expect(state.managers).toEqual([
      expect.objectContaining({
        id: 'api',
        title: 'API stream',
        workstreamId: 'api',
        agentId: 'manager-api',
        items: [
          expect.objectContaining({
            id: 'api-contract',
            title: 'API contract',
            agentIds: ['worker-api'],
          }),
        ],
      }),
      expect.objectContaining({ id: 'ui', title: 'UI stream', workstreamId: 'ui' }),
    ]);
  });

  it('records an explicit retained-history gap without creating a ghost agent', () => {
    const state = workspaceReducer(initialWorkspaceState, {
      type: 'event',
      taskId: 'task-gap',
      event: {
        type: 'timeline_gap',
        sequence: 9,
        retained_from_sequence: 10,
        latest_sequence: 40,
        history_incomplete: true,
      },
    });

    expect(state.timeline).toEqual({
      latestSequence: 40,
      retainedFromSequence: 10,
      historyIncomplete: true,
    });
    expect(state.agents).toEqual({});
  });

  it('tracks abandoned and skipped work without treating either as success', () => {
    const state = workspaceReducer(initialWorkspaceState, {
      type: 'events',
      taskId: 'task-terminal-outcomes',
      events: [
        {
          sequence: 1,
          type: 'manager_plan_created',
          agent_instance_id: 'manager-a',
          manager_id: 'manager-a',
          workstream_id: 'stream-a',
          role: 'manager',
          work_items: [
            { id: 'item-abandoned', title: 'Unrecoverable work' },
            { id: 'item-skipped', title: 'No longer needed' },
          ],
        },
        {
          sequence: 2,
          type: 'agent_abandoned',
          agent_instance_id: 'worker-a',
          manager_id: 'manager-a',
          workstream_id: 'stream-a',
          work_item_id: 'item-abandoned',
          role: 'worker',
          reason: 'Retry budget exhausted',
        },
        {
          sequence: 3,
          type: 'work_item_skipped',
          manager_id: 'manager-a',
          workstream_id: 'stream-a',
          work_item_id: 'item-skipped',
          reason: 'Dependency removed the need for this item',
        },
      ],
    });

    expect(state.agents['worker-a']).toMatchObject({
      status: 'abandoned',
      executionState: 'abandoned',
      error: 'Retry budget exhausted',
    });
    expect(state.managers[0].items).toEqual([
      expect.objectContaining({ id: 'item-abandoned', status: 'abandoned' }),
      expect.objectContaining({ id: 'item-skipped', status: 'skipped' }),
    ]);
    expect(state.managers[0].status).toBe('partial');
  });

  it('reconciles Manager terminal reports and preserves partial hierarchy outcomes', () => {
    const task = {
      id: 'task-manager-reports',
      name: 'Manager report barrier',
      status: 'MANAGING',
    };
    const loaded = workspaceReducer(initialWorkspaceState, { type: 'load-task', task });
    const state = workspaceReducer(loaded, {
      type: 'events',
      taskId: task.id,
      events: [
        {
          sequence: 1,
          type: 'plan_created',
          workstreams: [
            { id: 'stream-a', title: 'A' },
            { id: 'stream-b', title: 'B' },
          ],
        },
        {
          sequence: 2,
          type: 'manager_plan_created',
          agent_instance_id: 'manager-a',
          manager_id: 'manager-a',
          workstream_id: 'stream-a',
          role: 'manager',
          work_items: [{ id: 'item-a', title: 'A item' }],
        },
        {
          sequence: 3,
          type: 'manager_plan_created',
          agent_instance_id: 'manager-b',
          manager_id: 'manager-b',
          workstream_id: 'stream-b',
          role: 'manager',
          work_items: [{ id: 'item-b', title: 'B item' }],
        },
        {
          sequence: 4,
          type: 'manager_terminal_report',
          manager_id: 'manager-a',
          workstream_id: 'stream-a',
          status: 'partial',
          completed_item_ids: ['item-a'],
          abandoned_item_ids: ['item-a2'],
          skipped_item_ids: [],
          reasons: ['One item could not finish.'],
        },
        {
          sequence: 5,
          type: 'manager_terminal_report',
          manager_id: 'manager-b',
          workstream_id: 'stream-b',
          status: 'abandoned',
          completed_item_ids: [],
          abandoned_item_ids: ['item-b'],
          skipped_item_ids: [],
          reasons: ['Retry budget exhausted.'],
        },
        {
          sequence: 6,
          type: 'manager_report_barrier',
          expected_manager_ids: ['manager-a', 'manager-b'],
          reported_manager_ids: ['manager-a', 'manager-b'],
          expected_count: 2,
          reported_count: 2,
        },
        {
          sequence: 7,
          type: 'hierarchy_partial',
          summary: 'Useful output was retained.',
        },
        {
          sequence: 8,
          type: 'director_final_review',
          outcome: 'partial',
          verdict: 'accept_partial',
          summary: 'Ship the completed subset.',
        },
      ],
    });

    expect(state.managerReports).toMatchObject({
      barrierSatisfied: true,
      counts: {
        expected: 2,
        reported: 2,
        completed: 0,
        partial: 1,
        abandoned: 1,
        pending: 0,
      },
    });
    expect(state.managerReports.roster.pendingManagerIds).toEqual([]);
    expect(state.managerReports.reports['manager-a']).toMatchObject({
      counts: { planned: 2, completed: 1, abandoned: 1, terminal: 2 },
      workItemIds: {
        completed: ['item-a'],
        abandoned: ['item-a2'],
        skipped: [],
      },
      summary: 'One item could not finish.',
    });
    expect(state.managers).toEqual([
      expect.objectContaining({
        agentId: 'manager-a',
        status: 'partial',
        terminalReport: expect.objectContaining({ outcome: 'partial' }),
      }),
      expect.objectContaining({
        agentId: 'manager-b',
        status: 'abandoned',
        terminalReport: expect.objectContaining({ outcome: 'abandoned' }),
      }),
    ]);
    expect(state.hierarchyOutcome).toBe('partial');
    expect(state.directorFinalReview).toMatchObject({
      outcome: 'partial',
      verdict: 'accept_partial',
    });
    expect(state.task).toMatchObject({
      status: 'PARTIAL',
      phase: 'director_review',
      finished_at: expect.any(String),
    });
    expect(state.agents).toEqual({
      'manager-a': expect.any(Object),
      'manager-b': expect.any(Object),
    });
  });

  it('records crisis and remediation lifecycle events without ghost agents', () => {
    const state = workspaceReducer(
      {
        ...initialWorkspaceState,
        task: { id: 'task-crisis', name: 'Crisis recovery', status: 'RUNNING' },
      },
      {
        type: 'events',
        taskId: 'task-crisis',
        events: [
          {
            sequence: 1,
            type: 'crisis_detected',
            crisis_id: 'crisis-1',
            scope: 'workstream',
            workstream_id: 'stream-a',
            failure_kind: 'manager_review',
            reason: 'Manager stopped responding',
            retryable: true,
            affected_manager_ids: ['manager-a'],
            affected_work_item_ids: ['item-a'],
          },
          {
            sequence: 2,
            type: 'remediation_started',
            workstream_id: 'stream-a',
            crisis_id: 'crisis-1',
            action: 'retry',
            strategy: 'abandon_and_continue',
            instructions: 'Use the retained evidence.',
            summary: 'Preserving completed work.',
            affected_manager_ids: ['manager-a'],
            affected_work_item_ids: ['item-a'],
          },
        ],
      },
    );

    expect(state.crisis).toMatchObject({
      crisisId: 'crisis-1',
      status: 'detected',
      scope: 'workstream',
      failureKind: 'manager_review',
      reason: 'Manager stopped responding',
      retryable: true,
      affectedManagerIds: ['manager-a'],
      affectedWorkItemIds: ['item-a'],
    });
    expect(state.remediation).toMatchObject({
      crisisId: 'crisis-1',
      status: 'started',
      action: 'retry',
      strategy: 'abandon_and_continue',
      instructions: 'Use the retained evidence.',
      affectedManagerIds: ['manager-a'],
      affectedWorkItemIds: ['item-a'],
    });
    expect(state.task).toMatchObject({ status: 'RUNNING', phase: 'remediation_started' });
    expect(state.agents).toEqual({});
  });

  it('hydrates Manager reports and their barrier from a compacted task snapshot', () => {
    const state = workspaceReducer(initialWorkspaceState, {
      type: 'load-task',
      task: {
        id: 'task-report-snapshot',
        name: 'Compacted report snapshot',
        status: 'PARTIAL',
        hierarchy: {
          workstreams: {
            'stream-a': {
              id: 'stream-a',
              title: 'Stream A',
              agent_instance_id: 'manager-a',
              status: 'partial',
            },
          },
          manager_terminal_reports: [
            {
              manager_id: 'manager-a',
              agent_instance_id: 'manager-a',
              workstream_id: 'stream-a',
              status: 'partial',
              completed_item_ids: ['item-a'],
              abandoned_item_ids: ['item-b'],
              skipped_item_ids: [],
              reasons: ['item-b exhausted remediation'],
            },
          ],
          manager_report_barrier: {
            expected_manager_ids: ['manager-a'],
            reported_manager_ids: ['manager-a'],
            expected_count: 1,
            reported_count: 1,
            satisfied: true,
          },
        },
      },
    });

    expect(state.managerReports).toMatchObject({
      barrierSatisfied: true,
      counts: {
        expected: 1,
        reported: 1,
        partial: 1,
        pending: 0,
      },
    });
    expect(state.managers[0]).toMatchObject({
      status: 'partial',
      terminalReport: {
        managerId: 'manager-a',
        outcome: 'partial',
        counts: { planned: 2, terminal: 2, completed: 1, abandoned: 1 },
      },
    });
    expect(state.hierarchyOutcome).toBe('partial');
  });
});
