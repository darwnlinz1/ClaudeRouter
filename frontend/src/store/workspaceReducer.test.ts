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
    expect(state.managers).toEqual([
      expect.objectContaining({
        id: 'api',
        title: 'API stream',
        workstreamId: 'api',
        items: [expect.objectContaining({ id: 'api-contract', title: 'API contract' })],
      }),
      expect.objectContaining({ id: 'ui', title: 'UI stream', workstreamId: 'ui' }),
    ]);
  });
});
