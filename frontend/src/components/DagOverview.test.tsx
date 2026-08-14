// @vitest-environment jsdom

import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeAll, describe, expect, test } from 'vitest';
import { initialWorkspaceState, workspaceReducer } from '../store/workspaceReducer';
import type { AgentInstance, WorkspaceState } from '../types';
import { buildGraph, DagOverview } from './DagOverview';

beforeAll(() => {
  // React Flow measures its container; jsdom has neither observer.
  class Observer {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  globalThis.ResizeObserver ??= Observer as unknown as typeof ResizeObserver;
  globalThis.DOMMatrixReadOnly ??= class {
    m22 = 1;
    constructor() {}
  } as unknown as typeof DOMMatrixReadOnly;
});

// Vitest runs without globals here, so RTL never registers its own cleanup and
// renders would otherwise pile up in one shared document.
afterEach(cleanup);

const agent = (id: string, role: AgentInstance['role'], extra: Partial<AgentInstance> = {}) =>
  ({
    id,
    role,
    title: id,
    status: 'running',
    model: 'claude-sonnet-5',
    effort: 'max',
    stream: [],
    thinking: [],
    response: '',
    tools: [],
    unread: 0,
    replayCount: 0,
    ...extra,
  }) as AgentInstance;

const renderGraph = (state: WorkspaceState) =>
  render(<DagOverview state={state} onOpenAgent={() => undefined} />);

describe('DagOverview', () => {
  test('renders an empty graph when the run has no agents or workstreams', () => {
    // A run that failed before the director produced a plan reaches the graph
    // with empty hierarchy.agents and hierarchy.workstreams.
    expect(() => renderGraph(initialWorkspaceState)).not.toThrow();
    expect(screen.getByLabelText('Agent neural network')).toBeTruthy();
    expect(screen.getByText(/Agent nodes appear when Director starts planning/)).toBeTruthy();
  });

  test('an empty graph after failed model calls explains the run died on the way in', () => {
    renderGraph({
      ...initialWorkspaceState,
      fanout: {
        ...initialWorkspaceState.fanout,
        requestAttempts: 3,
        abortedRequests: 1,
        calledAgentIds: ['director_1'],
      },
    });
    expect(screen.getByText(/stopped before any agent was recorded/)).toBeTruthy();
    expect(screen.getByText(/3 model attempts, 1 aborted/)).toBeTruthy();
  });

  test('renders a director with no managers yet', () => {
    const director = agent('director_1', 'director');
    expect(() =>
      renderGraph({
        ...initialWorkspaceState,
        directorId: director.id,
        agents: { [director.id]: director },
      }),
    ).not.toThrow();
  });

  test('renders planned managers before any agent exists', () => {
    const state = {
      ...initialWorkspaceState,
      fanout: { ...initialWorkspaceState.fanout, plannedManagers: 4 },
    };
    const graph = buildGraph(state, 'swimlane', null, 1.6);
    expect(graph.nodes).toHaveLength(5);
    expect(
      graph.nodes.filter((node) => (node.data as { role?: string }).role === 'manager'),
    ).toHaveLength(4);
    expect(graph.edges).toHaveLength(4);
    expect(() => renderGraph(state)).not.toThrow();
  });

  test('surfaces a retained-history gap while using snapshot topology', () => {
    renderGraph({
      ...initialWorkspaceState,
      fanout: { ...initialWorkspaceState.fanout, plannedManagers: 1 },
      timeline: {
        latestSequence: 40,
        retainedFromSequence: 12,
        historyIncomplete: true,
      },
    });

    expect(screen.getByText('History compacted')).toBeTruthy();
  });

  test('renders the director even when directorId was never set', () => {
    // stateFromTask hydrates agents without ever setting directorId; only a
    // director event does that.
    const director = agent('director_1', 'director');
    renderGraph({ ...initialWorkspaceState, agents: { [director.id]: director } });
    expect(screen.queryByText(/Agent nodes appear when Director/)).toBeNull();
  });

  test('renders the first director when directorId points at a later supervisor', () => {
    // roleValue() folds supervisor into director, so a supervisor event moves
    // directorId off the real director.
    const director = agent('director_1', 'director', { title: 'Planning director' });
    const supervisor = agent('supervisor_1', 'director', { title: 'Escalation supervisor' });
    renderGraph({
      ...initialWorkspaceState,
      directorId: supervisor.id,
      agents: { [director.id]: director, [supervisor.id]: supervisor },
    });
    expect(screen.getByText('Planning director')).toBeTruthy();
    expect(screen.getByText('Escalation supervisor')).toBeTruthy();
  });

  test('renders agents whose backend role did not map to a known role', () => {
    // roleValue() returns 'agent' for anything it does not recognise, e.g. the
    // backend calling a coder something other than "worker".
    const director = agent('director_1', 'director');
    const manager = agent('mgr_1', 'manager', { workstreamId: 'stream-1' });
    const coder = agent('coder_1', 'agent', { managerId: 'mgr_1', title: 'Coder one' });
    renderGraph({
      ...initialWorkspaceState,
      directorId: director.id,
      agents: { [director.id]: director, [manager.id]: manager, [coder.id]: coder },
      managers: [
        {
          id: 'stream-1',
          title: 'Stream one',
          status: 'running',
          dependencies: [],
          agentId: 'mgr_1',
          workstreamId: 'stream-1',
          items: [],
        },
      ],
    });
    expect(screen.getByText('Coder one')).toBeTruthy();
  });

  test('hangs a worker off the manager that called it when no parent id was set', () => {
    const director = agent('director_1', 'director');
    const manager = agent('mgr_1', 'manager', { workstreamId: 'stream-1' });
    // No managerId and no workstreamId: the only record of the delegation is
    // the call itself.
    const worker = agent('wrk_1', 'worker', { title: 'Orphan coder' });
    const state: WorkspaceState = {
      ...initialWorkspaceState,
      directorId: director.id,
      agents: { [director.id]: director, [manager.id]: manager, [worker.id]: worker },
      managers: [
        {
          id: 'stream-1',
          title: 'Stream one',
          status: 'running',
          dependencies: [],
          agentId: 'mgr_1',
          workstreamId: 'stream-1',
          items: [],
        },
      ],
      signals: [
        {
          id: 'sig-1',
          sequence: 1,
          sourceAgentId: 'mgr_1',
          targetAgentId: 'wrk_1',
          signalType: 'work_assigned',
          summary: 'Assigned work item',
          timestamp: '2026-08-12T09:00:00Z',
        },
      ],
    };
    const { edges } = buildGraph(state, 'tree', null, 1.9);
    expect(edges.some((edge) => edge.source === 'mgr_1' && edge.target === 'wrk_1')).toBe(true);
    expect(edges.some((edge) => edge.source === 'director_1' && edge.target === 'wrk_1')).toBe(
      false,
    );
    renderGraph(state);
    expect(screen.getByText('Orphan coder')).toBeTruthy();
  });

  test('keeps workstreams apart when their manager ids were redacted', () => {
    // The backend used to redact "manager_<24 hex>" (32 chars) but not
    // "worker_<24 hex>" (31), so every workstream arrived carrying the same
    // "[REDACTED]" agent id. They collapsed into one column and their workers,
    // whose manager_id was that same placeholder, fell back to the director.
    const director = agent('director_1', 'director');
    const streams = ['core-pipeline', 'backend-api', 'web-frontend'];
    const workers = streams.map((stream, index) =>
      agent(`worker_${index}`, 'worker', { workstreamId: stream, title: `Coder ${index}` }),
    );
    const state: WorkspaceState = {
      ...initialWorkspaceState,
      directorId: director.id,
      agents: {
        [director.id]: director,
        ...Object.fromEntries(workers.map((worker) => [worker.id, worker])),
      },
      managers: streams.map((stream) => ({
        id: stream,
        title: stream,
        status: 'running',
        dependencies: [],
        // identityValue() drops the placeholder, so this arrives undefined.
        agentId: undefined,
        workstreamId: stream,
        items: [],
      })),
    };

    const { nodes, edges } = buildGraph(state, 'tree', null, 1.9);

    const managerNodes = nodes.filter((node) => String(node.id).startsWith('mgr-slot:'));
    expect(managerNodes).toHaveLength(3);

    streams.forEach((stream, index) => {
      expect(
        edges.some(
          (edge) => edge.source === `mgr-slot:${stream}` && edge.target === `worker_${index}`,
        ),
      ).toBe(true);
      expect(
        edges.some((edge) => edge.source === 'director_1' && edge.target === `worker_${index}`),
      ).toBe(false);
    });
  });

  test('shows the ordinal label and the account rather than an opaque id', () => {
    const director = agent('director_1', 'director');
    const manager = agent('mgr_1', 'manager', { workstreamId: 'stream-1', label: 'Manager 2' });
    const worker = agent('wrk_1', 'worker', {
      managerId: 'mgr_1',
      title: 'Thread-safe ApplicationState and ConfigManager',
      label: 'Worker 2.1',
      account: 'bb@gmail.com_default_claude_ai.txt',
    });
    renderGraph({
      ...initialWorkspaceState,
      directorId: director.id,
      agents: { [director.id]: director, [manager.id]: manager, [worker.id]: worker },
      managers: [
        {
          id: 'stream-1',
          title: 'Stream one',
          status: 'running',
          dependencies: [],
          agentId: 'mgr_1',
          workstreamId: 'stream-1',
          items: [],
        },
      ],
    });

    expect(screen.getByText('Worker 2.1')).toBeTruthy();
    expect(screen.getByText('bb@gmail.com_default_claude_ai.txt')).toBeTruthy();
    expect(screen.getByText('Manager 2')).toBeTruthy();
  });

  test('distinguishes partial, abandoned, and skipped nodes and reports their counts', () => {
    const completed = agent('completed-agent', 'worker', { status: 'passed' });
    const partial = agent('partial-agent', 'worker', { status: 'partial' });
    const abandoned = agent('abandoned-agent', 'worker', { status: 'abandoned' });
    const skipped = agent('skipped-agent', 'worker', { status: 'skipped' });

    renderGraph({
      ...initialWorkspaceState,
      agents: {
        [completed.id]: completed,
        [partial.id]: partial,
        [abandoned.id]: abandoned,
        [skipped.id]: skipped,
      },
    });

    expect(screen.getByText('1 completed')).toBeTruthy();
    expect(screen.getByText('1 partial')).toBeTruthy();
    expect(screen.getByText('1 abandoned')).toBeTruthy();
    expect(screen.getByText('1 skipped')).toBeTruthy();
    expect(screen.getByLabelText('partial-agent, worker, partial').className).toContain(
      'tone-partial',
    );
    expect(screen.getByLabelText('abandoned-agent, worker, abandoned').className).toContain(
      'tone-abandoned',
    );
    expect(screen.getByLabelText('skipped-agent, worker, skipped').className).toContain(
      'tone-skipped',
    );
  });

  test('renders a full hierarchy without throwing', () => {
    const director = agent('director_1', 'director');
    const manager = agent('mgr_1', 'manager', { workstreamId: 'stream-1' });
    const worker = agent('wrk_1', 'worker', { managerId: 'mgr_1' });
    const tester = agent('tst_1', 'tester', { managerId: 'mgr_1' });
    expect(() =>
      renderGraph({
        ...initialWorkspaceState,
        directorId: director.id,
        agents: {
          [director.id]: director,
          [manager.id]: manager,
          [worker.id]: worker,
          [tester.id]: tester,
        },
        managers: [
          {
            id: 'stream-1',
            title: 'Stream one',
            status: 'running',
            dependencies: [],
            agentId: 'mgr_1',
            workstreamId: 'stream-1',
            items: [],
          },
        ],
      }),
    ).not.toThrow();
  });

  test('uses feed-forward neural layers for the default network view', () => {
    const director = agent('director_1', 'director');
    const manager = agent('mgr_1', 'manager', { workstreamId: 'stream-1' });
    const worker = agent('wrk_1', 'worker', { managerId: 'mgr_1' });
    const tester = agent('tst_1', 'tester', { managerId: 'mgr_1' });
    const state: WorkspaceState = {
      ...initialWorkspaceState,
      directorId: director.id,
      agents: {
        [director.id]: director,
        [manager.id]: manager,
        [worker.id]: worker,
        [tester.id]: tester,
      },
      managers: [
        {
          id: 'stream-1',
          title: 'Stream one',
          status: 'running',
          dependencies: [],
          agentId: manager.id,
          workstreamId: 'stream-1',
          items: [],
        },
      ],
    };

    const graph = buildGraph(state, 'neural', null, 1.9);
    const byId = new Map(graph.nodes.map((node) => [node.id, node]));
    const directorNode = byId.get(director.id);
    const managerNode = byId.get(manager.id);
    const workerNode = byId.get(worker.id);
    const testerNode = byId.get(tester.id);

    expect(directorNode).toBeDefined();
    expect(managerNode).toBeDefined();
    expect(workerNode).toBeDefined();
    expect(testerNode).toBeDefined();
    expect(directorNode!.position.x).toBeLessThan(managerNode!.position.x);
    expect(managerNode!.position.x).toBeLessThan(workerNode!.position.x);
    expect(workerNode!.position.x).toBeLessThan(testerNode!.position.x);
    expect(String(workerNode!.className)).toContain('neural');
    expect(
      graph.edges.some((edge) => edge.source === manager.id && edge.target === worker.id),
    ).toBe(true);
    expect(graph.edges.some((edge) => edge.source === worker.id && edge.target === tester.id)).toBe(
      true,
    );
    expect(
      graph.edges.some((edge) => edge.source === manager.id && edge.target === tester.id),
    ).toBe(false);
    expect(graph.edges.every((edge) => (edge.data as { neural?: boolean }).neural)).toBe(true);
  });

  test('keeps all 25 logical agents in a four-by-four hierarchy', () => {
    const events: Record<string, unknown>[] = [
      {
        sequence: 1,
        type: 'agent_started',
        agent_instance_id: 'director-1',
        role: 'director',
        status: 'planning',
        title: 'Director',
      },
      {
        sequence: 2,
        type: 'plan_created',
        agent_instance_id: 'director-1',
        role: 'director',
        requested_manager_count: 4,
        workstreams: Array.from({ length: 4 }, (_, index) => ({
          id: `stream-${index + 1}`,
          title: `Stream ${index + 1}`,
          dependencies: [],
        })),
      },
    ];
    let sequence = events.length;
    for (let managerIndex = 1; managerIndex <= 4; managerIndex += 1) {
      const managerId = `manager-${managerIndex}`;
      const workstreamId = `stream-${managerIndex}`;
      events.push({
        sequence: ++sequence,
        type: 'agent_started',
        agent_instance_id: managerId,
        workstream_id: workstreamId,
        role: 'manager',
        status: 'queued',
        title: `Manager ${managerIndex}`,
      });
      for (let workerIndex = 1; workerIndex <= 4; workerIndex += 1) {
        events.push({
          sequence: ++sequence,
          type: 'agent_started',
          agent_instance_id: `worker-${managerIndex}-${workerIndex}`,
          manager_id: managerId,
          workstream_id: workstreamId,
          work_item_id: `${workstreamId}:item-${workerIndex}`,
          role: 'worker',
          status: 'queued',
          title: `Worker ${managerIndex}.${workerIndex}`,
        });
      }
      events.push({
        sequence: ++sequence,
        type: 'agent_started',
        agent_instance_id: `tester-${managerIndex}`,
        manager_id: managerId,
        workstream_id: workstreamId,
        role: 'tester',
        status: 'queued',
        title: `Tester ${managerIndex}`,
      });
    }
    events.push({
      sequence: sequence + 1,
      type: 'hierarchy_fanout_planned',
      agent_instance_id: 'director-1',
      role: 'director',
      manager_count: 4,
      coder_count: 16,
      tester_count: 4,
      child_agent_count: 20,
      primary_agent_count: 25,
    });

    const state = workspaceReducer(initialWorkspaceState, {
      type: 'events',
      taskId: 'task-4x4',
      events,
    });
    const graph = buildGraph(state, 'tree', null, 1.6);

    expect(Object.keys(state.agents)).toHaveLength(25);
    expect(state.managers).toHaveLength(4);
    expect(graph.nodes).toHaveLength(25);
    expect(new Set(graph.nodes.map((node) => node.id))).toHaveLength(25);
    expect(graph.edges.filter((edge) => String(edge.id).startsWith('base:'))).toHaveLength(24);
    renderGraph(state);
    expect(screen.getByText(/25 primary planned/)).toBeTruthy();
  });
});
