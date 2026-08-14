// Deterministic mocked orchestrator payloads used by the screenshot harness.
const NOW = Date.parse('2026-08-12T09:41:00Z');
const at = (offsetSeconds) => new Date(NOW + offsetSeconds * 1000).toISOString();

const contract = (id, risk, priority) => ({
  id,
  version: 3,
  read_scopes: ['src/**'],
  write_scopes: ['src/workspace/**', 'src/components/**'],
  acceptance_criteria: [
    'Dashboard renders without horizontal overflow at 390px.',
    'All controls keep a 40px hit area.',
  ],
  test_requirements: ['npm test -- --run', 'npx playwright test'],
  evidence_requirements: ['Screenshots at three viewports'],
  risk_level: risk,
  priority,
  approval_policy: 'risk_based',
});

export function buildFixture({ managerCount = 6, workersPerManager = 3 } = {}) {
  const events = [];
  let sequence = 0;
  const push = (event) => {
    sequence += 1;
    events.push({ sequence, timestamp: at(sequence * 3), ...event });
  };

  const directorId = 'director-01';
  push({
    type: 'hierarchy_fanout_planned',
    payload: {
      manager_count: managerCount,
      coder_count: managerCount * workersPerManager,
      tester_count: managerCount,
      child_agent_count: managerCount * (workersPerManager + 1),
      max_manager_count: 32,
      max_parallel_managers: 6,
      max_coders_per_manager: 31,
      max_parallel_workers_per_manager: 4,
      max_parallel_workers: 12,
    },
  });
  push({
    type: 'fanout_selected',
    payload: {
      level: 'manager',
      maximum: 32,
      selected: managerCount,
      reason: 'The plan needs six independent workstreams; the remaining cap stays unused.',
    },
  });

  const workstreams = Array.from({ length: managerCount }, (_, index) => ({
    id: `stream-${index + 1}`,
    title: WORKSTREAM_TITLES[index % WORKSTREAM_TITLES.length],
    dependencies: index > 1 ? [`stream-${index - 1}`] : [],
    contract: contract(`contract-stream-${index + 1}`, index === 2 ? 'high' : 'medium', index + 1),
  }));
  push({ type: 'plan_created', payload: { workstreams } });

  push({
    type: 'agent_started',
    agent_instance_id: directorId,
    role: 'director',
    payload: {
      name: 'Director',
      status: 'running',
      model: 'claude-sonnet-5',
      effort: 'max',
      goal: 'Split the redesign into independent workstreams and reconcile the results.',
    },
  });
  push({
    type: 'thinking',
    agent_instance_id: directorId,
    role: 'director',
    payload: {
      text: 'Six workstreams keep the write scopes disjoint, so managers can run in parallel without a shared lock.',
    },
  });

  const statuses = ['running', 'running', 'failed', 'passed', 'blocked', 'running'];

  workstreams.forEach((stream, index) => {
    const managerId = `manager-${index + 1}`;
    const managerStatus = statuses[index % statuses.length];
    push({
      type: 'agent_started',
      agent_instance_id: managerId,
      workstream_id: stream.id,
      role: 'manager',
      payload: {
        name: stream.title,
        status: managerStatus === 'failed' ? 'running' : managerStatus,
        model: 'claude-sonnet-5',
        effort: 'high',
        contract: stream.contract,
        goal: `Deliver ${stream.title.toLowerCase()} with evidence.`,
      },
    });
    push({
      type: 'agent_message',
      agent_instance_id: directorId,
      role: 'director',
      payload: {
        source_agent_id: directorId,
        target_agent_id: managerId,
        signal_type: 'assign_workstream',
        summary: `Assigned ${stream.title} to ${managerId}.`,
      },
    });
    push({
      type: 'manager_plan_created',
      agent_instance_id: managerId,
      workstream_id: stream.id,
      role: 'manager',
      payload: {
        workstream_title: stream.title,
        work_items: Array.from({ length: workersPerManager }, (_, itemIndex) => ({
          id: `${stream.id}-item-${itemIndex + 1}`,
          title: `${WORK_ITEM_TITLES[(index + itemIndex) % WORK_ITEM_TITLES.length]}`,
          dependencies: itemIndex > 0 ? [`${stream.id}-item-${itemIndex}`] : [],
          contract: contract(
            `contract-${stream.id}-${itemIndex + 1}`,
            itemIndex === 0 ? 'medium' : 'low',
            itemIndex + 1,
          ),
        })),
      },
    });

    for (let workerIndex = 0; workerIndex < workersPerManager; workerIndex += 1) {
      const workerId = `worker-${index + 1}-${workerIndex + 1}`;
      const itemId = `${stream.id}-item-${workerIndex + 1}`;
      const workerStatus = statuses[(index + workerIndex) % statuses.length];
      push({
        type: 'agent_started',
        agent_instance_id: workerId,
        manager_id: managerId,
        workstream_id: stream.id,
        work_item_id: itemId,
        role: 'worker',
        payload: {
          name: `${WORK_ITEM_TITLES[(index + workerIndex) % WORK_ITEM_TITLES.length]}`,
          status: workerStatus === 'failed' ? 'running' : workerStatus,
          model: 'claude-sonnet-5',
          effort: 'medium',
          files: [`src/components/${stream.id}-${workerIndex + 1}.tsx`],
          goal: 'Implement the assigned slice and prove it with tests.',
        },
      });
      push({
        type: 'model_request_started',
        agent_instance_id: workerId,
        manager_id: managerId,
        workstream_id: stream.id,
        role: 'worker',
        account_ref: `acct-${(index % 3) + 1}`,
        provider: 'web_claude',
        attempt: 1,
        attempt_id: `${workerId}:a1`,
        payload: { logical_request_id: `req-${workerId}`, request_revision: 1 },
      });
      if (workerStatus === 'failed') {
        push({
          type: 'model_request_failed',
          agent_instance_id: workerId,
          manager_id: managerId,
          role: 'worker',
          provider: 'web_claude',
          attempt: 1,
          attempt_id: `${workerId}:a1`,
          payload: { error: 'Upstream returned 429 before the first token.' },
        });
        push({
          type: 'account_switch',
          agent_instance_id: workerId,
          manager_id: managerId,
          role: 'worker',
          payload: {
            from_account: `acct-${(index % 3) + 1}`,
            to_account: `acct-${((index + 1) % 3) + 1}`,
            reason: 'rate limited',
          },
        });
      } else {
        push({
          type: 'model_request_completed',
          agent_instance_id: workerId,
          manager_id: managerId,
          role: 'worker',
          provider: 'web_claude',
          attempt: 1,
          attempt_id: `${workerId}:a1`,
          payload: { logical_request_id: `req-${workerId}` },
        });
      }
      push({
        type: 'thinking',
        agent_instance_id: workerId,
        manager_id: managerId,
        role: 'worker',
        provisional: workerIndex === 0,
        payload: {
          text: 'Reading the current styles, then replacing the ad-hoc values with tokens.',
          attempt_id: `${workerId}:a1`,
          chunk_index: 0,
        },
      });
      push({
        type: 'token',
        agent_instance_id: workerId,
        manager_id: managerId,
        role: 'worker',
        committed: true,
        payload: {
          text: 'Applied the shared control tokens and verified the contrast ratio at AA.',
        },
      });
      push({
        type: 'agent_message',
        agent_instance_id: managerId,
        role: 'manager',
        payload: {
          source_agent_id: managerId,
          target_agent_id: workerId,
          signal_type: 'assign_work_item',
          summary: `Assigned ${itemId}.`,
        },
      });
      if (workerIndex === 0) {
        push({
          type: 'agent_message',
          agent_instance_id: workerId,
          role: 'worker',
          payload: {
            source_agent_id: workerId,
            target_agent_id: managerId,
            signal_type: 'patch_submitted',
            summary: 'Submitted the token refactor for review.',
          },
        });
      }
    }

    const testerId = `tester-${index + 1}`;
    push({
      type: 'agent_started',
      agent_instance_id: testerId,
      manager_id: managerId,
      workstream_id: stream.id,
      role: 'tester',
      payload: {
        name: `${stream.title} gate`,
        status: index % 2 === 0 ? 'running' : 'passed',
        model: 'claude-sonnet-5',
        effort: 'high',
      },
    });
    push({
      type: 'test_result',
      agent_instance_id: testerId,
      manager_id: managerId,
      workstream_id: stream.id,
      role: 'tester',
      payload: {
        name: 'npm test -- --run',
        status: index === 2 ? 'failed' : 'passed',
        accepted: index !== 2,
        detail: index === 2 ? '1 failing assertion in workspaceReducer.' : '128 assertions passed.',
        duration_ms: 4200 + index * 130,
        requested_isolation: 'container',
        actual_isolation: index === 2 ? 'host' : 'container',
        isolation_details: index === 2 ? 'Container runtime unavailable.' : 'Rootless container.',
      },
    });
    push({
      type: 'agent_message',
      agent_instance_id: testerId,
      role: 'tester',
      payload: {
        source_agent_id: testerId,
        target_agent_id: managerId,
        signal_type: 'review_result',
        summary: index === 2 ? 'Gate failed; requesting a revision.' : 'Gate passed.',
      },
    });
    push({
      type: 'effect_applied',
      agent_instance_id: managerId,
      role: 'manager',
      payload: {
        effect_id: `effect-${index + 1}`,
        effect_kind: 'file_write',
        file_path: `src/components/${stream.id}.tsx`,
        before_sha256: `9f1${index}c4ab77de10cc51`,
        after_sha256: `2b7${index}ee31099ac4410d`,
      },
    });
    push({
      type: 'fanout_selected',
      workstream_id: stream.id,
      payload: {
        level: 'worker',
        maximum: 31,
        selected: workersPerManager,
        workstream_id: stream.id,
        reason: 'Only three disjoint slices were needed for this workstream.',
      },
    });
  });

  push({
    type: 'project_lease_acquired',
    payload: {
      project_key: 'demo-workspace',
      fencing_token: 42,
      isolation_level: 'exclusive project lease',
    },
  });
  push({
    type: 'completion_reconciliation',
    payload: {
      balanced: false,
      errors: ['One workstream still has a failing machine gate.'],
      workstreams: { planned: managerCount, terminal: managerCount - 1, balanced: false },
      work_items: {
        planned: managerCount * workersPerManager,
        terminal: managerCount * workersPerManager - 2,
        balanced: false,
      },
      agents: {
        planned: managerCount * (workersPerManager + 1) + 1,
        terminal: managerCount * workersPerManager,
        balanced: false,
      },
      calls: {
        planned: managerCount * workersPerManager,
        terminal: managerCount * workersPerManager - 1,
        balanced: false,
      },
    },
  });

  const task = {
    id: 'task-preview',
    name: 'Redesign the orchestrator operator surface',
    prompt: PROMPT,
    root: 'C:/workspace/ai-orchestrator',
    mode: 'orchestrator',
    status: 'CODING',
    phase: 'coding',
    current_agent: 'Director',
    turn_count: 14,
    created_at: at(-5400),
    started_at: at(-5400),
    updated_at: at(sequence * 3),
    settings: {
      max_managers: 32,
      max_parallel_managers: 6,
      max_workers_per_manager: 32,
      max_parallel_workers: 12,
    },
    changed_files: Object.fromEntries(
      Array.from({ length: 9 }, (_, index) => [
        `src/components/${['App', 'TaskDashboard', 'DagOverview', 'DockWorkspace', 'AgentWindow', 'EventConsole', 'CommandPalette', 'WorkspaceSplitter', 'styles'][index]}.tsx`,
        { additions: 40 + index * 17, deletions: 8 + index * 3 },
      ]),
    ),
    artifact: { status: 'ready', zip_path: '/tmp/task-preview.zip' },
    events: [],
    hierarchy: {},
  };

  const otherTasks = [
    {
      ...task,
      id: 'task-completed',
      name: 'Harden the durable event catalog',
      status: 'COMPLETED',
      phase: 'finished',
      updated_at: at(-3600),
      turn_count: 22,
    },
    {
      ...task,
      id: 'task-failed',
      name: 'Migrate provider cookie quarantine',
      status: 'FAILED',
      phase: 'error',
      root: 'C:/workspace/provider-lab',
      updated_at: at(-9000),
      turn_count: 6,
    },
    {
      ...task,
      id: 'task-waiting',
      name: 'Introduce release compatibility gates',
      status: 'WAITING_INPUT',
      phase: 'reviewing',
      root: 'C:/workspace/release-tools',
      updated_at: at(-600),
      turn_count: 11,
    },
  ];

  return { task, tasks: [task, ...otherTasks], events };
}

const PROMPT = `Bạn là Director của một hệ thống nhiều agent. Hãy thiết kế lại toàn bộ giao diện, tạo design tokens, refactor component, chạy kiểm thử và báo cáo lại kết quả kèm ảnh chụp màn hình trước và sau ở cả ba viewport.`;

const WORKSTREAM_TITLES = [
  'Design tokens and control system',
  'Task rail and header',
  'Execution graph',
  'Agent workspace dock',
  'Settings and launch wizard',
  'Responsive and accessibility pass',
  'Event console',
  'Documentation refresh',
];

const WORK_ITEM_TITLES = [
  'Extract colour and spacing tokens',
  'Rebuild the button and tab primitives',
  'Rework the task card layout',
  'Compact the task header toolbar',
  'Curve and colour the graph edges',
  'Add a real fullscreen graph mode',
  'Persist the workspace splitter',
  'Group planning and concurrency caps',
  'Audit contrast at AA',
];

export const ACCOUNTS = {
  accounts: [
    {
      account_id: 'acct-1',
      provider: 'web_claude',
      state: 'available',
      active_leases: 2,
      cooldown_active: false,
    },
    {
      account_id: 'acct-2',
      provider: 'web_claude',
      state: 'cooldown',
      active_leases: 0,
      cooldown_active: true,
      cooldown_until: Math.floor(NOW / 1000) + 900,
      reason: 'Rate limited by upstream',
    },
    {
      account_id: 'acct-3',
      provider: 'web_claude',
      state: 'available',
      active_leases: 1,
      cooldown_active: false,
    },
  ],
  active_leases: [],
};

export const APPROVALS = {
  approvals: [
    {
      approval_id: 'approval-1',
      task_id: 'task-preview',
      workstream_id: 'stream-3',
      kind: 'destructive_action',
      target: 'src/components/DagOverview.tsx',
      reason: 'The patch rewrites a shared module.',
      status: 'pending',
      created_at: at(-120),
    },
  ],
};
