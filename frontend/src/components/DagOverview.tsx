import { memo, useEffect, useMemo, useRef, useState, type KeyboardEvent } from 'react';
import {
  Background,
  BaseEdge,
  Controls,
  Handle,
  MarkerType,
  MiniMap,
  Position,
  ReactFlow,
  getBezierPath,
  getSmoothStepPath,
  useEdgesState,
  useNodesState,
  useReactFlow,
  type Edge,
  type EdgeProps,
  type Node,
  type NodeProps,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import {
  Bot,
  Crosshair,
  GitBranch,
  LayoutGrid,
  Maximize2,
  Minimize2,
  Waypoints,
} from 'lucide-react';
import type { AgentInstance, ManagerNode, WorkspaceState } from '../types';

interface DagOverviewProps {
  state: WorkspaceState;
  onOpenAgent: (agent: AgentInstance) => void;
  expanded?: boolean;
  onToggleExpand?: () => void;
}

type LayoutMode = 'tree' | 'swimlane' | 'radial';
const LAYOUTS: LayoutMode[] = ['tree', 'swimlane', 'radial'];

const moveLayoutFocus = (
  event: KeyboardEvent<HTMLButtonElement>,
  index: number,
  onSelect: (layout: LayoutMode) => void,
) => {
  let nextIndex: number | undefined;
  if (event.key === 'ArrowRight') nextIndex = (index + 1) % LAYOUTS.length;
  else if (event.key === 'ArrowLeft') nextIndex = (index - 1 + LAYOUTS.length) % LAYOUTS.length;
  else if (event.key === 'Home') nextIndex = 0;
  else if (event.key === 'End') nextIndex = LAYOUTS.length - 1;
  if (nextIndex == null) return;
  event.preventDefault();
  const controls =
    event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>('[role="tab"]');
  onSelect(LAYOUTS[nextIndex]);
  controls?.[nextIndex]?.focus();
};

type AgentNodeData = {
  role: string;
  title: string;
  status: string;
  dimmed: boolean;
  heat: number;
  contract?: string;
  requestContext?: string;
};

const CHILD_ROLES = new Set(['worker', 'tester', 'reviewer']);
const LEGACY_MANAGER_ID = 'manager:legacy';

const signalColor = (type: string) => {
  if (type.includes('assign') || type.includes('delegate')) return '#4f8bd6';
  if (type.includes('patch') || type.includes('approve')) return '#45a675';
  if (type.includes('review') || type.includes('ask') || type.includes('result')) return '#c58a35';
  if (type.includes('error') || type.includes('fail') || type.includes('reject')) return '#d85b67';
  return '#6f8fb8';
};

const heatOpacity = (agent: AgentInstance) => {
  if (agent.status === 'running' || agent.status === 'waiting') return 1;
  if (agent.status === 'failed') return 0.85;
  if (agent.status === 'passed') return 0.75;
  return 0.55;
};

const AgentFlowNode = memo(function AgentFlowNode({ data }: NodeProps) {
  const node = data as unknown as AgentNodeData;
  return (
    <>
      <Handle
        type="target"
        position={Position.Top}
        id="in"
        className="flow-handle"
        isConnectable={false}
      />
      <div
        className={`flow-agent-label role-${node.role} status-${node.status} ${node.dimmed ? 'dimmed' : ''}`}
        style={{ opacity: node.dimmed ? 0.28 : node.heat }}
        title={`${node.title} · ${node.role} · ${node.status}`}
        aria-label={`${node.title}, ${node.role}, ${node.status}`}
      >
        <span>{node.role.slice(0, 2).toUpperCase()}</span>
        <div>
          <small>{node.role}</small>
          <strong title={node.title}>{node.title}</strong>
          {node.contract && (
            <span className="flow-agent-meta" title={node.contract}>
              {node.contract}
            </span>
          )}
          {node.requestContext && (
            <span className="flow-agent-meta" title={node.requestContext}>
              {node.requestContext}
            </span>
          )}
        </div>
        <i />
      </div>
      <Handle
        type="source"
        position={Position.Bottom}
        id="out"
        className="flow-handle"
        isConnectable={false}
      />
    </>
  );
});

const nodeTypes = { agent: AgentFlowNode };

type LinkEdgeData = {
  kind: 'hierarchy' | 'signal';
  pulse?: boolean;
  color?: string;
  signalType?: string;
};

function LinkEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  markerEnd,
  data,
}: EdgeProps) {
  const link = (data ?? {}) as LinkEdgeData;
  const hierarchy = link.kind !== 'signal';
  const pulse = Boolean(link.pulse);
  const color = link.color ?? (hierarchy ? '#60758c' : '#7094bf');
  const [path] = hierarchy
    ? getSmoothStepPath({
        sourceX,
        sourceY,
        targetX,
        targetY,
        sourcePosition,
        targetPosition,
        borderRadius: 18,
        offset: 26,
      })
    : getBezierPath({
        sourceX,
        sourceY,
        targetX,
        targetY,
        sourcePosition,
        targetPosition,
        curvature: 0.32,
      });
  const strokeWidth = pulse ? 2.6 : hierarchy ? 1.8 : 1.65;

  return (
    <g className={`flow-link-edge kind-${link.kind ?? 'hierarchy'}${pulse ? ' is-live' : ''}`}>
      <BaseEdge
        id={`${id}:casing`}
        path={path}
        style={{
          stroke: '#080d13',
          strokeWidth: strokeWidth + 4,
          opacity: hierarchy ? 0.72 : 0.86,
        }}
        className="flow-link-edge-casing"
      />
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        style={{
          stroke: color,
          strokeWidth,
          strokeDasharray: hierarchy ? undefined : pulse ? '7 6' : '3 7',
          opacity: pulse ? 1 : hierarchy ? 0.82 : 0.72,
        }}
        className="flow-link-edge-path"
      />
      {pulse && (
        <circle r={4} fill="#080d13" stroke={color} strokeWidth={2} className="signal-packet">
          <animateMotion dur="1.15s" repeatCount="indefinite" path={path} />
        </circle>
      )}
    </g>
  );
}

const edgeTypes = {
  link: LinkEdge,
  // aliases kept for safety if any stale edges linger
  hierarchy: LinkEdge,
  signal: LinkEdge,
};

const managerKeys = (manager: ManagerNode, managerAgent?: AgentInstance) => {
  const keys = new Set<string>([manager.id]);
  if (manager.agentId) keys.add(manager.agentId);
  if (manager.workstreamId) keys.add(manager.workstreamId);
  if (managerAgent) {
    keys.add(managerAgent.id);
    if (managerAgent.workstreamId) keys.add(managerAgent.workstreamId);
  }
  return keys;
};

const belongsToManager = (
  agent: AgentInstance,
  manager: ManagerNode,
  managerAgent?: AgentInstance,
) => {
  const keys = managerKeys(manager, managerAgent);
  if (agent.managerId && keys.has(agent.managerId)) return true;
  if (agent.workstreamId && keys.has(agent.workstreamId)) return true;
  return manager.items.some((item) => item.agentIds.includes(agent.id));
};

function FitViewOnAddedNodes({ nodeIds }: { nodeIds: string }) {
  const { fitView } = useReactFlow();
  const prevRef = useRef('');
  useEffect(() => {
    if (!nodeIds) {
      prevRef.current = '';
      return;
    }
    const prev = new Set(prevRef.current.split('|').filter(Boolean));
    const next = nodeIds.split('|').filter(Boolean);
    const added = next.some((id) => !prev.has(id));
    const firstPaint = prev.size === 0 && next.length > 0;
    prevRef.current = nodeIds;
    if (!added && !firstPaint) return;
    const timer = window.setTimeout(() => {
      void fitView({ padding: 0.24, duration: 160 });
    }, 80);
    return () => window.clearTimeout(timer);
  }, [nodeIds, fitView]);
  return null;
}

function buildGraph(
  state: WorkspaceState,
  layout: LayoutMode,
  focusManagerId: string | null,
): { nodes: Node[]; edges: Edge[]; structureKey: string } {
  const graphNodes: Node[] = [];
  const graphEdges: Edge[] = [];
  const director = state.directorId ? state.agents[state.directorId] : undefined;

  const managerColumns: Array<{ manager: ManagerNode; agent?: AgentInstance }> = [];
  const seen = new Set<string>();

  const pushColumn = (manager: ManagerNode, agent?: AgentInstance) => {
    const existing = managerColumns.find(
      (column) =>
        column.manager.id === manager.id ||
        (manager.agentId != null &&
          (column.manager.agentId === manager.agentId || column.manager.id === manager.agentId)) ||
        (manager.workstreamId != null && column.manager.workstreamId === manager.workstreamId) ||
        (agent != null && (column.manager.agentId === agent.id || column.manager.id === agent.id)),
    );
    if (existing) {
      existing.manager = {
        ...existing.manager,
        ...manager,
        agentId: manager.agentId ?? existing.manager.agentId,
        workstreamId: manager.workstreamId ?? existing.manager.workstreamId,
        items: manager.items.length ? manager.items : existing.manager.items,
        title: manager.title || existing.manager.title,
      };
      existing.agent = agent ?? existing.agent;
      if (existing.manager.agentId) seen.add(existing.manager.agentId);
      seen.add(existing.manager.id);
      return;
    }
    managerColumns.push({ manager, agent });
    seen.add(manager.id);
    if (manager.agentId) seen.add(manager.agentId);
  };

  for (const manager of state.managers) {
    if (manager.id === LEGACY_MANAGER_ID) continue;
    const agent = manager.agentId ? state.agents[manager.agentId] : state.agents[manager.id];
    pushColumn(manager, agent);
  }

  for (const agent of Object.values(state.agents)) {
    if (agent.role !== 'manager' || seen.has(agent.id)) continue;
    pushColumn(
      {
        id: agent.id,
        title: agent.title,
        status: agent.status,
        dependencies: [],
        agentId: agent.id,
        workstreamId: agent.workstreamId,
        items: [],
      },
      agent,
    );
  }

  const managerCount = Math.max(1, managerColumns.length);
  const colWidth = layout === 'swimlane' ? 340 : 300;
  const width = Math.max(280, managerCount * colWidth);

  const inFocus = (agent: AgentInstance) => {
    if (!focusManagerId) return true;
    if (agent.id === focusManagerId || agent.role === 'director') return true;
    const focused = managerColumns.find(
      ({ manager, agent: mgr }) =>
        manager.id === focusManagerId ||
        mgr?.id === focusManagerId ||
        manager.agentId === focusManagerId,
    );
    if (!focused) return agent.managerId === focusManagerId;
    return belongsToManager(agent, focused.manager, focused.agent);
  };

  const toNode = (
    agent: AgentInstance,
    position: { x: number; y: number },
    compact = false,
  ): Node => ({
    id: agent.id,
    type: 'agent',
    position,
    data: {
      role: agent.role,
      title: agent.title,
      status: agent.status,
      dimmed: !inFocus(agent),
      heat: heatOpacity(agent),
      contract: agent.workContract
        ? `${agent.workContract.id} · v${agent.workContract.version}`
        : undefined,
      requestContext:
        agent.logicalRequestId || agent.replayCount > 0
          ? `${agent.logicalRequestId?.slice(-10) ?? 'request'}${
              agent.replayCount
                ? ` · ${agent.replayCount} replay${agent.replayCount === 1 ? '' : 's'}`
                : ''
            }${agent.account ? ` · ${agent.account}` : ''}`
          : undefined,
    } satisfies AgentNodeData,
    sourcePosition: Position.Bottom,
    targetPosition: Position.Top,
    className: `flow-agent-node${compact ? ' compact' : ''}${
      focusManagerId === agent.id ? ' focused' : ''
    }`,
  });

  const showDirectorSlot =
    !director && (managerColumns.length > 0 || state.fanout.plannedManagers > 0);
  const directorNodeId = director?.id ?? (showDirectorSlot ? 'director:planned' : undefined);
  if (director) {
    graphNodes.push(
      toNode(director, {
        x: width / 2 - 90,
        y: layout === 'radial' ? 140 : 10,
      }),
    );
  } else if (directorNodeId) {
    graphNodes.push({
      id: directorNodeId,
      type: 'agent',
      position: {
        x: width / 2 - 90,
        y: layout === 'radial' ? 140 : 10,
      },
      data: {
        role: 'director',
        title: 'Director plan',
        status: 'queued',
        dimmed: false,
        heat: 0.7,
      } satisfies AgentNodeData,
      sourcePosition: Position.Bottom,
      targetPosition: Position.Top,
      className: 'flow-agent-node',
    });
  }

  const placedChildIds = new Set<string>();

  managerColumns.forEach(({ manager, agent: managerAgent }, managerIndex) => {
    let managerX = 40 + managerIndex * colWidth;
    let managerY = 125;
    if (layout === 'radial') {
      const angle = ((managerIndex + 0.5) / managerCount) * Math.PI - Math.PI / 2;
      managerX = width / 2 + Math.cos(angle) * 210 - 70;
      managerY = 150 + Math.sin(angle) * 110;
    }

    const liveManager =
      managerAgent ??
      (manager.agentId ? state.agents[manager.agentId] : undefined) ??
      state.agents[manager.id];

    let parentId = liveManager?.id;
    if (liveManager) {
      graphNodes.push(toNode(liveManager, { x: managerX, y: managerY }));
    } else {
      parentId = `mgr-slot:${manager.id}`;
      graphNodes.push({
        id: parentId,
        type: 'agent',
        position: { x: managerX, y: managerY },
        data: {
          role: 'manager',
          title: manager.title,
          status: manager.status,
          dimmed: Boolean(focusManagerId && focusManagerId !== manager.id),
          heat: 0.7,
        } satisfies AgentNodeData,
        sourcePosition: Position.Bottom,
        targetPosition: Position.Top,
        className: 'flow-agent-node',
      });
    }

    if (directorNodeId && parentId) {
      graphEdges.push({
        id: `base:${directorNodeId}:${parentId}`,
        source: directorNodeId,
        sourceHandle: 'out',
        target: parentId,
        targetHandle: 'in',
        type: 'link',
        markerEnd: { type: MarkerType.ArrowClosed, color: '#60758c', width: 11, height: 11 },
        data: { kind: 'hierarchy' } satisfies LinkEdgeData,
        zIndex: 2,
      });
    }

    const children = Object.values(state.agents)
      .filter(
        (agent) =>
          CHILD_ROLES.has(agent.role) &&
          !placedChildIds.has(agent.id) &&
          belongsToManager(agent, manager, liveManager),
      )
      .sort((a, b) => {
        const rank = (role: string) => (role === 'worker' ? 0 : role === 'tester' ? 1 : 2);
        return rank(a.role) - rank(b.role) || a.title.localeCompare(b.title);
      });

    children.forEach((agent, index) => {
      placedChildIds.add(agent.id);
      let x = managerX - 40 + (index % 3) * 105;
      let y = managerY + 120 + Math.floor(index / 3) * 95;
      if (layout === 'swimlane') {
        x = managerX - 20 + (index % 2) * 150;
        y = managerY + 110 + Math.floor(index / 2) * 88;
      }
      if (layout === 'radial') {
        x = managerX - 30 + (index % 2) * 90;
        y = managerY + 90 + Math.floor(index / 2) * 80;
      }
      graphNodes.push(toNode(agent, { x, y }, true));
      if (parentId) {
        graphEdges.push({
          id: `base:${parentId}:${agent.id}`,
          source: parentId,
          sourceHandle: 'out',
          target: agent.id,
          targetHandle: 'in',
          type: 'link',
          markerEnd: { type: MarkerType.ArrowClosed, color: '#60758c', width: 11, height: 11 },
          data: { kind: 'hierarchy' } satisfies LinkEdgeData,
          zIndex: 2,
        });
      }
    });
  });

  Object.values(state.agents)
    .filter((agent) => CHILD_ROLES.has(agent.role) && !placedChildIds.has(agent.id))
    .forEach((agent, index) => {
      const x = 40 + (index % 4) * 120;
      const y = (managerColumns.length ? 260 : 130) + Math.floor(index / 4) * 90;
      graphNodes.push(toNode(agent, { x, y }, true));
      if (directorNodeId) {
        graphEdges.push({
          id: `base:${directorNodeId}:${agent.id}`,
          source: directorNodeId,
          sourceHandle: 'out',
          target: agent.id,
          targetHandle: 'in',
          type: 'link',
          markerEnd: { type: MarkerType.ArrowClosed, color: '#60758c', width: 11, height: 11 },
          data: { kind: 'hierarchy' } satisfies LinkEdgeData,
          zIndex: 2,
        });
      }
    });

  // Persistent communication wires: one edge per source→target pair.
  // Hierarchy wires stay forever; when a signal fires on that pair, a packet runs on the same wire.
  const nodeIds = new Set(graphNodes.map((node) => node.id));
  const liveSignals = state.signals.slice(-12);
  const livePairKeys = new Set(
    liveSignals.map((signal) => `${signal.sourceAgentId}->${signal.targetAgentId}`),
  );
  const pairLatest = new Map<string, (typeof state.signals)[number]>();
  for (const signal of state.signals) {
    if (!nodeIds.has(signal.sourceAgentId) || !nodeIds.has(signal.targetAgentId)) continue;
    if (signal.sourceAgentId === signal.targetAgentId) continue;
    pairLatest.set(`${signal.sourceAgentId}->${signal.targetAgentId}`, signal);
  }

  const edgeByPair = new Map<string, Edge>();
  for (const edge of graphEdges) {
    edgeByPair.set(`${edge.source}->${edge.target}`, edge);
  }

  for (const [pairKey, signal] of pairLatest) {
    const color = signalColor(signal.signalType);
    const pulse = livePairKeys.has(pairKey);
    const existing = edgeByPair.get(pairKey);
    if (existing) {
      // Reuse hierarchy (or prior) wire — keep it visible, only add packet when live
      existing.data = {
        ...((existing.data as LinkEdgeData | undefined) ?? { kind: 'hierarchy' }),
        kind: (existing.data as LinkEdgeData | undefined)?.kind ?? 'hierarchy',
        pulse,
        color: pulse ? color : (existing.data as LinkEdgeData | undefined)?.color,
        signalType: signal.signalType,
      } satisfies LinkEdgeData;
      if (pulse) {
        existing.markerEnd = { type: MarkerType.ArrowClosed, color, width: 12, height: 12 };
        existing.zIndex = 8;
      }
      continue;
    }
    // Non-tree route (e.g. manager → director): keep a permanent signal wire
    graphEdges.push({
      id: `link:${pairKey}`,
      source: signal.sourceAgentId,
      sourceHandle: 'out',
      target: signal.targetAgentId,
      targetHandle: 'in',
      type: 'link',
      markerEnd: { type: MarkerType.ArrowClosed, color, width: 12, height: 12 },
      data: {
        kind: 'signal',
        pulse,
        color,
        signalType: signal.signalType,
      } satisfies LinkEdgeData,
      zIndex: pulse ? 8 : 3,
    });
  }

  return {
    nodes: graphNodes,
    edges: graphEdges,
    structureKey: graphNodes
      .map((node) => node.id)
      .sort()
      .join('|'),
  };
}

export function DagOverview({
  state,
  onOpenAgent,
  expanded = false,
  onToggleExpand,
}: DagOverviewProps) {
  const [layout, setLayout] = useState<LayoutMode>('tree');
  const [focusManagerId, setFocusManagerId] = useState<string | null>(null);

  const layouted = useMemo(
    () => buildGraph(state, layout, focusManagerId),
    // Stream-only state changes retain the graph revision, so token and
    // thinking frames do not reconstruct every React Flow node and edge.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [state.graphRevision, layout, focusManagerId],
  );

  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const layoutRef = useRef(layout);

  useEffect(() => {
    const layoutChanged = layoutRef.current !== layout;
    layoutRef.current = layout;
    setNodes((current) => {
      const pos = new Map(current.map((node) => [node.id, node.position]));
      return layouted.nodes.map((node) => ({
        ...node,
        position: layoutChanged ? node.position : (pos.get(node.id) ?? node.position),
      }));
    });
    setEdges(layouted.edges);
  }, [layout, layouted, setEdges, setNodes]);

  const leafCount = Object.values(state.agents).filter((agent) =>
    CHILD_ROLES.has(agent.role),
  ).length;

  return (
    <section className="dag-overview" aria-label="Agent hierarchy">
      <div className="dag-heading">
        <div>
          <span className="eyebrow">Execution graph</span>
          <h2>Director orchestration</h2>
        </div>
        <div className="dag-heading-actions">
          <div className="layout-toggle" role="tablist" aria-label="Graph layout">
            <button
              type="button"
              role="tab"
              className={layout === 'tree' ? 'active' : ''}
              aria-selected={layout === 'tree'}
              tabIndex={layout === 'tree' ? 0 : -1}
              onClick={() => setLayout('tree')}
              onKeyDown={(event) => moveLayoutFocus(event, 0, setLayout)}
              title="Tree"
            >
              <GitBranch size={12} />
            </button>
            <button
              type="button"
              role="tab"
              className={layout === 'swimlane' ? 'active' : ''}
              aria-selected={layout === 'swimlane'}
              tabIndex={layout === 'swimlane' ? 0 : -1}
              onClick={() => setLayout('swimlane')}
              onKeyDown={(event) => moveLayoutFocus(event, 1, setLayout)}
              title="Swimlane"
            >
              <LayoutGrid size={12} />
            </button>
            <button
              type="button"
              role="tab"
              className={layout === 'radial' ? 'active' : ''}
              aria-selected={layout === 'radial'}
              tabIndex={layout === 'radial' ? 0 : -1}
              onClick={() => setLayout('radial')}
              onKeyDown={(event) => moveLayoutFocus(event, 2, setLayout)}
              title="Radial"
            >
              <Waypoints size={12} />
            </button>
          </div>
          <button
            type="button"
            className={`ghost-chip ${focusManagerId ? 'active' : ''}`}
            onClick={() => setFocusManagerId(null)}
            disabled={!focusManagerId}
            title="Clear manager focus"
            aria-pressed={focusManagerId != null}
          >
            <Crosshair size={12} /> Clear focus
          </button>
          {onToggleExpand && (
            <button
              type="button"
              className={`ghost-chip ${expanded ? 'active' : ''}`}
              onClick={onToggleExpand}
              title={expanded ? 'Exit full graph' : 'Expand graph'}
              aria-pressed={expanded}
              aria-expanded={expanded}
            >
              {expanded ? <Minimize2 size={12} /> : <Maximize2 size={12} />}
              {expanded ? 'Exit full' : 'Full graph'}
            </button>
          )}
          <span className="graph-count">
            <GitBranch size={13} />
            {state.managers.length} workstreams · {state.fanout.plannedChildren || leafCount}{' '}
            planned
            {' · '}
            {state.fanout.calledAgentIds.length} called
            {' · '}
            {state.fanout.requestAttempts} requests
            {' · '}
            {state.fanout.replayedRequests} replays
          </span>
        </div>
      </div>

      <div className="dag-stage signal-graph">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          minZoom={0.15}
          maxZoom={1.7}
          nodesDraggable
          nodesConnectable={false}
          elementsSelectable
          proOptions={{ hideAttribution: true }}
          defaultEdgeOptions={{ type: 'link', interactionWidth: 24 }}
          onNodeClick={(_, node) => {
            if (String(node.id).startsWith('mgr-slot:')) return;
            const agent = state.agents[node.id];
            if (!agent) return;
            if (agent.role === 'manager') {
              setFocusManagerId((current) => (current === agent.id ? null : agent.id));
            }
            onOpenAgent(agent);
          }}
        >
          <FitViewOnAddedNodes nodeIds={layouted.structureKey} />
          <Background gap={22} size={1} color="#172433" />
          <MiniMap
            pannable
            zoomable
            style={{ width: 120, height: 80 }}
            maskColor="rgba(8,12,18,0.72)"
            bgColor="#0c121c"
            nodeStrokeWidth={1}
            nodeColor={(node) => {
              const agent = state.agents[node.id];
              const role = agent?.role ?? (node.data as AgentNodeData | undefined)?.role;
              if (role === 'director') return '#b98438';
              if (role === 'manager') return '#4f8bd6';
              if (role === 'tester' || role === 'reviewer') return '#45a675';
              if (role === 'worker') return '#7b8ea5';
              return '#4e6074';
            }}
          />
          <Controls showInteractive={false} />
        </ReactFlow>
        {!nodes.length && (
          <div className="dag-empty">
            <Bot size={18} />
            <span>Agent nodes appear when Director starts planning.</span>
            <small>Launch a hierarchy run to populate the map.</small>
          </div>
        )}

        <div className="graph-legend" aria-hidden="true">
          <span>
            <i className="lg-director" /> Director
          </span>
          <span>
            <i className="lg-manager" /> Manager
          </span>
          <span>
            <i className="lg-worker" /> Worker
          </span>
          <span>
            <i className="lg-tester" /> Tester
          </span>
          <span>
            <i className="lg-signal" /> Signal
          </span>
        </div>
      </div>
    </section>
  );
}
