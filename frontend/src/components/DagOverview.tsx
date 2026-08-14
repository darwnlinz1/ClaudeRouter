import { memo, useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent } from 'react';
import {
  Background,
  BaseEdge,
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  getBezierPath,
  useEdgesState,
  useNodesState,
  useReactFlow,
  useStore,
  type Edge,
  type EdgeProps,
  type Node,
  type NodeProps,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import {
  BrainCircuit,
  Bot,
  Code2,
  Compass,
  Crosshair,
  FlaskConical,
  GitBranch,
  Layers,
  LayoutGrid,
  Maximize2,
  Minimize2,
  PanelRightOpen,
  ShieldCheck,
  Waypoints,
  X,
} from 'lucide-react';
import type { AgentInstance, ManagerNode, WorkspaceState } from '../types';

interface DagOverviewProps {
  state: WorkspaceState;
  onOpenAgent: (agent: AgentInstance) => void;
  expanded?: boolean;
  onToggleExpand?: () => void;
}

type LayoutMode = 'neural' | 'tree' | 'swimlane' | 'radial';

const LAYOUTS: Array<{ id: LayoutMode; label: string; icon: typeof GitBranch }> = [
  { id: 'neural', label: 'Neural', icon: BrainCircuit },
  { id: 'tree', label: 'Tree', icon: GitBranch },
  { id: 'swimlane', label: 'Swimlane', icon: LayoutGrid },
  { id: 'radial', label: 'Radial', icon: Waypoints },
];

/* Node geometry. Kept in sync with .flow-agent-node widths in styles.css so the
   layout never has to measure the DOM before it can place a node. */
const NODE_W = 216;
const NODE_H = 60;
const CHILD_W = 186;
const CHILD_H = 50;
const GAP_X = 16;
const GAP_Y = 28;
const COLUMN_GAP = 40;
const ROW_GAP = 72;
const CHILD_OFFSET_Y = 100;
const DIRECTOR_DROP = 150;
/* Single-column children hang to the right of their manager on a shared spine,
   the way a file tree does, so no connector has to cross a card. */
const SPINE_GAP = 26;
const STACK_GAP = 14;
const STACK_TOP = 22;
const NEURAL_LAYER_GAP = 316;
const NEURAL_NODE_GAP = 16;
const NEURAL_GROUP_GAP = 48;

/* 'agent' is what roleValue() falls back to for any role string the backend
   sends that the client does not recognise, so those have to hang off a manager
   like a worker does rather than vanish from the map. */
const CHILD_ROLES = new Set(['worker', 'tester', 'reviewer', 'agent', 'supervisor']);
const LEGACY_MANAGER_ID = 'manager:legacy';

/* Same hues as the --role-* tokens so a wire reads as an extension of the rail
   on the card it leaves; opacity, not a second palette, does the muting. */
const ROLE_COLOR: Record<string, string> = {
  director: '#e0b055',
  manager: '#6ba5ff',
  worker: '#63c48d',
  tester: '#b799f2',
  reviewer: '#b799f2',
};

const roleColor = (role: string) => ROLE_COLOR[role] ?? '#5d6b7d';

const ROLE_ICON: Record<string, typeof Bot> = {
  director: Compass,
  manager: Layers,
  worker: Code2,
  tester: FlaskConical,
  reviewer: ShieldCheck,
};

/* Status drives one pip per node rather than recolouring the whole card, which
   keeps role identity readable while a run churns through states. */
const statusTone = (status: string) => {
  if (status === 'running' || status === 'streaming') return 'running';
  if (status === 'waiting' || status === 'queued') return 'queued';
  if (status === 'passed' || status === 'completed' || status === 'done') return 'success';
  if (status === 'partial') return 'partial';
  if (status === 'abandoned') return 'abandoned';
  if (status === 'skipped') return 'skipped';
  if (status === 'failed' || status === 'error') return 'danger';
  if (status === 'blocked') return 'warn';
  return 'idle';
};

const signalColor = (type: string) => {
  if (type.includes('error') || type.includes('fail') || type.includes('reject')) return '#ef8080';
  if (type.includes('patch') || type.includes('approve')) return '#63c48d';
  if (type.includes('review') || type.includes('ask') || type.includes('result')) return '#e0b055';
  if (type.includes('assign') || type.includes('delegate')) return '#6ba5ff';
  return '#8b98a8';
};

/* Child grids get wider when there are few workstreams and narrower when there
   are many, so the whole tree keeps an aspect ratio close to the pane instead of
   turning into one very wide, very short band. */
const childColumnCount = (count: number, managerCount: number) => {
  if (count <= 1) return 1;
  const widest = managerCount >= 6 ? 1 : managerCount >= 3 ? 2 : managerCount === 2 ? 3 : 4;
  return Math.min(widest, count);
};

const heatOpacity = (agent: AgentInstance) => {
  if (agent.status === 'running' || agent.status === 'waiting') return 1;
  if (agent.status === 'failed') return 0.92;
  if (agent.status === 'passed') return 0.82;
  if (agent.status === 'partial') return 0.8;
  if (agent.status === 'abandoned') return 0.7;
  if (agent.status === 'skipped') return 0.58;
  return 0.66;
};

type AgentNodeData = {
  role: string;
  title: string;
  /** The descriptive title, when an ordinal label has taken the headline. */
  description?: string;
  status: string;
  dimmed: boolean;
  heat: number;
  meta?: string;
  contract?: string;
  requestContext?: string;
};

/* Every node exposes a handle on all four sides. Edges pick the pair that faces
   the other node, which is what keeps a child's reply from looping back around
   the card instead of leaving from its top edge. */
const HANDLE_SIDES = [
  { id: 'top', position: Position.Top },
  { id: 'bottom', position: Position.Bottom },
  { id: 'left', position: Position.Left },
  { id: 'right', position: Position.Right },
] as const;

type Point = { x: number; y: number };

function facingHandles(from: Point, to: Point) {
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  if (Math.abs(dy) >= Math.abs(dx)) {
    return dy >= 0
      ? { sourceHandle: 'out-bottom', targetHandle: 'in-top' }
      : { sourceHandle: 'out-top', targetHandle: 'in-bottom' };
  }
  return dx >= 0
    ? { sourceHandle: 'out-right', targetHandle: 'in-left' }
    : { sourceHandle: 'out-left', targetHandle: 'in-right' };
}

const AgentFlowNode = memo(function AgentFlowNode({ data }: NodeProps) {
  const node = data as unknown as AgentNodeData;
  const RoleIcon = ROLE_ICON[node.role] ?? Bot;
  return (
    <>
      {HANDLE_SIDES.map((side) => (
        <Handle
          key={`in-${side.id}`}
          type="target"
          position={side.position}
          id={`in-${side.id}`}
          className="flow-handle"
          isConnectable={false}
        />
      ))}
      <div
        className={`flow-agent-label role-${node.role} tone-${statusTone(node.status)} ${
          node.dimmed ? 'dimmed' : ''
        }`}
        style={{ opacity: node.dimmed ? 0.32 : node.heat }}
        title={[
          node.title,
          node.description,
          node.meta,
          node.status,
          node.contract,
          node.requestContext,
        ]
          .filter(Boolean)
          .join(' · ')}
        aria-label={`${node.title}, ${node.role}, ${node.status}`}
      >
        <span className="agent-neuron-orbit" aria-hidden="true">
          <span className="agent-node-icon">
            <RoleIcon size={14} strokeWidth={2} />
          </span>
        </span>
        <div className="agent-node-text">
          <strong>{node.title}</strong>
          <small>{node.meta ?? node.role}</small>
        </div>
        <i className="agent-node-pip" aria-hidden="true" />
      </div>
      {HANDLE_SIDES.map((side) => (
        <Handle
          key={`out-${side.id}`}
          type="source"
          position={side.position}
          id={`out-${side.id}`}
          className="flow-handle"
          isConnectable={false}
        />
      ))}
    </>
  );
});

const nodeTypes = { agent: AgentFlowNode };

type LinkEdgeData = {
  kind: 'hierarchy' | 'signal';
  neural?: boolean;
  pulse?: boolean;
  color?: string;
  signalType?: string;
  /** Intermediate waypoints; set when an edge is routed around the tree. */
  via?: Point[];
};

/* Polyline with arc corners. Used for routed edges so a wire that has to travel
   past other rows does it on straight runs through empty corridors instead of
   one long diagonal across whatever cards lie between. */
function roundedPath(points: Point[], radius = 16) {
  if (points.length < 2) return '';
  let d = `M ${points[0].x},${points[0].y}`;
  for (let index = 1; index < points.length - 1; index += 1) {
    const previous = points[index - 1];
    const corner = points[index];
    const next = points[index + 1];
    const inLength = Math.hypot(corner.x - previous.x, corner.y - previous.y) || 1;
    const outLength = Math.hypot(next.x - corner.x, next.y - corner.y) || 1;
    const r = Math.min(radius, inLength / 2, outLength / 2);
    const entry = {
      x: corner.x + ((previous.x - corner.x) / inLength) * r,
      y: corner.y + ((previous.y - corner.y) / inLength) * r,
    };
    const exit = {
      x: corner.x + ((next.x - corner.x) / outLength) * r,
      y: corner.y + ((next.y - corner.y) / outLength) * r,
    };
    d += ` L ${entry.x},${entry.y} Q ${corner.x},${corner.y} ${exit.x},${exit.y}`;
  }
  const last = points[points.length - 1];
  return `${d} L ${last.x},${last.y}`;
}

function LinkEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
}: EdgeProps) {
  const link = (data ?? {}) as LinkEdgeData;
  const hierarchy = link.kind !== 'signal';
  const pulse = Boolean(link.pulse);
  const color = link.color ?? (hierarchy ? '#4a5a6d' : '#5f7186');
  const [bezier] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    curvature: hierarchy ? 0.34 : 0.6,
  });
  const path = link.via?.length
    ? roundedPath([{ x: sourceX, y: sourceY }, ...link.via, { x: targetX, y: targetY }])
    : bezier;
  const strokeWidth = pulse ? 1.9 : hierarchy ? 1.4 : 1.2;

  return (
    <g
      className={`flow-link-edge kind-${link.kind ?? 'hierarchy'}${link.neural ? ' is-neural' : ''}${
        pulse ? ' is-live' : ''
      }`}
    >
      <BaseEdge
        id={`${id}:casing`}
        path={path}
        style={{ stroke: '#0a0d12', strokeWidth: strokeWidth + 5, opacity: 0.95 }}
        className="flow-link-edge-casing"
      />
      <BaseEdge
        id={id}
        path={path}
        style={{
          stroke: color,
          strokeWidth,
          strokeLinecap: 'round',
          strokeDasharray: hierarchy ? undefined : '4 7',
          opacity: pulse ? 0.92 : hierarchy ? 0.58 : 0.44,
        }}
        className="flow-link-edge-path"
      />
      {hierarchy && link.neural && (
        <>
          <circle cx={sourceX} cy={sourceY} r={2.5} fill={color} className="synapse-terminal" />
          <circle cx={targetX} cy={targetY} r={2.5} fill={color} className="synapse-terminal" />
        </>
      )}
      {pulse && (
        <>
          {/* A wide, faint halo trailing a small core reads as a packet in
              motion without resorting to a glow filter. */}
          <circle r={5} fill={color} className="signal-packet-halo">
            <animateMotion dur="1.9s" repeatCount="indefinite" path={path} />
          </circle>
          <circle r={2.4} fill="#f2f6fb" className="signal-packet">
            <animateMotion dur="1.9s" repeatCount="indefinite" path={path} />
          </circle>
        </>
      )}
    </g>
  );
}

const edgeTypes = {
  link: LinkEdge,
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
  delegatedParent?: Map<string, string>,
) => {
  const keys = managerKeys(manager, managerAgent);
  if (agent.managerId && keys.has(agent.managerId)) return true;
  if (agent.workstreamId && keys.has(agent.workstreamId)) return true;
  if (manager.items?.some((item) => item.agentIds?.includes(agent.id))) return true;
  // Last resort: the call itself. Some runs record the delegation only as a
  // signal, and without this the worker would hang off the director instead of
  // the manager that actually started it.
  const parent = delegatedParent?.get(agent.id);
  return Boolean(parent && keys.has(parent));
};

/* Earliest manager -> child signal wins, because that is the delegation; later
   traffic on the same pair is just the conversation. */
const delegationParents = (state: WorkspaceState) => {
  const parents = new Map<string, string>();
  for (const signal of state.signals) {
    if (parents.has(signal.targetAgentId)) continue;
    const source = state.agents[signal.sourceAgentId];
    const target = state.agents[signal.targetAgentId];
    if (source?.role !== 'manager' || !target || !CHILD_ROLES.has(target.role)) continue;
    parents.set(target.id, source.id);
  }
  return parents;
};

/** Keeps every node inside the viewport: refits on structure, layout and resize. */
function GraphAutoFit({ signature }: { signature: string }) {
  const { fitView } = useReactFlow();
  const width = useStore((store) => store.width);
  const height = useStore((store) => store.height);

  useEffect(() => {
    if (!width || !height) return undefined;
    const timer = window.setTimeout(() => {
      void fitView({ padding: 0.12, duration: 180, minZoom: 0.05, maxZoom: 1.4 });
    }, 60);
    return () => window.clearTimeout(timer);
  }, [fitView, height, signature, width]);

  return null;
}

/** Exported for tests: the placement rules are easier to assert here than
    through a React Flow canvas, which needs real element measurements. */
// eslint-disable-next-line react-refresh/only-export-components
export function buildGraph(
  state: WorkspaceState,
  layout: LayoutMode,
  focusManagerId: string | null,
  paneAspect: number,
): { nodes: Node[]; edges: Edge[]; structureKey: string } {
  const graphNodes: Node[] = [];
  const graphEdges: Edge[] = [];
  // directorId is only ever set by a director event, so a state hydrated from a
  // task snapshot has none; fall back to the role so the root still shows.
  const director =
    (state.directorId ? state.agents[state.directorId] : undefined) ??
    Object.values(state.agents).find((agent) => agent.role === 'director');
  const centres = new Map<string, Point>();

  const centreOf = (id: string) => centres.get(id) ?? { x: 0, y: 0 };
  const pushNode = (node: Node, compact = false) => {
    graphNodes.push(node);
    centres.set(node.id, {
      x: node.position.x + (compact ? CHILD_W : NODE_W) / 2,
      y: node.position.y + (compact ? CHILD_H : NODE_H) / 2,
    });
  };
  const linkEdge = (
    source: string,
    target: string,
    data: LinkEdgeData,
    zIndex: number,
    handles?: { sourceHandle: string; targetHandle: string },
  ): Edge => ({
    id: `${data.kind === 'signal' ? 'link' : 'base'}:${source}:${target}`,
    source,
    target,
    ...(handles ?? facingHandles(centreOf(source), centreOf(target))),
    type: 'link',
    data,
    zIndex,
  });

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
        items: manager.items?.length ? manager.items : existing.manager.items,
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
  while (managerColumns.length < state.fanout.plannedManagers) {
    const index = managerColumns.length + 1;
    pushColumn({
      id: `planned-manager-${index}`,
      title: `Manager ${index}`,
      status: 'queued',
      dependencies: [],
      items: [],
    });
  }

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
    return belongsToManager(agent, focused.manager, focused.agent, delegatedParent);
  };

  const toNode = (
    agent: AgentInstance,
    position: { x: number; y: number },
    compact = false,
    meta?: string,
  ): Node => ({
    id: agent.id,
    type: 'agent',
    position,
    data: {
      role: agent.role,
      title: agent.label ?? agent.title,
      description: agent.label ? agent.title : undefined,
      status: agent.status,
      dimmed: !inFocus(agent),
      heat: heatOpacity(agent),
      // The account is the single most useful thing to see per node during a
      // run, so it wins the subtitle whenever the agent is on one.
      meta: agent.account ?? meta ?? agent.status.replaceAll('_', ' '),
      contract: agent.workContract
        ? `contract ${agent.workContract.id} v${agent.workContract.version}`
        : undefined,
      requestContext:
        agent.logicalRequestId || agent.replayCount > 0
          ? `${agent.replayCount ? `${agent.replayCount} replay${agent.replayCount === 1 ? '' : 's'}` : 'live request'}`
          : undefined,
    } satisfies AgentNodeData,
    sourcePosition: Position.Bottom,
    targetPosition: Position.Top,
    className: `flow-agent-node${layout === 'neural' ? ' neural' : ''}${compact ? ' compact' : ''}${
      focusManagerId === agent.id ? ' focused' : ''
    }`,
  });

  // Resolve the live agent (or a placeholder) behind every manager column first so
  // the geometry pass can measure the real child counts.
  const columns = managerColumns.map(({ manager, agent }) => {
    const liveManager =
      agent ??
      (manager.agentId ? state.agents[manager.agentId] : undefined) ??
      state.agents[manager.id];
    return { manager, liveManager };
  });

  const delegatedParent = delegationParents(state);
  const placedChildIds = new Set<string>();
  const columnChildren = columns.map(({ manager, liveManager }) =>
    Object.values(state.agents)
      .filter(
        (agent) =>
          CHILD_ROLES.has(agent.role) &&
          !placedChildIds.has(agent.id) &&
          belongsToManager(agent, manager, liveManager, delegatedParent),
      )
      .sort((a, b) => {
        const rank = (role: string) => (role === 'worker' ? 0 : role === 'tester' ? 1 : 2);
        return rank(a.role) - rank(b.role) || a.title.localeCompare(b.title);
      })
      .map((agent) => {
        placedChildIds.add(agent.id);
        return agent;
      }),
  );

  // columnCount indexes the geometry arrays; managerCount only ever feeds ratio
  // maths, so it is clamped away from zero and must never be used as a bound.
  const columnCount = columns.length;
  const managerCount = Math.max(1, columnCount);

  /* Neural mode is a feed-forward network rather than another tree skin.
     Director is the input neuron, Managers form the routing layer, Workers the
     execution layer, and Testers the validation layer. Each manager owns one
     vertical activation band so dense fan-outs remain untangled. */
  const neuralManagerAnchors: Point[] = [];
  const neuralChildAnchors = new Map<string, Point>();
  const neuralGroupHeights: number[] = [];
  for (const children of columnChildren) {
    const executionCount = children.filter(
      (agent) => agent.role !== 'tester' && agent.role !== 'reviewer',
    ).length;
    const validationCount = children.length - executionCount;
    const rows = Math.max(1, executionCount, validationCount);
    neuralGroupHeights.push(
      Math.max(NODE_H, rows * CHILD_H + Math.max(0, rows - 1) * NEURAL_NODE_GAP),
    );
  }
  const neuralTotalHeight =
    neuralGroupHeights.reduce((total, height) => total + height, 0) +
    Math.max(0, columnCount - 1) * NEURAL_GROUP_GAP;
  let neuralCursor = -neuralTotalHeight / 2;
  columns.forEach((_, managerIndex) => {
    const children = columnChildren[managerIndex] ?? [];
    const groupHeight = neuralGroupHeights[managerIndex] ?? NODE_H;
    neuralManagerAnchors[managerIndex] = {
      x: 0,
      y: neuralCursor + (groupHeight - NODE_H) / 2,
    };
    const execution = children.filter(
      (agent) => agent.role !== 'tester' && agent.role !== 'reviewer',
    );
    const validation = children.filter(
      (agent) => agent.role === 'tester' || agent.role === 'reviewer',
    );
    const placeLayer = (agents: AgentInstance[], x: number) => {
      const layerHeight =
        agents.length * CHILD_H + Math.max(0, agents.length - 1) * NEURAL_NODE_GAP;
      const top = neuralCursor + (groupHeight - layerHeight) / 2;
      agents.forEach((agent, index) => {
        neuralChildAnchors.set(agent.id, {
          x,
          y: top + index * (CHILD_H + NEURAL_NODE_GAP),
        });
      });
    };
    placeLayer(execution, NEURAL_LAYER_GAP);
    placeLayer(validation, NEURAL_LAYER_GAP * 2);
    neuralCursor += groupHeight + NEURAL_GROUP_GAP;
  });

  // Column geometry for the tree layout.
  const columnWidths = columnChildren.map((children) => {
    const cols = childColumnCount(children.length, managerCount);
    if (cols === 1) return NODE_W / 2 + SPINE_GAP + CHILD_W;
    return Math.max(NODE_W, cols * CHILD_W + (cols - 1) * GAP_X);
  });
  const columnHeights = columnChildren.map((children) => {
    if (!children.length) return NODE_H;
    const cols = childColumnCount(children.length, managerCount);
    const rows = Math.ceil(children.length / cols);
    if (cols === 1) return NODE_H + STACK_TOP + rows * CHILD_H + (rows - 1) * STACK_GAP;
    return CHILD_OFFSET_Y + rows * CHILD_H + (rows - 1) * GAP_Y;
  });

  // One row of managers has the cleanest edges, but a very wide, very short tree
  // has to be zoomed far out to fit, which is what shrinks the labels. So pick
  // the row count that fills the pane the graph is actually being drawn into;
  // ties fall to fewer rows because those cross fewer edges.
  const measureRows = (perRowCandidate: number) => {
    let widest = 0;
    let total = DIRECTOR_DROP;
    for (let start = 0; start < columnCount; start += perRowCandidate) {
      const end = Math.min(start + perRowCandidate, columnCount);
      let rowWidth = COLUMN_GAP * (end - start - 1);
      let rowHeight = NODE_H;
      for (let index = start; index < end; index += 1) {
        rowWidth += columnWidths[index];
        rowHeight = Math.max(rowHeight, columnHeights[index]);
      }
      widest = Math.max(widest, rowWidth);
      total += rowHeight + ROW_GAP;
    }
    return { width: Math.max(widest, NODE_W), height: Math.max(total - ROW_GAP, NODE_H) };
  };

  let perRow = managerCount;
  let bestScore = -Infinity;
  for (let rows = 1; rows <= Math.max(1, columnCount); rows += 1) {
    const candidate = Math.ceil(columnCount / rows);
    if (rows > 1 && candidate === Math.ceil(columnCount / (rows - 1))) continue;
    const { width, height } = measureRows(candidate);
    const score = Math.min(paneAspect / width, 1 / height);
    if (score > bestScore * 1.001) {
      bestScore = score;
      perRow = candidate;
    }
  }

  const treeAnchors: Array<{ x: number; y: number }> = [];
  const treeRow: number[] = [];
  const rowTops: number[] = [];
  let contentLeft = 0;
  let rowTop = DIRECTOR_DROP;
  let rowIndex = 0;
  for (let start = 0; start < columnCount; start += perRow) {
    const indices: number[] = [];
    for (let index = start; index < Math.min(start + perRow, columnCount); index += 1) {
      indices.push(index);
    }
    const rowWidth =
      indices.reduce((total, index) => total + columnWidths[index], 0) +
      COLUMN_GAP * (indices.length - 1);
    let left = -rowWidth / 2;
    let rowHeight = NODE_H;
    contentLeft = Math.min(contentLeft, left);
    rowTops[rowIndex] = rowTop;
    for (const index of indices) {
      const indented = childColumnCount(columnChildren[index]?.length ?? 0, managerCount) === 1;
      const inset = indented ? 0 : (columnWidths[index] - NODE_W) / 2;
      treeAnchors[index] = { x: left + inset, y: rowTop };
      treeRow[index] = rowIndex;
      left += columnWidths[index] + COLUMN_GAP;
      rowHeight = Math.max(rowHeight, columnHeights[index]);
    }
    rowTop += rowHeight + ROW_GAP;
    rowIndex += 1;
  }

  /* Wrapped rows put cards between the director and its lower managers. Those
     wires run down a reserved gutter and along the empty band between rows, so
     they meet at a shared trunk instead of cutting across the row above. */
  const trunkX = contentLeft - 64;
  const directorEdgeRoute = (index: number): Point[] | undefined => {
    if (layout !== 'tree') return undefined;
    const row = treeRow[index] ?? 0;
    if (row < 1) return undefined;
    const busY = (rowTops[row] ?? 0) - ROW_GAP / 2;
    const anchor = treeAnchors[index];
    if (!anchor) return undefined;
    return [
      { x: trunkX, y: NODE_H + 34 },
      { x: trunkX, y: busY },
      { x: anchor.x + NODE_W / 2, y: busY },
    ];
  };

  const radialRadius = Math.max(300, (managerCount * (NODE_W + 56)) / (2 * Math.PI));

  const managerAnchor = (index: number): { x: number; y: number } => {
    if (layout === 'neural') {
      return neuralManagerAnchors[index] ?? { x: 0, y: 0 };
    }
    if (layout === 'swimlane') {
      return { x: 0, y: index * (Math.max(NODE_H, CHILD_H) + GAP_Y + 26) };
    }
    if (layout === 'radial') {
      const angle = (index / managerCount) * Math.PI * 2 - Math.PI / 2;
      return {
        x: Math.cos(angle) * radialRadius - NODE_W / 2,
        y: Math.sin(angle) * radialRadius - NODE_H / 2,
      };
    }
    return treeAnchors[index] ?? { x: 0, y: DIRECTOR_DROP };
  };

  const childAnchor = (
    managerIndex: number,
    childIndex: number,
    childCount: number,
    origin: { x: number; y: number },
    agent: AgentInstance,
  ): { x: number; y: number } => {
    if (layout === 'neural') {
      return (
        neuralChildAnchors.get(agent.id) ?? {
          x: NEURAL_LAYER_GAP,
          y: origin.y + childIndex * (CHILD_H + NEURAL_NODE_GAP),
        }
      );
    }
    if (layout === 'swimlane') {
      return {
        x: origin.x + NODE_W + 64 + childIndex * (CHILD_W + GAP_X),
        y: origin.y + (NODE_H - CHILD_H) / 2,
      };
    }
    if (layout === 'radial') {
      const base = (managerIndex / managerCount) * Math.PI * 2 - Math.PI / 2;
      const spread = Math.min(Math.PI / managerCount, 0.42);
      const angle = base + (childIndex - (childCount - 1) / 2) * (spread / Math.max(1, childCount));
      const radius = radialRadius + 170 + (childIndex % 2) * 70;
      return {
        x: Math.cos(angle) * radius - CHILD_W / 2,
        y: Math.sin(angle) * radius - CHILD_H / 2,
      };
    }
    const cols = childColumnCount(childCount, managerCount);
    if (cols === 1) {
      return {
        x: origin.x + NODE_W / 2 + SPINE_GAP,
        y: origin.y + NODE_H + STACK_TOP + childIndex * (CHILD_H + STACK_GAP),
      };
    }
    const gridWidth = cols * CHILD_W + (cols - 1) * GAP_X;
    const left = origin.x + NODE_W / 2 - gridWidth / 2;
    return {
      x: left + (childIndex % cols) * (CHILD_W + GAP_X),
      y: origin.y + CHILD_OFFSET_Y + Math.floor(childIndex / cols) * (CHILD_H + GAP_Y),
    };
  };

  const directorPosition = () => {
    if (layout === 'neural') return { x: -NEURAL_LAYER_GAP, y: -NODE_H / 2 };
    if (layout === 'swimlane') return { x: 0, y: -(NODE_H + 60) };
    if (layout === 'radial') return { x: -NODE_W / 2, y: -NODE_H / 2 };
    return { x: -NODE_W / 2, y: 0 };
  };

  const showDirectorSlot =
    !director && (managerColumns.length > 0 || state.fanout.plannedManagers > 0);
  const directorNodeId = director?.id ?? (showDirectorSlot ? 'director:planned' : undefined);

  const plural = (count: number, word: string) => `${count} ${word}${count === 1 ? '' : 's'}`;

  if (director) {
    pushNode(
      toNode(
        director,
        directorPosition(),
        false,
        columnCount ? plural(columnCount, 'workstream') : director.status.replaceAll('_', ' '),
      ),
    );
  } else if (directorNodeId) {
    pushNode({
      id: directorNodeId,
      type: 'agent',
      position: directorPosition(),
      data: {
        role: 'director',
        title: 'Director plan',
        status: 'queued',
        dimmed: false,
        heat: 0.8,
        meta: 'awaiting plan',
      } satisfies AgentNodeData,
      sourcePosition: Position.Bottom,
      targetPosition: Position.Top,
      className: `flow-agent-node${layout === 'neural' ? ' neural' : ''}`,
    });
  }

  columns.forEach(({ manager, liveManager }, managerIndex) => {
    const origin = managerAnchor(managerIndex);
    const children = columnChildren[managerIndex] ?? [];
    const managerMeta = children.length
      ? plural(children.length, 'agent')
      : (manager.items?.length ?? 0) > 0
        ? plural(manager.items.length, 'item')
        : 'no agents yet';
    let parentId = liveManager?.id;

    if (liveManager) {
      pushNode(toNode(liveManager, origin, false, managerMeta));
    } else {
      parentId = `mgr-slot:${manager.id}`;
      pushNode({
        id: parentId,
        type: 'agent',
        position: origin,
        data: {
          role: 'manager',
          title: manager.title,
          status: manager.status,
          dimmed: Boolean(focusManagerId && focusManagerId !== manager.id),
          heat: 0.8,
          meta: managerMeta,
        } satisfies AgentNodeData,
        sourcePosition: Position.Bottom,
        targetPosition: Position.Top,
        className: `flow-agent-node${layout === 'neural' ? ' neural' : ''}`,
      });
    }

    if (directorNodeId && parentId) {
      const via = directorEdgeRoute(managerIndex);
      graphEdges.push(
        linkEdge(
          directorNodeId,
          parentId,
          {
            kind: 'hierarchy',
            color: roleColor('manager'),
            via,
            neural: layout === 'neural',
          },
          2,
          layout === 'neural'
            ? { sourceHandle: 'out-right', targetHandle: 'in-left' }
            : via
              ? { sourceHandle: 'out-bottom', targetHandle: 'in-top' }
              : undefined,
        ),
      );
    }

    const spine =
      layout === 'tree' && childColumnCount(children.length, managerCount) === 1
        ? { sourceHandle: 'out-bottom', targetHandle: 'in-left' }
        : undefined;
    children.forEach((agent, childIndex) => {
      pushNode(
        toNode(agent, childAnchor(managerIndex, childIndex, children.length, origin, agent), true),
        true,
      );
    });
    if (parentId && layout === 'neural') {
      const execution = children.filter(
        (agent) => agent.role !== 'tester' && agent.role !== 'reviewer',
      );
      const validation = children.filter(
        (agent) => agent.role === 'tester' || agent.role === 'reviewer',
      );
      for (const agent of execution) {
        graphEdges.push(
          linkEdge(
            parentId,
            agent.id,
            { kind: 'hierarchy', neural: true, color: roleColor(agent.role) },
            2,
            { sourceHandle: 'out-right', targetHandle: 'in-left' },
          ),
        );
      }
      for (const validator of validation) {
        const sources = execution.length ? execution : parentId ? [{ id: parentId }] : [];
        for (const source of sources) {
          graphEdges.push(
            linkEdge(
              source.id,
              validator.id,
              { kind: 'hierarchy', neural: true, color: roleColor(validator.role) },
              2,
              { sourceHandle: 'out-right', targetHandle: 'in-left' },
            ),
          );
        }
      }
    } else if (parentId) {
      for (const agent of children) {
        graphEdges.push(
          linkEdge(
            parentId,
            agent.id,
            { kind: 'hierarchy', color: roleColor(agent.role) },
            2,
            spine,
          ),
        );
      }
    }
  });

  // Catch-all so the map is exhaustive: anything not already drawn lands here,
  // whether it is a child that never resolved to a manager or an agent whose
  // role the client does not model.
  const orphans = Object.values(state.agents).filter((agent) => !centres.has(agent.id));
  if (orphans.length) {
    const cols = Math.min(4, Math.max(1, Math.round(Math.sqrt(orphans.length * 1.6))));
    const gridWidth = cols * CHILD_W + (cols - 1) * GAP_X;
    const baseY =
      layout === 'neural'
        ? neuralTotalHeight / 2 + 80
        : layout === 'swimlane'
          ? columns.length * 100 + 120
          : 480;
    orphans.forEach((agent, index) => {
      pushNode(
        toNode(
          agent,
          {
            x:
              layout === 'neural'
                ? NEURAL_LAYER_GAP + (index % cols) * (CHILD_W + GAP_X)
                : -gridWidth / 2 + (index % cols) * (CHILD_W + GAP_X),
            y: baseY + Math.floor(index / cols) * (CHILD_H + GAP_Y),
          },
          true,
        ),
        true,
      );
      if (directorNodeId) {
        graphEdges.push(
          linkEdge(
            directorNodeId,
            agent.id,
            {
              kind: 'hierarchy',
              neural: layout === 'neural',
              color: roleColor(agent.role),
            },
            2,
            layout === 'neural'
              ? { sourceHandle: 'out-right', targetHandle: 'in-left' }
              : undefined,
          ),
        );
      }
    });
  }

  // Communication wires: hierarchy links stay put and carry a packet while live;
  // off-tree routes (manager → director) get their own dashed wire.
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
    // A reply travels back along the wire it came in on. Lighting up that wire
    // says the same thing as a second edge would, without the return loop that
    // a child-to-manager curve has to draw around its own column.
    const existing =
      edgeByPair.get(pairKey) ?? edgeByPair.get(`${signal.targetAgentId}->${signal.sourceAgentId}`);
    if (existing) {
      const previous = (existing.data as LinkEdgeData | undefined) ?? { kind: 'hierarchy' };
      existing.data = {
        ...previous,
        kind: previous.kind ?? 'hierarchy',
        pulse,
        color: pulse ? color : previous.color,
        signalType: signal.signalType,
      } satisfies LinkEdgeData;
      if (pulse) existing.zIndex = 8;
      continue;
    }
    // Two nodes sitting in the same column would otherwise be joined by a wire
    // straight through whatever cards sit between them; send it down the right
    // edge instead.
    const from = centreOf(signal.sourceAgentId);
    const to = centreOf(signal.targetAgentId);
    const stacked = Math.abs(to.x - from.x) < NODE_W && Math.abs(to.y - from.y) > CHILD_H * 1.5;
    graphEdges.push(
      linkEdge(
        signal.sourceAgentId,
        signal.targetAgentId,
        {
          kind: 'signal',
          neural: layout === 'neural',
          pulse,
          color,
          signalType: signal.signalType,
        },
        pulse ? 8 : 3,
        stacked ? { sourceHandle: 'out-right', targetHandle: 'in-right' } : undefined,
      ),
    );
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
  const [layout, setLayout] = useState<LayoutMode>('neural');
  const [focusManagerId, setFocusManagerId] = useState<string | null>(null);
  const [inspectedId, setInspectedId] = useState<string | null>(null);
  const stageRef = useRef<HTMLDivElement>(null);
  const [paneAspect, setPaneAspect] = useState(1.9);

  // Quantised so a drag of the splitter only re-lays-out the tree when the pane
  // shape has actually changed enough to want a different number of rows.
  useEffect(() => {
    const stage = stageRef.current;
    if (!stage || typeof ResizeObserver === 'undefined') return undefined;
    const observer = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      if (!width || !height) return;
      const next = Math.min(8, Math.max(0.6, Math.round((width / height) * 2) / 2));
      setPaneAspect((current) => (current === next ? current : next));
    });
    observer.observe(stage);
    return () => observer.disconnect();
  }, []);

  const layouted = useMemo(
    () => buildGraph(state, layout, focusManagerId, paneAspect),
    // Stream-only state changes retain the graph revision, so token and
    // thinking frames do not reconstruct every React Flow node and edge.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [state.graphRevision, layout, focusManagerId, paneAspect],
  );

  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  // Anything that re-shapes the tree, not just the picked mode: a pane that
  // changed shape enough to want a different row count has to re-place nodes,
  // otherwise the recomputed layout is discarded in favour of the old spots.
  const placement = `${layout}|${paneAspect}`;
  const placementRef = useRef(placement);

  useEffect(() => {
    // Manual drags survive data updates, but a new placement always re-places
    // every node so the chosen layout is what the operator actually sees.
    const layoutChanged = placementRef.current !== placement;
    placementRef.current = placement;
    setNodes((current) => {
      const placed = new Map(current.map((node) => [node.id, node.position]));
      return layouted.nodes.map((node) => ({
        ...node,
        position: layoutChanged ? node.position : (placed.get(node.id) ?? node.position),
      }));
    });
    setEdges(layouted.edges);
  }, [placement, layouted, setEdges, setNodes]);

  const moveLayoutFocus = useCallback((event: KeyboardEvent<HTMLButtonElement>, index: number) => {
    let nextIndex: number | undefined;
    if (event.key === 'ArrowRight') nextIndex = (index + 1) % LAYOUTS.length;
    else if (event.key === 'ArrowLeft') nextIndex = (index - 1 + LAYOUTS.length) % LAYOUTS.length;
    else if (event.key === 'Home') nextIndex = 0;
    else if (event.key === 'End') nextIndex = LAYOUTS.length - 1;
    if (nextIndex == null) return;
    event.preventDefault();
    const controls =
      event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>('[role="tab"]');
    setLayout(LAYOUTS[nextIndex].id);
    controls?.[nextIndex]?.focus();
  }, []);

  const leafCount = Object.values(state.agents).filter((agent) =>
    CHILD_ROLES.has(agent.role),
  ).length;
  const plannedManagerCount = state.fanout.plannedManagers || state.managers.length;
  const plannedChildCount = state.fanout.plannedChildren || leafCount;
  const plannedPrimaryCount =
    plannedManagerCount || plannedChildCount
      ? 1 + plannedManagerCount + plannedChildCount
      : Object.keys(state.agents).length;

  // A run that called a model but recorded no agent failed on the way in, which
  // is a different story from a run that has not started yet.
  const attempted = state.fanout.requestAttempts > 0 || state.fanout.calledAgentIds.length > 0;

  const inspected = inspectedId ? state.agents[inspectedId] : undefined;
  const signature = `${layouted.structureKey}|${placement}|${focusManagerId ?? ''}|${inspected ? 'i' : ''}|${expanded ? 'x' : ''}`;
  const graphOutcomeCounts = layouted.nodes.reduce(
    (counts, node) => {
      const status = String((node.data as AgentNodeData | undefined)?.status ?? '').toLowerCase();
      if (['passed', 'completed', 'done'].includes(status)) counts.completed += 1;
      else if (status === 'partial') counts.partial += 1;
      else if (status === 'abandoned') counts.abandoned += 1;
      else if (status === 'skipped') counts.skipped += 1;
      return counts;
    },
    { completed: 0, partial: 0, abandoned: 0, skipped: 0 },
  );

  return (
    <section className={`dag-overview layout-${layout}`} aria-label="Agent neural network">
      <div className="dag-heading">
        <div>
          <span className="eyebrow">Live activation network</span>
          <h2>Orchestration neural map</h2>
        </div>
        <div className="dag-heading-actions">
          <div className="layout-toggle" role="tablist" aria-label="Graph layout">
            {LAYOUTS.map(({ id, label, icon: Icon }, index) => (
              <button
                key={id}
                type="button"
                role="tab"
                className={layout === id ? 'active' : ''}
                aria-selected={layout === id}
                aria-label={`${label} layout`}
                tabIndex={layout === id ? 0 : -1}
                onClick={() => setLayout(id)}
                onKeyDown={(event) => moveLayoutFocus(event, index)}
                title={`${label} layout`}
              >
                <Icon size={14} />
              </button>
            ))}
          </div>
          <button
            type="button"
            className={`ghost-chip ${focusManagerId ? 'active' : ''}`}
            onClick={() => setFocusManagerId(null)}
            disabled={!focusManagerId}
            title="Clear manager focus"
            aria-pressed={focusManagerId != null}
          >
            <Crosshair size={13} /> Clear focus
          </button>
          {onToggleExpand && (
            <button
              type="button"
              className={`graph-expand-button ${expanded ? 'active' : ''}`}
              onClick={onToggleExpand}
              title={expanded ? 'Exit full graph (Esc)' : 'Expand the graph to the whole window'}
              aria-pressed={expanded}
              aria-expanded={expanded}
            >
              {expanded ? <Minimize2 size={13} /> : <Maximize2 size={13} />}
              {expanded ? 'Restore workspace' : 'Fullscreen graph'}
            </button>
          )}
          <span className="graph-count">
            <BrainCircuit size={13} />
            {state.managers.length} workstreams · {plannedPrimaryCount} primary planned
            {' · '}
            {state.fanout.calledAgentIds.length} called
            {' · '}
            {state.fanout.requestAttempts} attempts
          </span>
          {state.timeline?.historyIncomplete && (
            <span
              className="graph-count"
              title={`History retained from event ${state.timeline.retainedFromSequence}; agent topology was restored from the canonical task snapshot.`}
            >
              History compacted
            </span>
          )}
        </div>
      </div>

      <div ref={stageRef} className={`dag-stage signal-graph${inspected ? ' has-inspector' : ''}`}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          minZoom={0.05}
          maxZoom={1.6}
          nodesDraggable
          nodesConnectable={false}
          elementsSelectable
          fitView
          fitViewOptions={{ padding: 0.12, minZoom: 0.05, maxZoom: 1.4 }}
          proOptions={{ hideAttribution: true }}
          defaultEdgeOptions={{ type: 'link', interactionWidth: 20 }}
          onPaneClick={() => setInspectedId(null)}
          onNodeClick={(_, node) => {
            if (String(node.id).startsWith('mgr-slot:')) return;
            const agent = state.agents[node.id];
            if (!agent) return;
            setInspectedId(agent.id);
            if (agent.role === 'manager') {
              setFocusManagerId((current) => (current === agent.id ? null : agent.id));
            }
            onOpenAgent(agent);
          }}
        >
          <GraphAutoFit signature={signature} />
          <Background gap={26} size={1} color="#1a222c" />
          {/* Docked, the stage is too short to give up a corner to the minimap. */}
          {expanded && (
            <MiniMap
              pannable
              zoomable
              style={{ width: 148, height: 96 }}
              maskColor="rgba(10,13,18,0.74)"
              bgColor="#0d1117"
              nodeStrokeWidth={1}
              nodeColor={(node) => {
                const agent = state.agents[node.id];
                const role = agent?.role ?? (node.data as AgentNodeData | undefined)?.role;
                return roleColor(role ?? 'agent');
              }}
            />
          )}
          <Controls showInteractive={false} />
        </ReactFlow>

        <div className="graph-outcome-counts" aria-label="Agent outcome counts">
          <span className="outcome-completed">{graphOutcomeCounts.completed} completed</span>
          <span className="outcome-partial">{graphOutcomeCounts.partial} partial</span>
          <span className="outcome-abandoned">{graphOutcomeCounts.abandoned} abandoned</span>
          <span className="outcome-skipped">{graphOutcomeCounts.skipped} skipped</span>
        </div>

        {!nodes.length && (
          <div className="dag-empty">
            <Bot size={20} />
            {attempted ? (
              <>
                <span>The run stopped before any agent was recorded.</span>
                <small>
                  {state.fanout.requestAttempts} model attempt
                  {state.fanout.requestAttempts === 1 ? '' : 's'}
                  {state.fanout.abortedRequests > 0
                    ? `, ${state.fanout.abortedRequests} aborted`
                    : ''}
                  . Open Calls for the provider error.
                </small>
              </>
            ) : (
              <>
                <span>Agent nodes appear when Director starts planning.</span>
                <small>Launch a hierarchy run to populate the map.</small>
              </>
            )}
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

        {inspected && (
          <aside className="graph-inspector" aria-label={`${inspected.title} details`}>
            <header>
              <div>
                <span className="eyebrow">{inspected.label ?? inspected.role}</span>
                <h3>{inspected.title}</h3>
              </div>
              <button
                type="button"
                className="icon-button"
                onClick={() => setInspectedId(null)}
                aria-label="Close inspector"
                title="Close inspector"
              >
                <X size={15} />
              </button>
            </header>
            <div className="inspector-row">
              <span>Status</span>
              <strong>{(inspected.executionState ?? inspected.status).replaceAll('_', ' ')}</strong>
            </div>
            <div className="inspector-row">
              <span>Model</span>
              <strong>
                {inspected.model ?? 'not reported'} · {inspected.effort ?? 'default effort'}
              </strong>
            </div>
            <div className="inspector-row mono">
              <span>Account</span>
              <strong>{inspected.account ?? 'not leased yet'}</strong>
            </div>
            {inspected.workstreamId && (
              <div className="inspector-row mono">
                <span>Workstream</span>
                <strong>{inspected.workstreamId}</strong>
              </div>
            )}
            {inspected.workContract && (
              <div className="inspector-row mono">
                <span>Work contract</span>
                <strong>
                  {inspected.workContract.id} · v{inspected.workContract.version} ·{' '}
                  {inspected.workContract.riskLevel} risk
                </strong>
              </div>
            )}
            <div className="inspector-row">
              <span>Provider</span>
              <strong>
                {inspected.requestAttempts} attempt{inspected.requestAttempts === 1 ? '' : 's'} ·{' '}
                {inspected.replayCount} replay{inspected.replayCount === 1 ? '' : 's'} ·{' '}
                {inspected.account ?? 'account redacted'}
              </strong>
            </div>
            {inspected.goal && (
              <div className="inspector-row">
                <span>Goal</span>
                <p>{inspected.goal}</p>
              </div>
            )}
            <div className="inspector-actions">
              <button
                type="button"
                className="secondary-button"
                onClick={() => onOpenAgent(inspected)}
              >
                <PanelRightOpen size={14} /> Open in agent workspace
              </button>
            </div>
          </aside>
        )}
      </div>
    </section>
  );
}
