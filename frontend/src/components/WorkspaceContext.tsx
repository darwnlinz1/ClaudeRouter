import { createContext, useContext } from 'react';
import type { AgentInstance } from '../types';

interface WorkspaceContextValue {
  agents: Record<string, AgentInstance>;
  markFocused: (agentId: string) => void;
  configureAgent: (agentId: string, model: string, effort: string) => Promise<void>;
}

const WorkspaceContext = createContext<WorkspaceContextValue>({
  agents: {},
  markFocused: () => undefined,
  configureAgent: async () => undefined,
});

export const WorkspaceProvider = WorkspaceContext.Provider;
export const useWorkspaceContext = () => useContext(WorkspaceContext);
