import type { TaskSummary, WorkspaceSettings } from '../types';

/**
 * Resolve API origin.
 * When the UI is served by uvicorn (any port except Vite), always use same-origin
 * so a stale VITE_API_BASE_URL baked into the production bundle cannot point at a dead port.
 */
function resolveApiBase(): string {
  const configured = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.replace(/\/$/, '') ?? '';
  if (typeof window === 'undefined') return configured;
  const port = window.location.port;
  const isViteDev = port === '5173' || port === '5174' || port === '5175';
  if (isViteDev) return configured;
  return '';
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), 15_000);
  const apiBase = resolveApiBase();
  const url = `${apiBase}${path}`;
  try {
    const response = await fetch(url, {
      ...init,
      headers: { Accept: 'application/json', ...init?.headers },
      signal: controller.signal,
    });
    if (!response.ok) {
      let detail = `Request failed with HTTP ${response.status}`;
      try {
        const body = (await response.json()) as { detail?: string };
        detail = body.detail ?? detail;
      } catch {
        // Keep the HTTP fallback when the server did not return JSON.
      }
      throw new ApiError(detail, response.status);
    }
    return (await response.json()) as T;
  } catch (error) {
    if (error instanceof ApiError) throw error;
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw new ApiError('The orchestrator API timed out.');
    }
    const hint = typeof window !== 'undefined' ? window.location.origin : 'http://127.0.0.1:8000';
    const detail = error instanceof Error ? error.message : 'network error';
    throw new ApiError(
      `The orchestrator API is unavailable (tried ${url || path} from ${hint}: ${detail}). Start: python -m uvicorn server:app --host 127.0.0.1 --port 8000`,
    );
  } finally {
    window.clearTimeout(timeout);
  }
}

export const api = {
  async listTasks(): Promise<TaskSummary[]> {
    const response = await request<{ tasks?: TaskSummary[] }>('/api/tasks');
    return Array.isArray(response.tasks) ? response.tasks : [];
  },

  getTask(taskId: string): Promise<TaskSummary> {
    return request<TaskSummary>(`/api/tasks/${encodeURIComponent(taskId)}`);
  },

  async getTimeline(taskId: string, after = 0): Promise<Record<string, unknown>[]> {
    const response = await request<{ events?: Record<string, unknown>[] }>(
      `/api/tasks/${encodeURIComponent(taskId)}/timeline?after=${Math.max(0, after)}&limit=5000`,
    );
    return Array.isArray(response.events) ? response.events : [];
  },

  pickFolder(): Promise<{ root?: string; error?: string }> {
    return request('/api/pick-folder');
  },

  createHierarchyTask(input: {
    name: string;
    root: string;
    task: string;
    projectMode: 'edit' | 'new_project';
    testCmd?: string;
    settings: WorkspaceSettings;
  }): Promise<{ status: string; task_id: string }> {
    return request('/api/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: input.name,
        root: input.root,
        task: input.task,
        files: '',
        mode: 'orchestrator',
        project_mode: input.projectMode,
        test_cmd: input.testCmd || null,
        hierarchy_enabled: true,
        max_parallel_managers: input.settings.maxParallelManagers,
        max_workers_per_manager: input.settings.maxWorkersPerManager,
        max_parallel_workers: input.settings.maxParallelWorkers,
        director_model: input.settings.roleProfiles.director.model,
        director_effort: input.settings.roleProfiles.director.effort,
        manager_model: input.settings.roleProfiles.manager.model,
        manager_effort: input.settings.roleProfiles.manager.effort,
        model: input.settings.roleProfiles.worker.model,
        effort: input.settings.roleProfiles.worker.effort,
        reviewer_model: input.settings.roleProfiles.tester.model,
        reviewer_effort: input.settings.roleProfiles.tester.effort,
      }),
    });
  },

  updateAgentConfig(
    taskId: string,
    agentId: string,
    model: string,
    effort: string,
  ): Promise<{ status: string; applies_to?: string }> {
    return request(
      `/api/tasks/${encodeURIComponent(taskId)}/agents/${encodeURIComponent(agentId)}/config`,
      {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ model, effort }),
      },
    );
  },

  stopTask(taskId: string): Promise<{ status: string }> {
    return request(`/api/stop/${encodeURIComponent(taskId)}`, { method: 'POST' });
  },

  resumeTask(taskId: string): Promise<{ status: string; task_id?: string }> {
    return request(`/api/tasks/${encodeURIComponent(taskId)}/resume`, { method: 'POST' });
  },

  deleteTask(taskId: string): Promise<{ status: string }> {
    return request(`/api/tasks/${encodeURIComponent(taskId)}`, { method: 'DELETE' });
  },

  streamUrl(taskId: string, after: number): string {
    const query = new URLSearchParams({ after: String(Math.max(0, after)) });
    return `${resolveApiBase()}/api/stream/${encodeURIComponent(taskId)}?${query}`;
  },
};
