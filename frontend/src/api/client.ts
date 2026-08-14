import type { AccountHealth, ApprovalItem, TaskSummary, WorkspaceSettings } from '../types';

/**
 * Resolve API origin.
 * When the UI is served by uvicorn (any port except Vite), always use same-origin
 * so a stale VITE_API_BASE_URL baked into the production bundle cannot point at a dead port.
 */
function resolveApiBase(): string {
  const configured =
    (import.meta.env.VITE_API_BASE_URL as string | undefined)?.replace(/\/$/, '') ?? '';
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

let sessionState: { apiBase: string; csrfToken: string } | null = null;
let sessionRequest: Promise<string> | null = null;

async function ensureLocalSession(apiBase: string, force = false): Promise<string> {
  if (!force && sessionState?.apiBase === apiBase) return sessionState.csrfToken;
  if (!force && sessionRequest) return sessionRequest;
  sessionRequest = fetch(`${apiBase}/api/session`, {
    method: 'GET',
    credentials: 'include',
    headers: { Accept: 'application/json' },
    cache: 'no-store',
  })
    .then(async (response) => {
      if (!response.ok)
        throw new ApiError('Could not establish a local API session.', response.status);
      const body = (await response.json()) as { csrf_token?: string };
      if (!body.csrf_token)
        throw new ApiError('The local API session did not return a CSRF token.');
      sessionState = { apiBase, csrfToken: body.csrf_token };
      return body.csrf_token;
    })
    .finally(() => {
      sessionRequest = null;
    });
  return sessionRequest;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const controller = new AbortController();
  let timedOut = false;
  const timeout = window.setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, 15_000);
  const callerSignal = init?.signal;
  const abortFromCaller = () => controller.abort(callerSignal?.reason);
  if (callerSignal?.aborted) abortFromCaller();
  else callerSignal?.addEventListener('abort', abortFromCaller, { once: true });
  const apiBase = resolveApiBase();
  const url = `${apiBase}${path}`;
  try {
    const method = (init?.method ?? 'GET').toUpperCase();
    const mutating = ['POST', 'PUT', 'PATCH', 'DELETE'].includes(method);
    const csrfToken = mutating ? await ensureLocalSession(apiBase) : null;
    const requestHeaders = {
      Accept: 'application/json',
      ...(csrfToken ? { 'X-CSRF-Token': csrfToken } : {}),
      ...init?.headers,
    };
    let response = await fetch(url, {
      ...init,
      credentials: 'include',
      headers: requestHeaders,
      signal: controller.signal,
    });
    if (mutating && response.status === 403) {
      const refreshedToken = await ensureLocalSession(apiBase, true);
      response = await fetch(url, {
        ...init,
        credentials: 'include',
        headers: { ...requestHeaders, 'X-CSRF-Token': refreshedToken },
        signal: controller.signal,
      });
    }
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
    if (error instanceof DOMException && error.name === 'AbortError' && timedOut) {
      throw new ApiError('The orchestrator API timed out.');
    }
    if (error instanceof DOMException && error.name === 'AbortError') throw error;
    const hint = typeof window !== 'undefined' ? window.location.origin : 'http://127.0.0.1:8000';
    const detail = error instanceof Error ? error.message : 'network error';
    throw new ApiError(
      `The orchestrator API is unavailable (tried ${url || path} from ${hint}: ${detail}). Start: python -m uvicorn server:app --host 127.0.0.1 --port 8000`,
    );
  } finally {
    window.clearTimeout(timeout);
    callerSignal?.removeEventListener('abort', abortFromCaller);
  }
}

export const api = {
  async listTasks(signal?: AbortSignal): Promise<TaskSummary[]> {
    const response = await request<{ tasks?: TaskSummary[] }>('/api/tasks', { signal });
    return Array.isArray(response.tasks) ? response.tasks : [];
  },

  async getTask(taskId: string, signal?: AbortSignal): Promise<TaskSummary> {
    const response = await request<TaskSummary | { task?: TaskSummary; snapshot?: TaskSummary }>(
      `/api/tasks/${encodeURIComponent(taskId)}`,
      { signal },
    );
    if ('task' in response && response.task) return response.task;
    if ('snapshot' in response && response.snapshot) return response.snapshot;
    return response as TaskSummary;
  },

  async getTimeline(
    taskId: string,
    after = 0,
    signal?: AbortSignal,
  ): Promise<Record<string, unknown>[]> {
    const result: Record<string, unknown>[] = [];
    let afterSequence = Math.max(0, after);
    let pageCursor: string | undefined;
    const pageSize = 5000;
    for (let page = 0; page < 100; page += 1) {
      const query = new URLSearchParams({ limit: String(pageSize) });
      if (pageCursor) query.set('cursor', pageCursor);
      else query.set('after', String(afterSequence));
      const response = await request<
        | Record<string, unknown>[]
        | {
            events?: Record<string, unknown>[];
            items?: Record<string, unknown>[];
            next_after?: number | string | null;
            next_cursor?: number | string | null;
            has_more?: boolean;
            page?: {
              events?: Record<string, unknown>[];
              next_after?: number | string | null;
              next_cursor?: number | string | null;
              has_more?: boolean;
            };
          }
      >(`/api/tasks/${encodeURIComponent(taskId)}/timeline?${query}`, { signal });
      const pageBody = Array.isArray(response) ? undefined : response.page;
      const events = Array.isArray(response)
        ? response
        : Array.isArray(response.events)
          ? response.events
          : Array.isArray(response.items)
            ? response.items
            : Array.isArray(pageBody?.events)
              ? pageBody.events
              : [];
      result.push(...events);
      const hasMore = Array.isArray(response)
        ? undefined
        : (response.has_more ?? pageBody?.has_more);
      const explicitCursor = Array.isArray(response)
        ? undefined
        : (response.next_cursor ?? pageBody?.next_cursor);
      const explicitAfter = Array.isArray(response)
        ? undefined
        : (response.next_after ?? pageBody?.next_after);
      if (hasMore === false || (!explicitCursor && !explicitAfter && events.length < pageSize)) {
        break;
      }
      const observedAfter = Math.max(
        afterSequence,
        ...events.map((event) => Number(event.sequence ?? 0)).filter(Number.isFinite),
      );
      if (explicitCursor != null && String(explicitCursor)) {
        const cursorText = String(explicitCursor);
        if (cursorText === pageCursor) break;
        pageCursor = cursorText;
        continue;
      }
      const nextAfter = Number(explicitAfter ?? observedAfter);
      if (!Number.isFinite(nextAfter) || nextAfter <= afterSequence) break;
      afterSequence = nextAfter;
      pageCursor = undefined;
    }
    return result;
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
        max_managers: input.settings.maxManagers || input.settings.maxParallelManagers,
        max_parallel_managers: input.settings.maxParallelManagers,
        max_workers_per_manager: input.settings.maxWorkersPerManager,
        max_parallel_workers_per_manager: input.settings.maxParallelWorkersPerManager ?? null,
        max_parallel_workers: input.settings.maxParallelWorkers,
        max_model_calls: input.settings.maxModelCalls,
        max_wall_clock_seconds: input.settings.maxWallClockSeconds,
        max_estimated_input_tokens: input.settings.maxEstimatedInputTokens,
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

  async listAccountHealth(signal?: AbortSignal): Promise<AccountHealth[]> {
    const response = await request<{
      accounts?: Array<Record<string, unknown>>;
    }>('/api/accounts', { signal });
    return (response.accounts ?? []).map((account) => ({
      accountId: String(account.account_id ?? ''),
      provider: String(account.provider ?? 'legacy_web'),
      state: String(account.state ?? 'unknown'),
      reason: account.reason ? String(account.reason) : undefined,
      cooldownUntil: account.cooldown_until == null ? undefined : Number(account.cooldown_until),
      cooldownActive: account.cooldown_active === true,
      activeLeases: Number(account.active_leases ?? 0),
      leaseExpiresAt:
        account.lease_expires_at == null ? undefined : Number(account.lease_expires_at),
    }));
  },

  async listApprovals(taskId: string, signal?: AbortSignal): Promise<ApprovalItem[]> {
    const response = await request<{
      approvals?: Array<Record<string, unknown>>;
    }>(`/api/tasks/${encodeURIComponent(taskId)}/approvals`, { signal });
    return (response.approvals ?? []).map((approval) => ({
      approvalId: String(approval.approval_id ?? ''),
      taskId: String(approval.task_id ?? taskId),
      workstreamId: approval.workstream_id ? String(approval.workstream_id) : undefined,
      kind: String(approval.kind ?? 'action'),
      target: String(approval.target ?? ''),
      reason: String(approval.reason ?? ''),
      status: String(approval.status ?? 'pending') as ApprovalItem['status'],
      createdAt: String(approval.created_at ?? ''),
      decidedAt: approval.decided_at ? String(approval.decided_at) : undefined,
    }));
  },

  decideApproval(
    taskId: string,
    approvalId: string,
    decision: 'approved' | 'rejected',
    reason = '',
  ): Promise<Record<string, unknown>> {
    return request(
      `/api/tasks/${encodeURIComponent(taskId)}/approvals/${encodeURIComponent(approvalId)}/decision`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ decision, reason }),
      },
    );
  },

  streamUrl(taskId: string, after: number): string {
    const query = new URLSearchParams({ after: String(Math.max(0, after)) });
    return `${resolveApiBase()}/api/stream/${encodeURIComponent(taskId)}?${query}`;
  },
};
