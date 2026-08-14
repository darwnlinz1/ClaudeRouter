// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from './client';

const jsonResponse = (body: unknown) =>
  new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('frontend API compatibility', () => {
  it('reads direct timeline pages and follows an explicit cursor page', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          events: [{ sequence: 1, type: 'status' }],
          next_cursor: 'page-two',
          has_more: true,
          latest_sequence: 2,
          retained_from_sequence: 1,
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          page: {
            events: [{ sequence: 2, type: 'done' }],
            has_more: false,
          },
        }),
      );
    vi.stubGlobal('fetch', fetchMock);

    const timeline = await api.getTimeline('task-1');

    expect(timeline.events.map((event) => event.sequence)).toEqual([1, 2]);
    expect(timeline).toMatchObject({
      latestSequence: 2,
      retainedFromSequence: 1,
      historyIncomplete: false,
    });
    expect(String(fetchMock.mock.calls[0][0])).toContain('after=0');
    expect(String(fetchMock.mock.calls[1][0])).toContain('cursor=page-two');
  });

  it('accepts a wrapped task snapshot without breaking the direct endpoint contract', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        jsonResponse({
          snapshot: { id: 'task-1', name: 'Wrapped snapshot', status: 'RUNNING' },
        }),
      ),
    );

    await expect(api.getTask('task-1')).resolves.toMatchObject({
      id: 'task-1',
      name: 'Wrapped snapshot',
    });
  });

  it('sends frontend fan-out caps without a hidden task-count limit', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ csrf_token: 'csrf-test' }))
      .mockResolvedValueOnce(jsonResponse({ task_id: 'task-large' }));
    vi.stubGlobal('fetch', fetchMock);

    await api.createHierarchyTask({
      name: 'Large fan-out',
      root: 'C:/workspace',
      task: 'Build independent packages',
      projectMode: 'new_project',
      testCmd: 'python -m pytest -q',
      settings: {
        maxManagers: 20,
        maxParallelManagers: 8,
        maxWorkersPerManager: 21,
        maxParallelWorkersPerManager: 20,
        maxParallelWorkers: 32,
        mockWhenUnavailable: false,
        roleProfiles: {
          director: { model: 'claude-sonnet-5', effort: 'max' },
          manager: { model: 'claude-sonnet-5', effort: 'max' },
          worker: { model: 'claude-sonnet-5', effort: 'max' },
          tester: { model: 'claude-sonnet-5', effort: 'high' },
        },
      },
    });

    const requestInit = fetchMock.mock.calls[1][1] as RequestInit;
    const payload = JSON.parse(String(requestInit.body));
    expect(payload).toMatchObject({
      max_managers: 20,
      max_parallel_managers: 8,
      max_workers_per_manager: 21,
      max_parallel_workers_per_manager: 20,
      max_parallel_workers: 32,
    });
  });
});
