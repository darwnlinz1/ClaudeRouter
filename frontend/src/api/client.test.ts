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

    const events = await api.getTimeline('task-1');

    expect(events.map((event) => event.sequence)).toEqual([1, 2]);
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
});
