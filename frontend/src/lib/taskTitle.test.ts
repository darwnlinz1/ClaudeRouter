import { describe, expect, it } from 'vitest';
import { shortTaskTitle } from './taskTitle';

describe('shortTaskTitle', () => {
  it('uses the explicit task name without exposing the raw prompt', () => {
    expect(
      shortTaskTitle({
        id: 'task-1',
        name: 'Invisible Virtual Camera',
        prompt: '# VAI TRÒ\nBạn là Senior Full-stack Developer',
        root: 'C:/workspace/camera',
      }),
    ).toBe('Invisible Virtual Camera');
  });

  it('falls back to the project folder for prompt-shaped legacy names', () => {
    expect(
      shortTaskTitle({
        id: 'task-2',
        name: '# VAI TRÒ\nBạn là Senior Full-stack Developer',
        prompt: '# VAI TRÒ\nBạn là Senior Full-stack Developer',
        root: 'C:/workspace/camera-project',
      }),
    ).toBe('camera-project');
  });
});
