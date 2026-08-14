function firstNonEmptyLine(...values: (string | undefined)[]): string {
  for (const value of values) {
    if (!value) continue;
    const line = value
      .split('\n')
      .map((part) => part.trim())
      .find(Boolean);
    if (line) return line;
  }
  return '';
}

function stripRolePrefix(text: string): string {
  return text.replace(/^Bạn là[^\n.]*/i, '').trim();
}

function truncateTitle(text: string): string {
  const cleaned = stripRolePrefix(text);
  if (!cleaned) return '';
  const sentence = cleaned.match(/^[^.!?]+[.!?]?/)?.[0]?.trim() ?? cleaned;
  if (sentence.length <= 48) return sentence;
  return `${sentence.slice(0, 48).trim()}…`;
}

function pathBasename(path?: string): string {
  if (!path) return '';
  const normalized = path.replace(/\\/g, '/').replace(/\/+$/, '');
  const parts = normalized.split('/');
  return parts[parts.length - 1] ?? '';
}

export function shortTaskTitle(task: {
  name?: string;
  prompt?: string;
  root?: string;
  id: string;
}): string {
  const rawName = firstNonEmptyLine(task.name);
  const promptLikeName =
    /^#{1,6}\s|^Bạn là\b|^You are\b/i.test(rawName) || Boolean(task.name?.includes('\n'));
  const fromName = promptLikeName ? '' : truncateTitle(rawName);
  if (fromName) return fromName;
  const fromRoot = pathBasename(task.root);
  if (fromRoot) return fromRoot;
  return task.id.slice(0, 8);
}

export function taskProjectKey(root?: string): string {
  return pathBasename(root) || 'No project';
}
