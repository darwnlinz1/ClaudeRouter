import { useState, type KeyboardEvent, type PointerEvent } from 'react';
import {
  WORKSPACE_SPLIT_DEFAULT,
  WORKSPACE_SPLIT_MAX,
  WORKSPACE_SPLIT_MIN,
  WORKSPACE_SPLIT_STEP,
} from '../lib/preferences';

interface WorkspaceSplitterProps {
  value: number;
  onChange: (value: number) => void;
}

const clampSplit = (value: number) =>
  Math.min(WORKSPACE_SPLIT_MAX, Math.max(WORKSPACE_SPLIT_MIN, value));

export function WorkspaceSplitter({ value, onChange }: WorkspaceSplitterProps) {
  const [dragging, setDragging] = useState(false);

  const updateFromPointer = (event: PointerEvent<HTMLDivElement>) => {
    const container = event.currentTarget.parentElement;
    if (!container) return;
    const bounds = container.getBoundingClientRect();
    if (!bounds.height) return;
    onChange(clampSplit(((event.clientY - bounds.top) / bounds.height) * 100));
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    let next: number | undefined;
    if (event.key === 'ArrowUp') next = value - WORKSPACE_SPLIT_STEP;
    if (event.key === 'ArrowDown') next = value + WORKSPACE_SPLIT_STEP;
    if (event.key === 'Home') next = WORKSPACE_SPLIT_MIN;
    if (event.key === 'End') next = WORKSPACE_SPLIT_MAX;
    if (next === undefined) return;
    event.preventDefault();
    onChange(clampSplit(next));
  };

  return (
    <div
      className={`workspace-splitter${dragging ? ' dragging' : ''}`}
      role="separator"
      aria-label="Resize Workspace and Agent workspace panels"
      aria-orientation="horizontal"
      aria-valuemin={WORKSPACE_SPLIT_MIN}
      aria-valuemax={WORKSPACE_SPLIT_MAX}
      aria-valuenow={Math.round(value)}
      aria-valuetext={`${Math.round(value)}% Workspace, ${Math.round(100 - value)}% Agent workspace`}
      tabIndex={0}
      onDoubleClick={() => onChange(WORKSPACE_SPLIT_DEFAULT)}
      onKeyDown={handleKeyDown}
      onPointerDown={(event) => {
        if (event.button !== 0) return;
        event.preventDefault();
        event.currentTarget.setPointerCapture(event.pointerId);
        setDragging(true);
        updateFromPointer(event);
      }}
      onPointerMove={(event) => {
        if (event.currentTarget.hasPointerCapture(event.pointerId)) {
          updateFromPointer(event);
        }
      }}
      onPointerUp={(event) => {
        if (event.currentTarget.hasPointerCapture(event.pointerId)) {
          event.currentTarget.releasePointerCapture(event.pointerId);
        }
        setDragging(false);
      }}
      onPointerCancel={() => setDragging(false)}
      onLostPointerCapture={() => setDragging(false)}
    >
      <span aria-hidden="true" />
    </div>
  );
}
