import { useEffect, useMemo, useRef, useState } from 'react';
import { Search } from 'lucide-react';

export interface CommandItem {
  id: string;
  label: string;
  hint?: string;
  shortcut?: string;
  disabled?: boolean;
  run: () => void;
}

interface CommandPaletteProps {
  open: boolean;
  onClose: () => void;
  commands: CommandItem[];
}

export function CommandPalette({ open, onClose, commands }: CommandPaletteProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [query, setQuery] = useState('');
  const [highlight, setHighlight] = useState(0);

  const filtered = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return commands.filter((command) => !command.disabled);
    return commands.filter(
      (command) =>
        !command.disabled &&
        (command.label.toLowerCase().includes(needle) ||
          command.hint?.toLowerCase().includes(needle)),
    );
  }, [commands, query]);

  useEffect(() => {
    if (!open) {
      setQuery('');
      setHighlight(0);
      return undefined;
    }
    const timer = window.setTimeout(() => inputRef.current?.focus(), 0);
    return () => window.clearTimeout(timer);
  }, [open]);

  useEffect(() => {
    setHighlight(0);
  }, [query]);

  useEffect(() => {
    if (!open) return undefined;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        onClose();
        return;
      }
      if (event.key === 'ArrowDown') {
        event.preventDefault();
        setHighlight((value) => Math.min(value + 1, Math.max(filtered.length - 1, 0)));
        return;
      }
      if (event.key === 'ArrowUp') {
        event.preventDefault();
        setHighlight((value) => Math.max(value - 1, 0));
        return;
      }
      if (event.key === 'Enter' && filtered[highlight]) {
        event.preventDefault();
        filtered[highlight].run();
        onClose();
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [filtered, highlight, onClose, open]);

  if (!open) return null;

  return (
    <div className="command-palette-backdrop" onMouseDown={onClose}>
      <section
        className="command-palette"
        role="dialog"
        aria-modal="true"
        aria-label="Command palette"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="command-palette-search">
          <Search size={14} />
          <input
            ref={inputRef}
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Type a command…"
            aria-label="Filter commands"
            role="combobox"
            aria-autocomplete="list"
            aria-expanded="true"
            aria-controls="command-palette-list"
            aria-activedescendant={
              filtered[highlight] ? `command-option-${filtered[highlight].id}` : undefined
            }
          />
        </div>
        <ul className="command-palette-list" id="command-palette-list" role="listbox">
          {filtered.map((command, index) => (
            <li key={command.id} role="presentation">
              <button
                type="button"
                id={`command-option-${command.id}`}
                role="option"
                aria-selected={index === highlight}
                className={index === highlight ? 'active' : ''}
                disabled={command.disabled}
                onMouseEnter={() => setHighlight(index)}
                onClick={() => {
                  command.run();
                  onClose();
                }}
              >
                <span>{command.label}</span>
                {command.hint && <small>{command.hint}</small>}
                {command.shortcut && <kbd>{command.shortcut}</kbd>}
              </button>
            </li>
          ))}
          {!filtered.length && <li className="command-palette-empty">No matching commands</li>}
        </ul>
      </section>
    </div>
  );
}
