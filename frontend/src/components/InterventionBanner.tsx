import type { ReactNode } from 'react';
import { AlertTriangle } from 'lucide-react';

interface InterventionBannerProps {
  visible: boolean;
  title?: string;
  message?: string;
  onFocus: () => void;
  actions?: ReactNode;
}

export function InterventionBanner({
  visible,
  title,
  message,
  onFocus,
  actions,
}: InterventionBannerProps) {
  if (!visible) return null;

  return (
    <div className="intervention-banner" role="alert">
      <AlertTriangle size={15} />
      <div>
        <strong>{title ?? 'Input required'}</strong>
        <span>{message ?? 'An agent is waiting for your response.'}</span>
      </div>
      <div className="intervention-actions">
        {actions ?? (
          <button type="button" className="primary-button" onClick={onFocus}>
            Respond
          </button>
        )}
      </div>
    </div>
  );
}
