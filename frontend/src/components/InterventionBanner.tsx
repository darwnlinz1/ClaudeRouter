import { AlertTriangle } from 'lucide-react';

interface InterventionBannerProps {
  visible: boolean;
  message?: string;
  onFocus: () => void;
}

export function InterventionBanner({ visible, message, onFocus }: InterventionBannerProps) {
  if (!visible) return null;

  return (
    <div className="intervention-banner" role="alert">
      <AlertTriangle size={15} />
      <div>
        <strong>Input required</strong>
        <span>{message ?? 'An agent is waiting for your response.'}</span>
      </div>
      <button type="button" className="primary-button" onClick={onFocus}>
        Respond
      </button>
    </div>
  );
}
