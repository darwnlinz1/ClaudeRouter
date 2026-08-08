import { phaseIndex, STEPPER_LABELS } from '../lib/phases';

interface PhaseStepperProps {
  status: string;
}

export function PhaseStepper({ status }: PhaseStepperProps) {
  const current = phaseIndex(status);

  return (
    <ol className="phase-stepper" aria-label="Task phase">
      {STEPPER_LABELS.map((label, index) => {
        const complete = index < current;
        const active = index === current;
        return (
          <li
            key={label}
            className={`phase-step ${complete ? 'complete' : ''} ${active ? 'active' : ''}`}
            aria-current={active ? 'step' : undefined}
          >
            <span className="phase-step-dot" />
            <span className="phase-step-label">{label}</span>
          </li>
        );
      })}
    </ol>
  );
}
