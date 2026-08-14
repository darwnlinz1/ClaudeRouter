interface HierarchyPreviewProps {
  managers: number;
  workersPerManager: number;
}

const MAX_MANAGERS = 4;
const MAX_CODERS = 3;

export function HierarchyPreview({ managers, workersPerManager }: HierarchyPreviewProps) {
  const managerCount = Math.max(1, managers);
  const coderCount = Math.max(1, workersPerManager - 1);
  const displayManagers = Math.min(managerCount, MAX_MANAGERS);
  const displayWorkers = Math.min(coderCount, MAX_CODERS);
  const hiddenManagers = managerCount - displayManagers;
  const hiddenWorkers = coderCount - displayWorkers;

  return (
    <div className="hierarchy-org-preview" aria-label="Hierarchy preview">
      <div className="hierarchy-org-director">
        <span>D</span>
        <small>Director</small>
      </div>
      <div className="hierarchy-org-trunk" />
      <div className="hierarchy-org-managers">
        {Array.from({ length: displayManagers }, (_, managerIndex) => (
          <div className="hierarchy-org-branch" key={managerIndex}>
            <div className="hierarchy-org-manager">
              <span>M</span>
              <small>Manager</small>
            </div>
            <div className="hierarchy-org-workers">
              {Array.from({ length: displayWorkers }, (_, workerIndex) => (
                <div className="hierarchy-org-worker" key={workerIndex}>
                  <span>W</span>
                </div>
              ))}
              <div className="hierarchy-org-worker hierarchy-org-tester">
                <span>T</span>
              </div>
              {hiddenWorkers > 0 && <div className="hierarchy-org-more">+{hiddenWorkers}</div>}
            </div>
          </div>
        ))}
        {hiddenManagers > 0 && <div className="hierarchy-org-more managers">+{hiddenManagers}</div>}
      </div>
    </div>
  );
}
