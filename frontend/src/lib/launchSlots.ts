/** Backend ceiling for max_parallel_workers. */
export const MAX_GLOBAL_WORKER_SLOTS = 64;

/**
 * Global worker slots a launch should request.
 *
 * The wizard decides how wide the plan is, so the slot count has to be at least
 * that wide. Leaving it at an unrelated default meant a run configured for five
 * managers of four coders executed eight coders at a time and queued the rest,
 * which looks like the fan-out silently shrinking.
 */
export const launchWorkerSlots = (
  managers: number,
  workersPerManager: number,
  configuredSlots: number,
): number => {
  const plannedCoders = Math.max(0, managers) * Math.max(1, workersPerManager - 1);
  return Math.max(configuredSlots, Math.min(MAX_GLOBAL_WORKER_SLOTS, plannedCoders));
};

/**
 * Managers allowed to execute at once.
 *
 * Workers only run inside an executing workstream, so a manager cap below the
 * number of managers idles every worker in the streams that did not get a slot.
 * The plan already decided how many managers there are; running fewer of them
 * just defers work without saving anything.
 */
export const launchManagerSlots = (managers: number, configuredSlots: number): number =>
  Math.max(1, Math.min(Math.max(managers, configuredSlots), managers));
