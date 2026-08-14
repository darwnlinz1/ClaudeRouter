# Stuck task and lease runbook

1. Record the task ID, phase, latest durable event sequence, and project root.
2. Check whether the task is waiting for approval, budget, dependency,
   provider cooldown, or a write-scope claim. These are waiting states, not
   crashes.
3. Inspect project and account lease expiry. Do not delete a live lease.
4. If the owning process is gone, wait for TTL expiry, restart the service, and
   resume through the operator control. Startup reconciliation marks orphaned
   active work interrupted.
5. Compare planned and terminal counts. Do not mark the task complete unless
   reconciliation is balanced.
6. If progress remains impossible, stop the task, export the redacted timeline,
   create a verified backup, and retain the project snapshot for diagnosis.

Never modify SQLite rows by hand. Never start a second orchestrator against the
same project root to bypass a lease.
