# Provider incident runbook

## Repeated HTTP 429

1. Open account health and confirm which opaque account IDs are cooling down.
2. Do not edit the active task or resend it manually. The runtime replays the
   same logical request on another eligible account.
3. If every account is cooling down, allow the task to wait until the earliest
   cooldown expires. Add a valid cookie file only through the normal local
   credential procedure.
4. Search correlated events for `account_rate_limited`, `account_switch`, and
   the same `logical_request_id`.
5. Escalate if an account is selected before its cooldown or if replay bytes
   differ. Preserve redacted events; never attach cookie contents.

## Incomplete stream

1. Confirm an `IncompleteStreamError` or missing terminal provider event.
2. Verify the attempt is failed/retryable rather than completed.
3. Check local connectivity and Web Claude session validity.
4. Resume only through the task control so the same logical request and attempt
   history remain auditable.

## Authentication failure

1. Stop new work on the affected account.
2. Replace the expired local cookie file outside logs and project roots.
3. Confirm account health recovers with a small non-mutating task.
