"""Launch and optionally monitor one real hierarchy run through the local API."""

from __future__ import annotations

import argparse
import http.cookiejar
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any, BinaryIO, TextIO

DEFAULT_API = os.environ.get("ORCH_API", "http://127.0.0.1:8000")
TERMINAL_STATUSES = frozenset(
    {
        "COMPLETED",
        "PARTIAL",
        "FAILED",
        "STOPPED",
        "MAX_TURNS",
        "INTERRUPTED",
        "CANCELLED",
        "ERROR",
    }
)
INTERRUPTED_STABILITY_SECONDS = 30.0


class LocalApiClient:
    """Same-origin client that keeps the session cookie and CSRF token together."""

    def __init__(self, base: str = DEFAULT_API) -> None:
        self.base = base.rstrip("/")
        self._jar = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self._jar)
        )
        self.csrf_token: str | None = None

    def get(self, path: str, *, timeout: float = 30) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self.base}{path}",
            headers={"Origin": self.base},
        )
        with self._opener.open(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def open_session(self) -> str:
        token = str(self.get("/api/session")["csrf_token"])
        self.csrf_token = token
        return token

    def post(
        self,
        path: str,
        payload: dict[str, Any],
        *,
        timeout: float = 120,
    ) -> dict[str, Any]:
        if self.csrf_token is None:
            self.open_session()
        request = urllib.request.Request(
            f"{self.base}{path}",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "X-CSRF-Token": str(self.csrf_token),
                "Origin": self.base,
            },
            method="POST",
        )
        with self._opener.open(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Start a new-project hierarchy task. The complete prompt is read "
            "from --prompt-file, --prompt-task-id, --prompt-transcript, or stdin."
        )
    )
    parser.add_argument("destination", type=Path, help="Empty new-project destination")
    parser.add_argument(
        "--prompt-file",
        type=Path,
        help="UTF-8/UTF-8-BOM prompt file; omit to read stdin",
    )
    parser.add_argument(
        "--prompt-task-id",
        help="Replay the complete prompt stored by an existing local task",
    )
    parser.add_argument(
        "--prompt-transcript",
        type=Path,
        help="Extract the latest user prompt beginning at --prompt-marker from JSONL",
    )
    parser.add_argument("--prompt-marker", default="# VAI TRÒ")
    parser.add_argument("--name", help="Task display name (defaults to prompt first line)")
    parser.add_argument("--api", default=DEFAULT_API, help="Local orchestrator API origin")
    parser.add_argument("--wait", action="store_true", help="Wait for a terminal task state")
    parser.add_argument(
        "--wait-timeout",
        type=float,
        default=0,
        help="Maximum wait seconds; 0 waits indefinitely",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Task/timeline polling interval in seconds",
    )
    parser.add_argument(
        "--timeline-jsonl",
        type=Path,
        help="Timeline capture path (a task-named file is used with --wait by default)",
    )
    parser.add_argument(
        "--test-command",
        help="Integration test command passed verbatim to the API",
    )
    parser.add_argument(
        "--approval-mode",
        choices=("manual", "staging_auto"),
        default="staging_auto",
        help="staging_auto is confined by the server to managed new-project staging",
    )
    parser.add_argument(
        "--hierarchy",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--max-managers", type=int, default=4)
    parser.add_argument("--max-parallel-managers", type=int, default=2)
    parser.add_argument("--max-workers-per-manager", type=int, default=5)
    parser.add_argument("--max-parallel-workers-per-manager", type=int)
    parser.add_argument("--max-parallel-workers", type=int, default=4)
    parser.add_argument("--max-turns", type=int)
    parser.add_argument("--model", default="claude-sonnet-5")
    parser.add_argument("--effort", default="max")
    parser.add_argument("--supervisor-model", default="claude-sonnet-5")
    parser.add_argument("--supervisor-effort", default="max")
    parser.add_argument("--reviewer-model", default="claude-sonnet-5")
    parser.add_argument("--reviewer-effort", default="high")
    parser.add_argument(
        "--auto-apply",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--create-zip",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    return parser


def _decode_prompt(value: str | bytes) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8-sig")
    return value[1:] if value.startswith("\ufeff") else value


def read_prompt(
    prompt_file: Path | None,
    stdin: TextIO | BinaryIO | None = None,
) -> str:
    """Read the entire prompt without truncating or stripping meaningful text."""

    if prompt_file is not None:
        prompt = prompt_file.read_bytes().decode("utf-8-sig")
    else:
        source = stdin if stdin is not None else sys.stdin
        binary_source = getattr(source, "buffer", source)
        prompt = _decode_prompt(binary_source.read())
    if not prompt.strip():
        raise ValueError("Prompt is empty; use --prompt-file or pipe UTF-8 text to stdin.")
    return prompt


def read_prompt_from_transcript(path: Path, marker: str) -> str:
    """Extract the latest complete user prompt from a Cursor JSONL transcript."""

    selected: str | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            entry = json.loads(line)
        except (TypeError, ValueError):
            continue
        if entry.get("role") != "user":
            continue
        content = (entry.get("message") or {}).get("content") or []
        text = "\n".join(
            str(item.get("text") or "")
            for item in content
            if isinstance(item, dict) and item.get("type") == "text"
        )
        if marker not in text:
            continue
        candidate = text[text.index(marker) :]
        if "</user_query>" in candidate:
            candidate = candidate.split("</user_query>", 1)[0]
        if candidate.strip():
            selected = candidate.strip() + "\n"
    if selected is None:
        raise ValueError(f"No user prompt beginning with {marker!r} exists in {path}")
    return selected


def _default_name(prompt: str) -> str:
    first_line = next((line.strip() for line in prompt.splitlines() if line.strip()), "")
    return first_line[:80] or "Live orchestrator task"


def build_payload(args: argparse.Namespace, prompt: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": args.name or _default_name(prompt),
        "root": str(args.destination.resolve()),
        "task": prompt,
        "mode": "orchestrator",
        "project_mode": "new_project",
        "approval_mode": args.approval_mode,
        "hierarchy_enabled": bool(args.hierarchy),
        "max_managers": args.max_managers,
        "max_parallel_managers": args.max_parallel_managers,
        "max_workers_per_manager": args.max_workers_per_manager,
        "max_parallel_workers": args.max_parallel_workers,
        "auto_apply": bool(args.auto_apply),
        "create_zip": bool(args.create_zip),
        "model": args.model,
        "effort": args.effort,
        "supervisor_model": args.supervisor_model,
        "supervisor_effort": args.supervisor_effort,
        "reviewer_model": args.reviewer_model,
        "reviewer_effort": args.reviewer_effort,
    }
    if args.max_parallel_workers_per_manager is not None:
        payload["max_parallel_workers_per_manager"] = (
            args.max_parallel_workers_per_manager
        )
    if args.max_turns is not None:
        payload["max_turns"] = args.max_turns
    if args.test_command:
        payload["test_cmd"] = args.test_command
    return payload


def append_timeline_jsonl(
    stream: TextIO,
    events: list[dict[str, Any]],
) -> None:
    for event in events:
        stream.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    stream.flush()


def summarize_diagnostics(
    events: list[dict[str, Any]],
    task: dict[str, Any],
) -> dict[str, Any]:
    event_counts = Counter(str(event.get("type") or "event") for event in events)
    failure_events = [
        event
        for event in events
        if str(event.get("type") or "").endswith(("_failed", "_failure"))
        or event.get("type")
        in {
            "account_invalidated",
            "event_schema_validation_failure",
            "protocol_retry",
        }
    ]
    return {
        "task_id": task.get("id"),
        "status": task.get("status"),
        "phase": task.get("phase"),
        "last_error": task.get("last_error"),
        "events_captured": len(events),
        "model_requests": {
            "started": event_counts["model_request_started"],
            "completed": event_counts["model_request_completed"],
            "failed": event_counts["model_request_failed"],
            "aborted": event_counts["model_request_aborted"],
            "protocol_retries": event_counts["protocol_retry"],
        },
        "accounts": {
            "switches": event_counts["account_switch"],
            "rate_limited": event_counts["account_rate_limited"],
            "invalidated": event_counts["account_invalidated"],
            "waits": event_counts["agent_waiting_account"],
        },
        "event_schema": {
            "repaired": event_counts["event_schema_validation_repaired"],
            "rejected": event_counts["event_schema_validation_failure"],
        },
        "failure_event_count": len(failure_events),
        "failure_summaries": [
            {
                "type": event.get("type"),
                "role": event.get("role"),
                "error": event.get("error") or event.get("summary") or event.get("reason"),
                "sequence": event.get("sequence"),
            }
            for event in failure_events[-20:]
        ],
    }


def _timeline_path(
    requested: Path | None,
    destination: Path,
    task_id: str,
) -> Path:
    if requested is not None:
        return requested
    return destination.parent / f"{destination.name}-{task_id[:8]}.timeline.jsonl"


def wait_for_task(
    client: LocalApiClient,
    task_id: str,
    *,
    timeline_path: Path,
    poll_interval: float,
    timeout: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    deadline = time.monotonic() + timeout if timeout > 0 else None
    after = 0
    events: list[dict[str, Any]] = []
    last_status: str | None = None
    terminal_observed = False
    unfinalized_interrupted_since: float | None = None
    timeline_path.parent.mkdir(parents=True, exist_ok=True)
    with timeline_path.open("w", encoding="utf-8", newline="\n") as timeline:
        while True:
            query = urllib.parse.urlencode({"after": after, "limit": 1000})
            page = client.get(f"/api/tasks/{task_id}/timeline?{query}")
            batch = [
                dict(event)
                for event in page.get("events", [])
                if isinstance(event, dict)
            ]
            if batch:
                append_timeline_jsonl(timeline, batch)
                events.extend(batch)
                after = max(after, max(int(event.get("sequence") or 0) for event in batch))
            task = client.get(f"/api/tasks/{task_id}")
            status = str(task.get("status") or "UNKNOWN")
            if status != last_status:
                print(
                    json.dumps(
                        {
                            "task_id": task_id,
                            "status": status,
                            "phase": task.get("phase"),
                            "timeline_sequence": after,
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                last_status = status
            now = time.monotonic()
            if status == "INTERRUPTED" and not task.get("finished_at"):
                if batch or unfinalized_interrupted_since is None:
                    unfinalized_interrupted_since = now
                interruption_stable = (
                    now - unfinalized_interrupted_since
                    >= INTERRUPTED_STABILITY_SECONDS
                )
            else:
                unfinalized_interrupted_since = None
                interruption_stable = True
            terminal_ready = status in TERMINAL_STATUSES and (
                status != "INTERRUPTED"
                or bool(task.get("finished_at"))
                or interruption_stable
            )
            if terminal_ready and not bool(page.get("has_more")):
                if terminal_observed:
                    return task, events
                # The task snapshot is finalized immediately before the worker
                # emits its closing event. One confirmation poll captures that
                # tail instead of racing the final JSONL append.
                terminal_observed = True
            else:
                terminal_observed = False
            if deadline is not None and now >= deadline:
                raise TimeoutError(
                    f"Task {task_id} did not reach a terminal state within {timeout:g}s"
                )
            time.sleep(max(0.05, poll_interval))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    client = LocalApiClient(args.api)
    prompt_sources = sum(
        value is not None
        for value in (args.prompt_file, args.prompt_task_id, args.prompt_transcript)
    )
    if prompt_sources > 1:
        parser.error("prompt file, task id, and transcript are mutually exclusive")
    try:
        if args.prompt_task_id:
            source_task = client.get(f"/api/tasks/{args.prompt_task_id}")
            prompt = str(source_task.get("prompt") or "")
            if not prompt.strip():
                raise ValueError(
                    f"Task {args.prompt_task_id} does not expose a replayable prompt"
                )
        elif args.prompt_transcript is not None:
            prompt = read_prompt_from_transcript(
                args.prompt_transcript,
                args.prompt_marker,
            )
        else:
            prompt = read_prompt(args.prompt_file)
    except (OSError, UnicodeError, ValueError) as exc:
        parser.error(str(exc))
    created = client.post("/api/run", build_payload(args, prompt))
    print(json.dumps(created, ensure_ascii=False, indent=2))
    task_id = str(created["task_id"])
    if not args.wait:
        return 0
    capture_path = _timeline_path(args.timeline_jsonl, args.destination, task_id)
    try:
        task, events = wait_for_task(
            client,
            task_id,
            timeline_path=capture_path,
            poll_interval=args.poll_interval,
            timeout=args.wait_timeout,
        )
    except TimeoutError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "timeline_jsonl": str(capture_path.resolve()),
                "diagnostics": summarize_diagnostics(events, task),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if str(task.get("status")) == "COMPLETED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
