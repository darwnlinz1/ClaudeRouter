"""Run one prompt in a disposable project with a hard process timeout."""
# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DEFAULT_PROMPT = "Change the fixture message from before to after."
DEFAULT_TIMEOUT_SECONDS = 30.0


def _milliseconds(started: float) -> float:
    return round((time.perf_counter() - started) * 1_000, 3)


def _deterministic_provider(counter: list[int]):
    from orchestrator.llm_client import ToolCallResult

    def call(
        _system_prompt: str,
        _user_message: str,
        tools: list[dict[str, Any]],
    ) -> ToolCallResult:
        counter[0] += 1
        names = [str(tool["name"]) for tool in tools]
        if "delegate_task" in names:
            return ToolCallResult(
                tool_name="delegate_task",
                tool_input={
                    "file_path": "src/app.py",
                    "instructions": "Replace the deterministic fixture value.",
                    "is_final_ticket": True,
                    "context_note": "Single disposable smoke ticket.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            )
        if names == ["submit_patch"]:
            return ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "task_status": "completed",
                    "worker_feedback": "Updated the deterministic smoke fixture.",
                },
                raw_response={
                    "content": (
                        "<patch>\n"
                        "<<<< SEARCH\n"
                        '    return "before"\n'
                        "====\n"
                        '    return "after"\n'
                        ">>>> REPLACE\n"
                        "</patch>"
                    )
                },
            )
        if names == ["review_patch"]:
            return ToolCallResult(
                tool_name="review_patch",
                tool_input={
                    "verdict": "approved",
                    "reviewer_feedback": "Deterministic fixture change verified.",
                    "next_instructions": "",
                },
                raw_response={},
            )
        raise RuntimeError(f"unexpected smoke provider tools: {names}")

    return call


def _run_worker(
    *,
    workspace: Path,
    result_path: Path,
    prompt: str,
    use_real_cookies: bool,
    worker_delay_seconds: float,
) -> int:
    if worker_delay_seconds:
        time.sleep(worker_delay_seconds)

    from orchestrator.orchestrator import run_session

    source = workspace / "src" / "app.py"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(
        'def message():\n    return "before"\n',
        encoding="utf-8",
        newline="\n",
    )
    provider_calls = [0]
    llm_call = None if use_real_cookies else _deterministic_provider(provider_calls)
    started = time.perf_counter()
    try:
        kwargs: dict[str, Any] = {
            "root": workspace,
            "task_description": prompt,
            "source_files": ["src/app.py"],
            "max_turns": 1,
        }
        if llm_call is not None:
            kwargs["llm_call"] = llm_call
        result = run_session(**kwargs)
        duration_ms = _milliseconds(started)
        final_text = source.read_text(encoding="utf-8")
        changed = final_text == 'def message():\n    return "after"\n'
        invariants = {
            "one_prompt_submitted": True,
            "task_completed": result.stopped_reason == "task_completed",
            "one_turn_completed": len(result.turns) == 1,
            "fixture_changed": changed,
            "workspace_is_disposable": True,
            "cookies_require_explicit_flag": not use_real_cookies,
        }
        # Real-provider runs intentionally do not assert a specific response body.
        if use_real_cookies:
            invariants["fixture_changed"] = result.stopped_reason == "task_completed"
            invariants["cookies_require_explicit_flag"] = True
        report = {
            "worker_passed": all(invariants.values()),
            "provider_mode": (
                "real_cookie_provider" if use_real_cookies else "deterministic_fake"
            ),
            "metrics": {
                "duration_ms": duration_ms,
                "prompt_count": 1,
                "provider_call_count": (
                    None if use_real_cookies else provider_calls[0]
                ),
                "turn_count": len(result.turns),
                "final_file_bytes": len(final_text.encode("utf-8")),
            },
            "invariants": invariants,
            "stopped_reason": result.stopped_reason,
        }
    except BaseException as error:
        report = {
            "worker_passed": False,
            "provider_mode": (
                "real_cookie_provider" if use_real_cookies else "deterministic_fake"
            ),
            "metrics": {
                "duration_ms": _milliseconds(started),
                "prompt_count": 1,
                "provider_call_count": (
                    None if use_real_cookies else provider_calls[0]
                ),
            },
            "invariants": {
                "one_prompt_submitted": True,
                "task_completed": False,
                "workspace_is_disposable": True,
                "cookies_require_explicit_flag": True,
            },
            "error": f"{type(error).__name__}: {error}",
        }
    result_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return int(not report["worker_passed"])


def run_smoke(
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    prompt: str = DEFAULT_PROMPT,
    use_real_cookies: bool = False,
    worker_delay_seconds: float = 0.0,
) -> dict[str, Any]:
    """Run the smoke worker out of process so the timeout is enforceable."""

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if worker_delay_seconds < 0:
        raise ValueError("worker_delay_seconds must be non-negative")
    if not prompt.strip():
        raise ValueError("prompt must not be empty")

    disposable = Path(tempfile.mkdtemp(prefix="orchestrator-one-prompt-smoke-"))
    workspace = disposable / "project"
    result_path = disposable / "worker-result.json"
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--_worker",
        "--_workspace",
        str(workspace),
        "--_result-path",
        str(result_path),
        "--prompt",
        prompt,
        "--_worker-delay",
        str(worker_delay_seconds),
    ]
    if use_real_cookies:
        command.append("--use-real-cookies")
    environment = os.environ.copy()
    if not use_real_cookies:
        # The fake mode cannot accidentally discover the operator's cookie files.
        environment["ORCH_COOKIES_DIR"] = str(disposable / "disabled-cookies")

    started = time.perf_counter()
    timed_out = False
    exit_code: int | None = None
    worker_report: dict[str, Any] = {}
    stderr = ""
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
        )
        exit_code = completed.returncode
        stderr = completed.stderr.strip()
        if result_path.is_file():
            worker_report = json.loads(result_path.read_text(encoding="utf-8"))
    except subprocess.TimeoutExpired:
        timed_out = True
    duration_ms = _milliseconds(started)
    shutil.rmtree(disposable, ignore_errors=True)
    workspace_removed = not disposable.exists()

    worker_metrics = dict(worker_report.get("metrics") or {})
    metrics = {
        **worker_metrics,
        "wall_duration_ms": duration_ms,
        "timeout_seconds": timeout_seconds,
        "worker_exit_code": exit_code,
        "real_cookies_enabled": use_real_cookies,
    }
    invariants = {
        **dict(worker_report.get("invariants") or {}),
        "hard_timeout_not_exceeded": not timed_out,
        "worker_exited_successfully": exit_code == 0,
        "workspace_removed": workspace_removed,
    }
    passed = bool(
        worker_report.get("worker_passed")
        and not timed_out
        and exit_code == 0
        and workspace_removed
        and duration_ms <= timeout_seconds * 1_000 + 250
    )
    report: dict[str, Any] = {
        "schema_version": 1,
        "check_id": "one_prompt_smoke",
        "provider_mode": (
            "real_cookie_provider" if use_real_cookies else "deterministic_fake"
        ),
        "passed": passed,
        "timed_out": timed_out,
        "thresholds": {"hard_timeout_seconds": timeout_seconds},
        "metrics": metrics,
        "invariants": invariants,
    }
    if worker_report.get("error"):
        report["error"] = worker_report["error"]
    elif stderr and exit_code:
        report["error"] = stderr[-2_000:]
    return report


def _write_report(report: dict[str, Any], output: Path | None) -> None:
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8", newline="\n")
    print(rendered, end="")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--prompt", default=DEFAULT_PROMPT)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--use-real-cookies",
        action="store_true",
        help="explicitly permit the production cookie provider instead of the fake",
    )
    parser.add_argument("--_worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--_workspace", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--_result-path", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--_worker-delay", type=float, default=0.0, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args._worker:
        if args._workspace is None or args._result_path is None:
            parser.error("internal worker requires workspace and result path")
        return _run_worker(
            workspace=args._workspace,
            result_path=args._result_path,
            prompt=args.prompt,
            use_real_cookies=args.use_real_cookies,
            worker_delay_seconds=args._worker_delay,
        )
    try:
        report = run_smoke(
            timeout_seconds=args.timeout_seconds,
            prompt=args.prompt,
            use_real_cookies=args.use_real_cookies,
        )
    except ValueError as error:
        parser.error(str(error))
    _write_report(report, args.output)
    return int(not report["passed"])


if __name__ == "__main__":
    raise SystemExit(main())
