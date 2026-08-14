"""Run the Windows-friendly acceptance matrix and preserve exact gate evidence."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
NPM = "npm.cmd" if os.name == "nt" else "npm"
PYTHON = sys.executable
ISOLATED_PATHS = {
    "ORCHESTRATOR_DB_PATH": "orchestrator.sqlite3",
    "ORCH_ACCOUNT_LEASE_DB": "account-leases.sqlite3",
    "ORCH_TASKS_FILE": "tasks.json",
    "ORCH_ARTIFACTS_DIR": "artifacts",
    "ORCH_AGENT_LOG_DIR": "logs/agents",
    "ORCH_SNAPSHOTS_DIR": "snapshots",
    "ORCH_COOKIES_DIR": "cookies",
    "ORCHESTRATOR_PROJECT_LOCK_ROOT": "project-locks",
    "ORCH_ACCOUNT_FINGERPRINT_SALT_FILE": "secrets/account-fingerprint.salt",
}


@dataclass(frozen=True, slots=True)
class Gate:
    gate_id: str
    description: str
    command: tuple[str, ...]
    cwd: Path = ROOT
    profiles: tuple[str, ...] = ("full",)
    modules: tuple[str, ...] = ()
    executables: tuple[str, ...] = ()
    needs_frontend_install: bool = False


@dataclass(slots=True)
class GateResult:
    gate_id: str
    description: str
    status: str
    command: str
    cwd: str
    exit_code: int | None
    duration_seconds: float
    started_at: str
    finished_at: str
    log_path: str
    log_sha256: str | None
    reason: str | None = None


def _pytest(*targets: str) -> tuple[str, ...]:
    return (PYTHON, "-m", "pytest", "-q", *targets)


GATES: tuple[Gate, ...] = (
    Gate(
        "backend-lint",
        "Ruff production, scripts, and tests",
        (PYTHON, "-m", "ruff", "check", "orchestrator", "server.py", "scripts", "tests"),
        profiles=("full", "backend"),
        modules=("ruff",),
    ),
    Gate(
        "backend-types",
        "Strict types for durable coordination boundaries",
        (
            PYTHON,
            "-m",
            "mypy",
            "--strict",
            "orchestrator/models.py",
            "orchestrator/effects.py",
            "orchestrator/reconciliation.py",
        ),
        profiles=("full", "backend"),
        modules=("mypy",),
    ),
    Gate(
        "backend-full",
        "Complete offline backend regression suite",
        _pytest(),
        profiles=("full", "backend"),
        modules=("pytest",),
    ),
    Gate(
        "sqlite-migration",
        "Versioned SQLite migration, old fixtures, and transactional rollback",
        _pytest(
            "tests/test_state_repository.py::test_additive_migration_preserves_existing_rows",
            "tests/test_wave2_persistence_observability.py::test_old_schema_migrates_events_and_exposes_version",
            "tests/test_wave2_persistence_observability.py::test_failed_migration_rolls_back_schema_and_version",
            "tests/test_wave2_persistence_observability.py::test_task_manager_imports_json_once_then_uses_sqlite_snapshot",
        ),
        profiles=("full", "backend", "focused", "migration"),
        modules=("pytest",),
    ),
    Gate(
        "replay-reconciliation",
        "Durable replay cursors, resume, and terminal reconciliation",
        _pytest(
            "tests/test_event_broker.py",
            "tests/test_reconciliation.py",
            "tests/test_hierarchy.py::test_resume_skips_work_item_with_approved_evidence",
            "tests/test_wave2_persistence_observability.py::test_task_end_rejects_unfinished_call_then_persists_terminal_state",
        ),
        profiles=("full", "backend", "focused"),
        modules=("pytest",),
    ),
    Gate(
        "fanout-contracts",
        "Dynamic fan-out, dependency scheduling, and typed work contracts",
        _pytest(
            "tests/test_typed_work_contract.py",
            "tests/test_director_settings_cap.py",
            "tests/test_manager_plan_deps.py",
            "tests/test_manager_spawn_flow.py",
            "tests/test_scheduler.py",
            "tests/test_hierarchy.py::test_hierarchy_runs_dynamic_manager_workers_and_tester",
        ),
        profiles=("full", "backend", "focused"),
        modules=("pytest",),
    ),
    Gate(
        "effects-leases",
        "Effect idempotency and account/project lease fencing",
        _pytest(
            "tests/test_account_lease.py",
            "tests/test_state_repository.py::test_effect_begin_is_idempotent_across_restart",
            "tests/test_state_repository.py::test_failed_effect_retries_same_receipt_and_reconciles",
            "tests/test_state_repository.py::test_project_lease_fencing_rejects_stale_owner",
            "tests/test_ticket_executor.py",
        ),
        profiles=("full", "backend", "focused"),
        modules=("pytest",),
    ),
    Gate(
        "sandbox-security",
        "Sandbox limits, local API boundary, redaction, and safe patching",
        _pytest(
            "tests/test_sandbox.py",
            "tests/test_safety.py",
            "tests/test_patch_engine.py",
            "tests/test_redaction.py",
            "tests/test_server.py::test_local_api_requires_same_origin_session_and_csrf",
            "tests/test_wave2_persistence_observability.py::test_secret_fixture_is_detected_redacted_and_blocked",
        ),
        profiles=("full", "backend", "focused"),
        modules=("pytest",),
    ),
    Gate(
        "retention-artifacts",
        "Retention, backup restore, artifact hashes, and crash reconciliation",
        _pytest(
            "tests/test_artifact_manager.py",
            "tests/test_backup.py",
            "tests/test_wave2_persistence_observability.py::test_retention_preserves_terminal_events_records_and_cursor",
            "tests/test_wave2_persistence_observability.py::test_artifact_reconciliation_detects_missing_tampered_and_extras",
        ),
        profiles=("full", "backend", "focused", "migration"),
        modules=("pytest",),
    ),
    Gate(
        "compatibility",
        "Reviewed HTTP and event contract compatibility",
        (PYTHON, "scripts/check_compatibility.py"),
        profiles=("full", "backend", "focused"),
    ),
    Gate(
        "event-schema-drift",
        "Generated TypeScript event declarations match Python source",
        (PYTHON, "scripts/acceptance_checks.py", "event-schema"),
        profiles=("full", "backend", "frontend", "focused"),
    ),
    Gate(
        "cookie-only-provider",
        "No Anthropic Messages endpoint, API-key environment, parameter, or flag",
        (PYTHON, "scripts/acceptance_checks.py", "cookie-only-provider"),
        profiles=("full", "backend", "focused"),
    ),
    Gate(
        "provider-behavior",
        "Cookie adapter retry, replay, transcript, and complete-attempt semantics",
        _pytest(
            "tests/test_provider_adapter.py",
            "tests/test_provider_transcript.py",
            "tests/test_llm_client.py::test_provider_prompt_content_is_not_blocked_before_transport",
            "tests/test_llm_client.py::test_web_stream_classifies_authentication_error_for_cookie_rotation",
            "tests/test_llm_client.py::test_web_stream_rejects_eof_without_terminal_event",
            "tests/test_llm_client.py::test_worker_429_replays_identical_request_on_new_account",
            "tests/test_llm_client.py::test_transport_failure_rotates_cookie_and_replays_same_assignment",
            "tests/test_llm_client.py::test_auth_failure_quarantines_cookie_and_replays_with_next_account",
            "tests/test_llm_client.py::test_rate_limit_error_honors_retry_after_header",
            "tests/test_llm_client.py::test_final_failure_emits_one_terminal_event",
            "tests/test_llm_client.py::test_cancel_emits_one_aborted_terminal_event",
            "tests/test_llm_client.py::test_aborted_web_stream_never_exposes_buffered_partial_chunks",
            "tests/test_llm_client.py::test_rate_limited_partial_attempt_is_not_committed_twice",
        ),
        profiles=("full", "backend", "focused"),
        modules=("pytest",),
    ),
    Gate(
        "frontend-lint",
        "ESLint operator application",
        (NPM, "run", "lint"),
        cwd=FRONTEND,
        profiles=("full", "frontend"),
        executables=(NPM,),
        needs_frontend_install=True,
    ),
    Gate(
        "frontend-format",
        "Prettier check for quality and browser sources",
        (NPM, "run", "format:check"),
        cwd=FRONTEND,
        profiles=("full", "frontend"),
        executables=(NPM,),
        needs_frontend_install=True,
    ),
    Gate(
        "frontend-types",
        "TypeScript project references",
        (NPM, "run", "typecheck"),
        cwd=FRONTEND,
        profiles=("full", "frontend"),
        executables=(NPM,),
        needs_frontend_install=True,
    ),
    Gate(
        "frontend-tests",
        "Vitest reducer and operator component suite",
        (NPM, "test", "--", "--run"),
        cwd=FRONTEND,
        profiles=("full", "frontend"),
        executables=(NPM,),
        needs_frontend_install=True,
    ),
    Gate(
        "frontend-build",
        "Production TypeScript and Vite bundle",
        (NPM, "run", "build"),
        cwd=FRONTEND,
        profiles=("full", "frontend"),
        executables=(NPM,),
        needs_frontend_install=True,
    ),
    Gate(
        "frontend-e2e",
        "Playwright operator flow and responsive viewport checks",
        (NPM, "run", "test:e2e"),
        cwd=FRONTEND,
        profiles=("full", "frontend"),
        executables=(NPM,),
        needs_frontend_install=True,
    ),
)

SOURCE_PATTERNS = (
    ".github/**/*.yml",
    ".github/**/*.yaml",
    "docs/**/*.json",
    "docs/**/*.md",
    "frontend/e2e/**/*.ts",
    "frontend/src/**/*.ts",
    "frontend/src/**/*.tsx",
    "frontend/*.json",
    "frontend/*.ts",
    "orchestrator/**/*.py",
    "scripts/**/*.py",
    "scripts/**/*.ps1",
    "tests/**/*.py",
    "*.js",
    "*.json",
    "*.md",
    "*.py",
    "*.toml",
    "requirements*.txt",
)
SOURCE_EXCLUSIONS = frozenset()


def _command_text(command: Sequence[str]) -> str:
    return subprocess.list2cmdline(list(command))


def _console_write(output: str) -> None:
    encoding = sys.stdout.encoding or "utf-8"
    safe_output = output.encode(encoding, errors="backslashreplace").decode(encoding)
    sys.stdout.write(safe_output)
    if safe_output and not safe_output.endswith("\n"):
        sys.stdout.write("\n")


def _missing_requirement(gate: Gate) -> str | None:
    missing_modules = [name for name in gate.modules if importlib.util.find_spec(name) is None]
    if missing_modules:
        return f"missing Python module(s): {', '.join(missing_modules)}"
    missing_executables = [name for name in gate.executables if shutil.which(name) is None]
    if missing_executables:
        return f"missing executable(s): {', '.join(missing_executables)}"
    if gate.needs_frontend_install and not (FRONTEND / "node_modules").is_dir():
        return "frontend/node_modules is missing; run npm ci in frontend"
    return None


def _git_output(*args: str, cwd: Path = ROOT) -> str | None:
    try:
        result = subprocess.run(
            ("git", *args),
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return None
    return result.stdout.strip()


def _source_snapshot() -> dict[str, object]:
    status = _git_output("status", "--porcelain")
    snapshot: dict[str, object] = {
        "git_commit": _git_output("rev-parse", "HEAD"),
        "git_branch": _git_output("branch", "--show-current"),
        "working_tree_dirty": bool(status),
        "python": platform.python_version(),
        "node": _executable_version("node", "--version"),
        "npm": _executable_version(NPM, "--version"),
        "os": platform.platform(),
        "source_fingerprint_sha256": _source_fingerprint(),
    }
    nested_root = ROOT / "orchestrator"
    nested_commit = (
        _git_output("rev-parse", "HEAD", cwd=nested_root)
        if (nested_root / ".git").exists()
        else None
    )
    if nested_commit is not None:
        nested_status = _git_output("status", "--porcelain", cwd=nested_root)
        snapshot["nested_orchestrator"] = {
            "git_commit": nested_commit,
            "git_branch": _git_output("branch", "--show-current", cwd=nested_root),
            "working_tree_dirty": bool(nested_status),
            "status_sha256": hashlib.sha256((nested_status or "").encode("utf-8")).hexdigest(),
        }
    return snapshot


def _source_fingerprint() -> str:
    files: set[Path] = set()
    for pattern in SOURCE_PATTERNS:
        files.update(path for path in ROOT.glob(pattern) if path.is_file())
    digest = hashlib.sha256()
    for path in sorted(files):
        relative = path.relative_to(ROOT).as_posix()
        if relative in SOURCE_EXCLUSIONS:
            continue
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _executable_version(executable: str, argument: str) -> str | None:
    try:
        result = subprocess.run(
            (executable, argument),
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return None
    return result.stdout.strip().splitlines()[-1]


def _isolated_environment(root: Path) -> dict[str, str]:
    """Return a subprocess environment whose mutable state is disposable."""

    resolved = root.resolve()
    environment = os.environ.copy()
    for name, relative in ISOLATED_PATHS.items():
        target = resolved / relative
        if Path(relative).suffix:
            target.parent.mkdir(parents=True, exist_ok=True)
        else:
            target.mkdir(parents=True, exist_ok=True)
        environment[name] = str(target)
    return environment


def _run_gate(gate: Gate, log_path: Path, timeout: int) -> GateResult:
    started = datetime.now(timezone.utc)
    before = time.perf_counter()
    missing = _missing_requirement(gate)
    if missing:
        output = f"BLOCKED: {missing}\n"
        log_path.write_text(output, encoding="utf-8")
        _console_write(output)
        status = "blocked"
        exit_code = None
        reason = missing
    else:
        try:
            with tempfile.TemporaryDirectory(
                prefix=f"ai-orchestrator-{gate.gate_id}-"
            ) as temporary:
                completed = subprocess.run(
                    gate.command,
                    cwd=gate.cwd,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=timeout,
                    env=_isolated_environment(Path(temporary)),
                )
            output = completed.stdout + completed.stderr
            log_path.write_text(output, encoding="utf-8", newline="\n")
            _console_write(output)
            exit_code = completed.returncode
            status = "passed" if completed.returncode == 0 else "failed"
            reason = None if completed.returncode == 0 else f"exit code {completed.returncode}"
        except subprocess.TimeoutExpired as error:
            stdout = error.stdout or ""
            stderr = error.stderr or ""
            if isinstance(stdout, bytes):
                stdout = stdout.decode("utf-8", errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", errors="replace")
            output = f"{stdout}{stderr}\nTIMEOUT after {timeout} seconds\n"
            log_path.write_text(output, encoding="utf-8", newline="\n")
            _console_write(output)
            exit_code = None
            status = "failed"
            reason = f"timeout after {timeout} seconds"
    finished = datetime.now(timezone.utc)
    log_bytes = log_path.read_bytes()
    return GateResult(
        gate_id=gate.gate_id,
        description=gate.description,
        status=status,
        command=_command_text(gate.command),
        cwd=str(gate.cwd.relative_to(ROOT) or "."),
        exit_code=exit_code,
        duration_seconds=round(time.perf_counter() - before, 3),
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        log_path=str(log_path),
        log_sha256=hashlib.sha256(log_bytes).hexdigest(),
        reason=reason,
    )


def _write_markdown(
    path: Path,
    *,
    label: str,
    snapshot: dict[str, object],
    results: list[GateResult],
    started_at: str,
    finished_at: str,
    source_changed: bool,
) -> None:
    counts = {
        status: sum(result.status == status for result in results)
        for status in ("passed", "failed", "blocked")
    }
    lines = [
        f"# Acceptance run: {label}",
        "",
        f"- Started: `{started_at}`",
        f"- Finished: `{finished_at}`",
        f"- Git commit: `{snapshot.get('git_commit')}`",
        f"- Dirty working tree: `{snapshot.get('working_tree_dirty')}`",
        f"- Nested orchestrator: `{snapshot.get('nested_orchestrator')}`",
        f"- Source changed during run: `{source_changed}`",
        f"- Result: {counts['passed']} passed, {counts['failed']} failed, {counts['blocked']} blocked",
        "",
        "## Gates",
        "",
    ]
    for result in results:
        lines.extend(
            [
                f"### {result.gate_id}: {result.status.upper()}",
                "",
                f"- Command: `{result.command}`",
                f"- Exit code: `{result.exit_code}`",
                f"- Duration: `{result.duration_seconds}s`",
                f"- Log: `{result.log_path}`",
                f"- Log SHA-256: `{result.log_sha256}`",
            ]
        )
        if result.reason:
            lines.append(f"- Reason: {result.reason}")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def _select_gates(profile: str, gate_ids: list[str], skip_e2e: bool) -> list[Gate]:
    known_ids = {gate.gate_id for gate in GATES}
    unknown = sorted(set(gate_ids) - known_ids)
    if unknown:
        raise ValueError(f"unknown gate(s): {', '.join(unknown)}")
    selected = [
        gate
        for gate in GATES
        if (gate.gate_id in gate_ids if gate_ids else profile in gate.profiles)
    ]
    if skip_e2e:
        selected = [gate for gate in selected if gate.gate_id != "frontend-e2e"]
    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        choices=("full", "backend", "frontend", "focused", "migration"),
        default="full",
    )
    parser.add_argument(
        "--gate", action="append", default=[], help="run only this gate; repeatable"
    )
    parser.add_argument("--label", default="current-baseline")
    parser.add_argument("--results-dir", type=Path)
    parser.add_argument("--timeout", type=int, default=1200, help="per-gate timeout in seconds")
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--allow-blocked", action="store_true")
    parser.add_argument("--skip-e2e", action="store_true")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    if args.timeout < 1:
        parser.error("--timeout must be positive")
    try:
        selected = _select_gates(args.profile, args.gate, args.skip_e2e)
    except ValueError as error:
        parser.error(str(error))
    if args.list:
        for gate in selected:
            print(f"{gate.gate_id}: {gate.description}")
        return 0
    if not selected:
        parser.error("no gates selected")

    safe_label = re_sub_non_filename(args.label)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = args.results_dir or Path(tempfile.gettempdir()) / "ai-orchestrator-acceptance"
    run_dir = base.expanduser().resolve() / f"{timestamp}-{safe_label}"
    run_dir.mkdir(parents=True, exist_ok=False)
    snapshot = _source_snapshot()
    started_at = datetime.now(timezone.utc).isoformat()
    results: list[GateResult] = []

    print(f"Acceptance label: {args.label}")
    print(f"Results directory: {run_dir}")
    print(f"Selected gates: {len(selected)}")
    for index, gate in enumerate(selected, start=1):
        print(f"\n[{index}/{len(selected)}] {gate.gate_id}: {gate.description}")
        print(f"$ {_command_text(gate.command)}")
        result = _run_gate(gate, run_dir / f"{index:02d}-{gate.gate_id}.log", args.timeout)
        results.append(result)
        print(f"=> {result.status.upper()} ({result.duration_seconds}s)")
        if args.fail_fast and result.status != "passed":
            break

    finished_at = datetime.now(timezone.utc).isoformat()
    finished_snapshot = _source_snapshot()
    source_changed = snapshot.get("source_fingerprint_sha256") != finished_snapshot.get(
        "source_fingerprint_sha256"
    )
    report = {
        "schema_version": 1,
        "label": args.label,
        "profile": args.profile,
        "started_at": started_at,
        "finished_at": finished_at,
        "source": snapshot,
        "source_finished": finished_snapshot,
        "source_changed_during_run": source_changed,
        "results": [asdict(result) for result in results],
    }
    json_path = run_dir / "acceptance-report.json"
    markdown_path = run_dir / "acceptance-report.md"
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    _write_markdown(
        markdown_path,
        label=args.label,
        snapshot=snapshot,
        results=results,
        started_at=started_at,
        finished_at=finished_at,
        source_changed=source_changed,
    )
    failed = sum(result.status == "failed" for result in results)
    blocked = sum(result.status == "blocked" for result in results)
    passed = sum(result.status == "passed" for result in results)
    print(f"\nAcceptance result: {passed} passed, {failed} failed, {blocked} blocked")
    if source_changed:
        print("WARNING: source changed during the run; results are not release evidence")
    print(f"JSON report: {json_path}")
    print(f"Markdown report: {markdown_path}")
    if failed or source_changed or (blocked and not args.allow_blocked):
        return 1
    return 0


def re_sub_non_filename(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in "-_." else "-" for char in value)
    return cleaned.strip("-.") or "acceptance"


if __name__ == "__main__":
    raise SystemExit(main())
