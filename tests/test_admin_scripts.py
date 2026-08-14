from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run_script(*arguments: str, environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (sys.executable, *arguments),
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )


def test_migrate_command_succeeds_without_touching_live_home(tmp_path: Path) -> None:
    live_home = tmp_path / "home"
    live_state = live_home / ".ai_orchestrator"
    live_state.mkdir(parents=True)
    live_database = live_state / "orchestrator.sqlite3"
    live_account_database = live_state / "account_leases.sqlite3"
    live_tasks = live_state / "tasks.json"
    live_database.write_bytes(b"live database must remain untouched")
    live_account_database.write_bytes(b"live leases must remain untouched")
    live_tasks.write_text('{"live": true}\n', encoding="utf-8")
    before = {
        path.name: path.read_bytes()
        for path in (live_database, live_account_database, live_tasks)
    }

    isolated = tmp_path / "isolated"
    database = isolated / "orchestrator.sqlite3"
    account_database = isolated / "account-leases.sqlite3"
    artifacts = isolated / "artifacts"
    logs = isolated / "logs"
    environment = os.environ.copy()
    environment.update(
        {
            "HOME": str(live_home),
            "USERPROFILE": str(live_home),
            "ORCHESTRATOR_DB_PATH": str(database),
            "ORCH_ACCOUNT_LEASE_DB": str(account_database),
            "ORCH_TASKS_FILE": str(isolated / "tasks.json"),
            "ORCH_ARTIFACTS_DIR": str(artifacts),
            "ORCH_AGENT_LOG_DIR": str(logs / "agents"),
            "ORCH_SNAPSHOTS_DIR": str(isolated / "snapshots"),
            "ORCH_COOKIES_DIR": str(isolated / "cookies"),
            "ORCHESTRATOR_PROJECT_LOCK_ROOT": str(isolated / "project-locks"),
            "ORCH_ACCOUNT_FINGERPRINT_SALT_FILE": str(
                isolated / "secrets" / "account-fingerprint.salt"
            ),
        }
    )

    migrated = _run_script(
        "scripts/orchestrator_admin.py",
        "--database",
        str(database),
        "--account-database",
        str(account_database),
        "--artifacts-root",
        str(artifacts),
        "--logs-root",
        str(logs),
        "migrate",
        environment=environment,
    )

    assert migrated.returncode == 0, migrated.stdout + migrated.stderr
    result = json.loads(migrated.stdout)
    assert isinstance(result["schema_version"], int)
    assert result["schema_version"] == result["migrations"][-1]["version"]
    assert database.is_file()

    preflight = _run_script(
        "scripts/migration_preflight.py",
        "--database",
        str(database),
        "--account-database",
        str(account_database),
        environment=environment,
    )
    assert preflight.returncode == 0, preflight.stdout + preflight.stderr
    preflight_result = json.loads(preflight.stdout)
    assert preflight_result["safe_to_continue"] is True
    assert (
        preflight_result["database"]["maximum_supported_version"]
        == result["schema_version"]
    )
    assert {
        path.name: path.read_bytes()
        for path in (live_database, live_account_database, live_tasks)
    } == before
