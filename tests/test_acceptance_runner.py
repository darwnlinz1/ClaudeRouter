from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from scripts import run_acceptance

ROOT = Path(__file__).resolve().parents[1]
MUTABLE_ENVIRONMENT = tuple(run_acceptance.ISOLATED_PATHS)


def _tree_snapshot(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _seed_live_home(root: Path) -> None:
    state = root / ".ai_orchestrator"
    for directory in ("artifacts", "logs", "snapshots", "cookies"):
        target = state / directory
        target.mkdir(parents=True, exist_ok=True)
        (target / "keep.txt").write_text(f"live {directory}\n", encoding="utf-8")
    (state / "orchestrator.sqlite3").write_bytes(b"live database must not be opened")
    (state / "account_leases.sqlite3").write_bytes(b"live leases must not be opened")
    (state / "tasks.json").write_text('{"live": true}\n', encoding="utf-8")


def test_gate_server_import_uses_disposable_state_and_preserves_home(
    monkeypatch,
    tmp_path: Path,
) -> None:
    live_home = tmp_path / "home"
    _seed_live_home(live_home)
    before = _tree_snapshot(live_home)
    monkeypatch.setenv("HOME", str(live_home))
    monkeypatch.setenv("USERPROFILE", str(live_home))
    for name in MUTABLE_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)

    report_path = tmp_path / "gate-environment.json"
    probe_path = tmp_path / "probe_gate_environment.py"
    probe_path.write_text(
        """
import json
import os
import sys
from pathlib import Path

names = json.loads(sys.argv[2])
sys.path.insert(0, sys.argv[3])
import server

paths = {name: os.environ[name] for name in names}
directory_names = {
    "ORCH_ARTIFACTS_DIR",
    "ORCH_AGENT_LOG_DIR",
    "ORCH_SNAPSHOTS_DIR",
    "ORCH_COOKIES_DIR",
    "ORCHESTRATOR_PROJECT_LOCK_ROOT",
}
report = {
    "paths": paths,
    "common_root": os.path.commonpath(paths.values()),
    "directories_exist": all(Path(paths[name]).is_dir() for name in directory_names),
    "database_exists": Path(paths["ORCHESTRATOR_DB_PATH"]).is_file(),
}
server.hierarchy_repository.close()
if server.llm_account_leases is not None:
    server.llm_account_leases.close()
Path(sys.argv[1]).write_text(json.dumps(report), encoding="utf-8")
""".lstrip(),
        encoding="utf-8",
    )
    gate = run_acceptance.Gate(
        "isolation-probe",
        "Import server with disposable state",
        (
            sys.executable,
            str(probe_path),
            str(report_path),
            json.dumps(MUTABLE_ENVIRONMENT),
            str(ROOT),
        ),
    )

    result = run_acceptance._run_gate(gate, tmp_path / "gate.log", timeout=60)

    assert result.status == "passed"
    assert _tree_snapshot(live_home) == before
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert set(report["paths"]) == set(MUTABLE_ENVIRONMENT)
    assert report["directories_exist"] is True
    assert report["database_exists"] is True
    assert not Path(report["common_root"]).exists()
    for value in report["paths"].values():
        assert not Path(value).is_relative_to(live_home)


def test_compatibility_cli_preserves_home_and_uses_react_routes(
    tmp_path: Path,
) -> None:
    live_home = tmp_path / "home"
    _seed_live_home(live_home)
    before = _tree_snapshot(live_home)
    environment = os.environ.copy()
    environment["HOME"] = str(live_home)
    environment["USERPROFILE"] = str(live_home)
    for name in MUTABLE_ENVIRONMENT:
        environment.pop(name, None)

    completed = subprocess.run(
        (sys.executable, "scripts/check_compatibility.py"),
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "Compatibility gates passed" in completed.stdout
    assert _tree_snapshot(live_home) == before

    baseline = json.loads(
        (ROOT / "docs" / "compatibility-baseline.json").read_text(encoding="utf-8")
    )
    routes = baseline["routes"]
    assert routes["/assets/{asset_path}"] == ["get"]
    for removed in ("/style.css", "/main.js", "/api.js", "/legacy", "/task/{task_id}"):
        assert removed not in routes
