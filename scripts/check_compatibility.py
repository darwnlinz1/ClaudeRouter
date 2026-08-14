"""Reject accidental breaking changes to events and HTTP route surface."""
# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from orchestrator.event_schema import EVENT_SCHEMAS, SCHEMA_VERSION

BASELINE = ROOT / "docs" / "compatibility-baseline.json"
HTTP_METHODS = {"get", "post", "put", "patch", "delete"}
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


@contextmanager
def _isolated_runtime_environment() -> Iterator[None]:
    """Keep importing the production application away from operator state."""

    previous = {name: os.environ.get(name) for name in ISOLATED_PATHS}
    with tempfile.TemporaryDirectory(
        prefix="ai-orchestrator-compatibility-",
        ignore_cleanup_errors=True,
    ) as temporary:
        root = Path(temporary)
        for name, relative in ISOLATED_PATHS.items():
            target = root / relative
            if Path(relative).suffix:
                target.parent.mkdir(parents=True, exist_ok=True)
            else:
                target.mkdir(parents=True, exist_ok=True)
            os.environ[name] = str(target)
        try:
            yield
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def current_contract() -> dict[str, Any]:
    with _isolated_runtime_environment():
        import server

        try:
            openapi = server.app.openapi()
        finally:
            account_store = getattr(server, "llm_account_leases", None)
            if account_store is not None:
                account_store.close()
            server.hierarchy_repository.close()
    routes = {
        path: sorted(HTTP_METHODS.intersection(definition))
        for path, definition in openapi["paths"].items()
    }
    events = {
        name: {
            "fields": dict(sorted(spec.fields.items())),
            "required": sorted(spec.required),
            "version": spec.version,
        }
        for name, spec in sorted(EVENT_SCHEMAS.items())
    }
    return {
        "event_schema_version": SCHEMA_VERSION,
        "events": events,
        "routes": routes,
    }


def breaking_changes(
    baseline: dict[str, Any],
    current: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    for name, previous in baseline["events"].items():
        active = current["events"].get(name)
        if active is None:
            errors.append(f"event removed: {name}")
            continue
        for field, field_type in previous["fields"].items():
            if field not in active["fields"]:
                errors.append(f"event field removed: {name}.{field}")
            elif active["fields"][field] != field_type:
                errors.append(f"event field type changed: {name}.{field}")
        added_required = set(active["required"]) - set(previous["required"])
        if added_required:
            errors.append(
                f"event gained required fields: {name}: {sorted(added_required)}"
            )
    for path, methods in baseline["routes"].items():
        active_methods = set(current["routes"].get(path, []))
        for method in methods:
            if method not in active_methods:
                errors.append(f"HTTP route removed: {method.upper()} {path}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--update",
        action="store_true",
        help="replace the reviewed compatibility baseline",
    )
    args = parser.parse_args()
    current = current_contract()
    if args.update:
        BASELINE.write_text(
            json.dumps(current, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Updated {BASELINE}")
        return 0
    baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
    errors = breaking_changes(baseline, current)
    if errors:
        print("\n".join(errors))
        return 1
    print("Compatibility gates passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
