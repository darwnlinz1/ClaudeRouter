"""Read-only SQLite preflight for orchestrator migration and rollback."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from orchestrator.state_repository import CURRENT_SCHEMA_VERSION  # noqa: E402


def _read_only_connection(path: Path) -> sqlite3.Connection:
    uri_path = quote(path.resolve().as_posix(), safe="/:")
    return sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True, timeout=5)


def inspect_database(path: Path, *, maximum_version: int | None) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    result: dict[str, Any] = {
        "path": str(resolved),
        "exists": resolved.is_file(),
        "wal_present": Path(f"{resolved}-wal").is_file(),
        "shm_present": Path(f"{resolved}-shm").is_file(),
    }
    if not resolved.is_file():
        result.update({"status": "missing", "safe_to_migrate": True})
        return result

    try:
        with _read_only_connection(resolved) as connection:
            integrity_rows = connection.execute("PRAGMA quick_check").fetchall()
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            tables = [
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
            ]
    except sqlite3.Error as error:
        result.update(
            {
                "status": "error",
                "safe_to_migrate": False,
                "error": str(error),
            }
        )
        return result

    integrity = [str(row[0]) for row in integrity_rows]
    too_new = maximum_version is not None and version > maximum_version
    healthy = integrity == ["ok"] and not too_new
    result.update(
        {
            "status": "ok" if healthy else "unsafe",
            "safe_to_migrate": healthy,
            "quick_check": integrity,
            "schema_version": version,
            "maximum_supported_version": maximum_version,
            "tables": tables,
            "size_bytes": resolved.stat().st_size,
        }
    )
    if too_new:
        result["error"] = (
            f"schema version {version} is newer than supported version {maximum_version}"
        )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        default=Path(
            os.environ.get(
                "ORCHESTRATOR_DB_PATH",
                Path.home() / ".ai_orchestrator" / "orchestrator.sqlite3",
            )
        ),
    )
    parser.add_argument(
        "--account-database",
        type=Path,
        default=Path(
            os.environ.get(
                "ORCH_ACCOUNT_LEASE_DB",
                Path.home() / ".ai_orchestrator" / "account_leases.sqlite3",
            )
        ),
    )
    parser.add_argument(
        "--maximum-schema-version",
        type=int,
        default=CURRENT_SCHEMA_VERSION,
    )
    args = parser.parse_args()
    if args.maximum_schema_version < 0:
        parser.error("--maximum-schema-version cannot be negative")

    database = inspect_database(
        args.database,
        maximum_version=args.maximum_schema_version,
    )
    account_database = inspect_database(args.account_database, maximum_version=None)
    target_parent = args.database.expanduser().resolve().parent
    disk_probe = target_parent
    while not disk_probe.exists() and disk_probe != disk_probe.parent:
        disk_probe = disk_probe.parent
    disk = shutil.disk_usage(disk_probe)
    report = {
        "read_only": True,
        "database": database,
        "account_database": account_database,
        "disk": {
            "path": str(disk_probe),
            "free_bytes": disk.free,
            "total_bytes": disk.total,
        },
        "safe_to_continue": bool(
            database["safe_to_migrate"] and account_database["safe_to_migrate"]
        ),
        "warnings": [
            warning
            for warning, present in (
                (
                    "SQLite WAL/SHM sidecars are present; use the online backup command "
                    "and do not copy database files directly.",
                    database["wal_present"] or database["shm_present"],
                ),
                (
                    "Account SQLite WAL/SHM sidecars are present; include the account "
                    "database through the online backup command.",
                    account_database["wal_present"] or account_database["shm_present"],
                ),
            )
            if present
        ],
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["safe_to_continue"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
