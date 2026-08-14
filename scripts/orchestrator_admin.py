"""Administrative migration, backup, verification, and restore commands."""
# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from orchestrator import artifact_manager
from orchestrator.backup import create_backup, restore_backup, verify_backup
from orchestrator.sqlite_account_lease import DEFAULT_ACCOUNT_DB
from orchestrator.state_repository import DEFAULT_DB_PATH, StateRepository


def _paths(args: argparse.Namespace) -> dict[str, Path]:
    return {
        "database": Path(args.database).expanduser(),
        "account_database": Path(args.account_database).expanduser(),
        "artifacts_root": Path(args.artifacts_root).expanduser(),
        "logs_root": Path(args.logs_root).expanduser(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=os.environ.get("ORCHESTRATOR_DB_PATH", str(DEFAULT_DB_PATH)),
    )
    parser.add_argument(
        "--account-database",
        default=os.environ.get("ORCH_ACCOUNT_LEASE_DB", str(DEFAULT_ACCOUNT_DB)),
    )
    parser.add_argument(
        "--artifacts-root",
        default=str(artifact_manager.ARTIFACTS_ROOT),
    )
    parser.add_argument("--logs-root", default="logs")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate")
    commands.add_parser("status")
    backup = commands.add_parser("backup")
    backup.add_argument("archive")
    verify = commands.add_parser("verify")
    verify.add_argument("archive")
    restore = commands.add_parser("restore")
    restore.add_argument("archive")
    restore.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    paths = _paths(args)

    if args.command in {"migrate", "status"}:
        with StateRepository(paths["database"]) as repository:
            result = {
                "schema_version": repository.current_schema_version,
                "migrations": repository.migration_history(),
            }
    elif args.command == "backup":
        result = {
            "archive": str(
                create_backup(
                    args.archive,
                    **paths,
                )
            )
        }
    elif args.command == "verify":
        result = verify_backup(args.archive)
    else:
        result = restore_backup(
            args.archive,
            overwrite=args.overwrite,
            **paths,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
