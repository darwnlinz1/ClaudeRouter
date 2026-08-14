import hashlib
import json
import sqlite3
import stat
import zipfile
from pathlib import Path

import pytest

from orchestrator import backup
from orchestrator.backup import create_backup, restore_backup, verify_backup
from orchestrator.state_repository import StateRepository


def test_backup_round_trip_preserves_state_and_operator_files(tmp_path):
    database = tmp_path / "state.sqlite3"
    account_database = tmp_path / "accounts.sqlite3"
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    with StateRepository(database) as repository:
        repository.save_task_snapshot(
            "task-backup",
            {"id": "task-backup", "status": "COMPLETED"},
        )
    with sqlite3.connect(account_database) as connection:
        connection.execute("CREATE TABLE health (account TEXT PRIMARY KEY)")
        connection.execute("INSERT INTO health VALUES ('account-a')")
    (artifacts / "task-backup").mkdir(parents=True)
    (artifacts / "task-backup" / "result.txt").write_text("result", encoding="utf-8")
    logs.mkdir()
    (logs / "events.jsonl").write_text('{"type":"done"}\n', encoding="utf-8")

    archive = create_backup(
        tmp_path / "backup.zip",
        database=database,
        account_database=account_database,
        artifacts_root=artifacts,
        logs_root=logs,
    )
    manifest = verify_backup(archive)
    restored = tmp_path / "restored"
    restore_backup(
        archive,
        database=restored / "state.sqlite3",
        account_database=restored / "accounts.sqlite3",
        artifacts_root=restored / "artifacts",
        logs_root=restored / "logs",
    )

    assert manifest["format_version"] == 1
    with StateRepository(restored / "state.sqlite3") as repository:
        assert repository.get_task_snapshot("task-backup")["status"] == "COMPLETED"
    with sqlite3.connect(restored / "accounts.sqlite3") as connection:
        assert connection.execute("SELECT account FROM health").fetchone() == ("account-a",)
    assert (
        restored / "artifacts" / "task-backup" / "result.txt"
    ).read_text(encoding="utf-8") == "result"
    assert (restored / "logs" / "events.jsonl").is_file()


def test_restore_refuses_to_overwrite_existing_state(tmp_path):
    database = tmp_path / "state.sqlite3"
    with StateRepository(database):
        pass
    archive = create_backup(tmp_path / "backup.zip", database=database)

    with pytest.raises(FileExistsError):
        restore_backup(archive, database=database)


def test_verify_rejects_archive_traversal(tmp_path):
    archive = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr(
            "manifest.json",
            json.dumps({"format_version": 1, "files": []}),
        )
        bundle.writestr("../escape.txt", "unsafe")

    with pytest.raises(ValueError, match="unsafe archive path"):
        verify_backup(archive)


def test_verify_rejects_member_not_listed_in_manifest(tmp_path):
    database = tmp_path / "state.sqlite3"
    with StateRepository(database):
        pass
    archive = create_backup(tmp_path / "backup.zip", database=database)
    with zipfile.ZipFile(archive, "a") as bundle:
        bundle.writestr("artifacts/unlisted.txt", "not in manifest")

    with pytest.raises(ValueError, match="member set does not match manifest"):
        verify_backup(archive)


def test_verify_rejects_zip_bomb_compression_ratio(monkeypatch, tmp_path):
    archive = tmp_path / "bomb.zip"
    payload = b"A" * (1024 * 1024)
    manifest = {
        "format_version": 1,
        "files": [
            {
                "path": "artifacts/payload.bin",
                "size_bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        ],
    }
    with zipfile.ZipFile(
        archive, "w", compression=zipfile.ZIP_DEFLATED
    ) as bundle:
        bundle.writestr("manifest.json", json.dumps(manifest))
        bundle.writestr("artifacts/payload.bin", payload)
    monkeypatch.setattr(backup, "MAX_BACKUP_COMPRESSION_RATIO", 10.0)

    with pytest.raises(ValueError, match="compression ratio"):
        verify_backup(archive)


def test_verify_enforces_member_and_size_limits(monkeypatch, tmp_path):
    database = tmp_path / "state.sqlite3"
    with StateRepository(database):
        pass
    archive = create_backup(tmp_path / "backup.zip", database=database)

    monkeypatch.setattr(backup, "MAX_BACKUP_MEMBER_COUNT", 1)
    with pytest.raises(ValueError, match="member count"):
        verify_backup(archive)

    monkeypatch.setattr(backup, "MAX_BACKUP_MEMBER_COUNT", 100_000)
    monkeypatch.setattr(backup, "MAX_BACKUP_EXPANDED_BYTES", 1)
    with pytest.raises(ValueError, match="expanded size"):
        verify_backup(archive)

    monkeypatch.setattr(backup, "MAX_BACKUP_EXPANDED_BYTES", 32 * 1024**3)
    monkeypatch.setattr(backup, "MAX_BACKUP_COMPRESSED_BYTES", 1)
    with pytest.raises(ValueError, match="compressed size"):
        verify_backup(archive)


def test_backup_manifest_uses_staged_database_schema_version(tmp_path):
    database = tmp_path / "state.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA user_version = 7")

    manifest = verify_backup(
        create_backup(tmp_path / "backup.zip", database=database)
    )

    assert manifest["schema_version"] == 7


def test_backup_records_and_restore_replaces_empty_roots(tmp_path):
    database = tmp_path / "state.sqlite3"
    with StateRepository(database):
        pass
    artifacts = tmp_path / "source-artifacts"
    logs = tmp_path / "source-logs"
    artifacts.mkdir()
    logs.mkdir()
    archive = create_backup(
        tmp_path / "backup.zip",
        database=database,
        artifacts_root=artifacts,
        logs_root=logs,
    )

    destination = tmp_path / "destination"
    restored_artifacts = destination / "artifacts"
    restored_logs = destination / "logs"
    restored_artifacts.mkdir(parents=True)
    restored_logs.mkdir()
    old_artifact = restored_artifacts / "old.txt"
    old_log = restored_logs / "old.log"
    old_artifact.write_text("old artifact", encoding="utf-8")
    old_log.write_text("old log", encoding="utf-8")
    old_artifact.chmod(stat.S_IREAD)
    old_log.chmod(stat.S_IREAD)

    manifest = restore_backup(
        archive,
        database=destination / "state.sqlite3",
        artifacts_root=restored_artifacts,
        logs_root=restored_logs,
        overwrite=True,
    )

    assert manifest["included_roots"] == ["artifacts", "logs"]
    assert restored_artifacts.is_dir()
    assert restored_logs.is_dir()
    assert list(restored_artifacts.iterdir()) == []
    assert list(restored_logs.iterdir()) == []


def test_create_backup_rejects_destination_and_stage_root_overlap(
    monkeypatch,
    tmp_path,
):
    database = tmp_path / "state.sqlite3"
    with StateRepository(database):
        pass
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()

    with pytest.raises(ValueError, match="backup destination overlaps"):
        create_backup(
            artifacts / "backup.zip",
            database=database,
            artifacts_root=artifacts,
        )

    class OverlappingStage:
        def __enter__(self):
            return str(artifacts / "stage")

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        backup.tempfile,
        "TemporaryDirectory",
        lambda **_kwargs: OverlappingStage(),
    )
    with pytest.raises(ValueError, match="backup staging directory overlaps"):
        create_backup(
            tmp_path / "outside.zip",
            database=database,
            artifacts_root=artifacts,
        )


def test_restore_rolls_back_every_target_when_later_swap_fails(
    monkeypatch,
    tmp_path,
):
    source = tmp_path / "source"
    source.mkdir()
    source_database = source / "state.sqlite3"
    with StateRepository(source_database):
        pass
    source_account = source / "accounts.sqlite3"
    with sqlite3.connect(source_account) as connection:
        connection.execute("CREATE TABLE marker (value TEXT)")
        connection.execute("INSERT INTO marker VALUES ('new account')")
    source_artifacts = source / "artifacts"
    source_logs = source / "logs"
    source_artifacts.mkdir()
    source_logs.mkdir()
    (source_artifacts / "version.txt").write_text("new artifact", encoding="utf-8")
    (source_logs / "version.txt").write_text("new log", encoding="utf-8")
    archive = create_backup(
        tmp_path / "backup.zip",
        database=source_database,
        account_database=source_account,
        artifacts_root=source_artifacts,
        logs_root=source_logs,
    )

    destination = tmp_path / "destination"
    destination.mkdir()
    restored_database = destination / "state.sqlite3"
    restored_account = destination / "accounts.sqlite3"
    restored_artifacts = destination / "artifacts"
    restored_logs = destination / "logs"
    restored_database.write_bytes(b"old state")
    restored_account.write_bytes(b"old account")
    restored_artifacts.mkdir()
    restored_logs.mkdir()
    (restored_artifacts / "version.txt").write_text(
        "old artifact",
        encoding="utf-8",
    )
    (restored_logs / "version.txt").write_text("old log", encoding="utf-8")

    real_replace = backup.os.replace
    failure_injected = False

    def fail_late_swap(source_path, target_path):
        nonlocal failure_injected
        if (
            not failure_injected
            and Path(source_path).name == "replacement"
            and Path(target_path) == restored_logs.resolve()
        ):
            failure_injected = True
            raise OSError("injected late swap failure")
        real_replace(source_path, target_path)

    monkeypatch.setattr(backup.os, "replace", fail_late_swap)

    with pytest.raises(OSError, match="injected late swap failure"):
        restore_backup(
            archive,
            database=restored_database,
            account_database=restored_account,
            artifacts_root=restored_artifacts,
            logs_root=restored_logs,
            overwrite=True,
        )

    assert failure_injected
    assert restored_database.read_bytes() == b"old state"
    assert restored_account.read_bytes() == b"old account"
    assert (restored_artifacts / "version.txt").read_text(
        encoding="utf-8"
    ) == "old artifact"
    assert (restored_logs / "version.txt").read_text(
        encoding="utf-8"
    ) == "old log"
    assert not list(destination.glob(".*.orchestrator-restore-*"))


def test_restore_does_not_swap_any_target_when_preparation_fails(
    monkeypatch,
    tmp_path,
):
    source_database = tmp_path / "source-state.sqlite3"
    with StateRepository(source_database):
        pass
    source_artifacts = tmp_path / "source-artifacts"
    source_logs = tmp_path / "source-logs"
    source_artifacts.mkdir()
    source_logs.mkdir()
    (source_artifacts / "new.txt").write_text("new artifact", encoding="utf-8")
    (source_logs / "new.txt").write_text("new log", encoding="utf-8")
    archive = create_backup(
        tmp_path / "backup.zip",
        database=source_database,
        artifacts_root=source_artifacts,
        logs_root=source_logs,
    )

    destination = tmp_path / "destination"
    destination.mkdir()
    restored_database = destination / "state.sqlite3"
    restored_artifacts = destination / "artifacts"
    restored_logs = destination / "logs"
    restored_database.write_bytes(b"old state")
    restored_artifacts.mkdir()
    restored_logs.mkdir()
    (restored_artifacts / "old.txt").write_text("old artifact", encoding="utf-8")
    (restored_logs / "old.txt").write_text("old log", encoding="utf-8")

    real_copytree = backup.shutil.copytree
    replace_calls = []

    def fail_log_preparation(source_path, target_path, *args, **kwargs):
        if Path(source_path).name == "logs":
            raise OSError("injected preparation failure")
        return real_copytree(source_path, target_path, *args, **kwargs)

    monkeypatch.setattr(backup.shutil, "copytree", fail_log_preparation)
    real_replace = backup.os.replace

    def record_replace(source_path, target_path):
        replace_calls.append((source_path, target_path))
        return real_replace(source_path, target_path)

    monkeypatch.setattr(backup.os, "replace", record_replace)

    with pytest.raises(OSError, match="injected preparation failure"):
        restore_backup(
            archive,
            database=restored_database,
            artifacts_root=restored_artifacts,
            logs_root=restored_logs,
            overwrite=True,
        )

    assert replace_calls == []
    assert restored_database.read_bytes() == b"old state"
    assert (restored_artifacts / "old.txt").read_text(
        encoding="utf-8"
    ) == "old artifact"
    assert (restored_logs / "old.txt").read_text(encoding="utf-8") == "old log"
    assert not list(destination.glob(".*.orchestrator-restore-*"))
