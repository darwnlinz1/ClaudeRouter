import hashlib
import os
import stat
from pathlib import Path

import pytest

from orchestrator import artifact_manager, project_workspace


def test_finalize_new_project_applies_files_and_builds_zip(monkeypatch, tmp_path):
    monkeypatch.setattr(artifact_manager, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    destination = tmp_path / "new-project"
    staging = artifact_manager.create_workspace(
        "task-new",
        destination,
        auto_apply=True,
        create_zip=True,
    )
    (staging / "src").mkdir()
    (staging / "src" / "main.py").write_text("print('hello')\n", encoding="utf-8")
    (staging / "README.md").write_text("# New project\n", encoding="utf-8")
    (staging / "state.json").write_text('{"internal": true}', encoding="utf-8")
    (staging / ".git").mkdir()
    (staging / ".git" / "config").write_text("internal", encoding="utf-8")

    progress = artifact_manager.materialize_approved_file(
        "task-new", "src/main.py"
    )

    assert progress["status"] == "partially_applied"
    progress_file = next(
        item for item in progress["files"] if item["path"] == "src/main.py"
    )
    assert progress_file["before_sha256"] is None
    assert progress_file["after_sha256"] == hashlib.sha256(
        (staging / "src" / "main.py").read_bytes()
    ).hexdigest()
    assert (destination / "src" / "main.py").read_text(
        encoding="utf-8"
    ) == "print('hello')\n"
    assert not (destination / "README.md").exists()

    manifest = artifact_manager.finalize_workspace("task-new")

    assert manifest["status"] == "applied"
    assert (destination / "src" / "main.py").exists()
    assert (destination / "README.md").exists()
    assert not (destination / "state.json").exists()
    assert not (destination / ".git").exists()
    assert artifact_manager.get_zip_path("task-new").is_file()
    assert {item["path"] for item in manifest["files"]} == {
        "README.md",
        "src/main.py",
    }


def test_new_project_rejects_nonempty_destination(monkeypatch, tmp_path):
    monkeypatch.setattr(artifact_manager, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    destination = tmp_path / "existing"
    destination.mkdir()
    (destination / "keep.txt").write_text("do not overwrite", encoding="utf-8")

    with pytest.raises(ValueError, match="trống"):
        artifact_manager.create_workspace(
            "task-blocked",
            destination,
            auto_apply=True,
            create_zip=True,
        )


def test_materialization_replace_failure_is_atomic(monkeypatch, tmp_path):
    monkeypatch.setattr(artifact_manager, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    destination = tmp_path / "destination"
    staging = artifact_manager.create_workspace(
        "task-crash",
        destination,
        auto_apply=True,
        create_zip=False,
    )
    source = staging / "src" / "main.py"
    source.parent.mkdir()
    source.write_text("VALUE = 1\n", encoding="utf-8")
    target = destination / "src" / "main.py"
    real_replace = project_workspace.os.replace

    def crash_before_replace(temp_path, destination_path):
        if Path(destination_path) == target:
            raise OSError("injected artifact crash")
        return real_replace(temp_path, destination_path)

    monkeypatch.setattr(project_workspace.os, "replace", crash_before_replace)

    with pytest.raises(OSError, match="injected artifact crash"):
        artifact_manager.materialize_approved_file(
            "task-crash", "src/main.py"
        )

    assert not target.exists()
    assert list(target.parent.glob(".*.orchestrator-tmp")) == []
    assert artifact_manager.get_manifest("task-crash")["files"] == []


def test_finalize_reconciles_matching_file_after_manifest_crash(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(artifact_manager, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    destination = tmp_path / "destination"
    staging = artifact_manager.create_workspace(
        "task-reconcile",
        destination,
        auto_apply=True,
        create_zip=False,
    )
    (staging / "one.txt").write_text("one\n", encoding="utf-8")
    (staging / "two.txt").write_text("two\n", encoding="utf-8")
    real_copy = artifact_manager._copy_file_atomic
    copied = []

    def crash_after_copy(target_root, relative, source, **kwargs):
        real_copy(target_root, relative, source, **kwargs)
        copied.append(relative.as_posix())
        raise OSError("crash after durable replace")

    monkeypatch.setattr(
        artifact_manager, "_copy_file_atomic", crash_after_copy
    )
    with pytest.raises(OSError, match="crash after durable replace"):
        artifact_manager.finalize_workspace("task-reconcile")

    assert len(copied) == 1
    assert (destination / copied[0]).is_file()
    monkeypatch.setattr(artifact_manager, "_copy_file_atomic", real_copy)

    manifest = artifact_manager.finalize_workspace("task-reconcile")

    assert manifest["status"] == "applied"
    assert (destination / "one.txt").is_file()
    assert (destination / "two.txt").is_file()


def test_artifact_file_download_and_retention_pin(monkeypatch, tmp_path):
    monkeypatch.setattr(artifact_manager, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    staging = artifact_manager.create_workspace(
        "task-managed",
        tmp_path / "destination",
        auto_apply=False,
        create_zip=False,
    )
    (staging / "result.txt").write_text("result\n", encoding="utf-8")
    artifact_manager.finalize_workspace("task-managed")

    assert artifact_manager.get_artifact_file(
        "task-managed",
        "result.txt",
    ).read_text(encoding="utf-8") == "result\n"
    pinned = artifact_manager.set_retention("task-managed", pinned=True)
    assert pinned["retention"]["pinned"] is True
    with pytest.raises(PermissionError, match="pinned"):
        artifact_manager.delete_artifacts("task-managed")
    assert artifact_manager.delete_artifacts("task-managed", force=True) is True
    assert artifact_manager.get_manifest("task-managed") is None


@pytest.mark.parametrize(
    ("task_id", "content"),
    [
        ("task-oversized", b"A" * (2 * 1024 * 1024 + 1)),
        ("task-binary", b"binary\x00AKIAIOSFODNN7EXAMPLE"),
    ],
    ids=["oversized", "binary"],
)
def test_unscannable_artifacts_are_rejected(
    monkeypatch, tmp_path, task_id, content
):
    monkeypatch.setattr(artifact_manager, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    staging = artifact_manager.create_workspace(
        task_id,
        tmp_path / task_id,
        auto_apply=False,
        create_zip=False,
    )
    (staging / "candidate.bin").write_bytes(content)

    with pytest.raises(ValueError, match="potential credentials"):
        artifact_manager.finalize_workspace(task_id)


def test_delete_artifacts_clears_readonly_git_files(monkeypatch, tmp_path):
    monkeypatch.setattr(artifact_manager, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    staging = artifact_manager.create_workspace(
        "task-readonly",
        tmp_path / "destination",
        auto_apply=False,
        create_zip=False,
    )
    git_dir = staging / ".git"
    git_dir.mkdir()
    config = git_dir / "config"
    config.write_text("readonly\n", encoding="utf-8")
    os.chmod(config, stat.S_IREAD)
    os.chmod(git_dir, stat.S_IREAD | stat.S_IEXEC)

    assert artifact_manager.delete_artifacts("task-readonly") is True
    assert not (tmp_path / "artifacts" / "task-readonly").exists()


def test_owned_destination_hash_blocks_external_overwrite(monkeypatch, tmp_path):
    monkeypatch.setattr(artifact_manager, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    destination = tmp_path / "destination"
    staging = artifact_manager.create_workspace(
        "task-owned",
        destination,
        auto_apply=True,
        create_zip=False,
    )
    source = staging / "result.txt"
    source.write_text("owned\n", encoding="utf-8")
    artifact_manager.materialize_approved_file("task-owned", "result.txt")
    target = destination / "result.txt"
    target.write_text("external\n", encoding="utf-8")

    with pytest.raises(ValueError, match="file ngoài task"):
        artifact_manager.finalize_workspace("task-owned")

    assert target.read_text(encoding="utf-8") == "external\n"


def test_every_managed_artifact_path_is_registered_and_pin_is_updated(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(artifact_manager, "ARTIFACTS_ROOT", tmp_path / "artifacts")

    class FakeRepository:
        def __init__(self):
            self.records = []

        def record_artifact(self, task_id, path, content_sha256, **metadata):
            self.records.append((task_id, path, content_sha256, metadata))
            return str(len(self.records))

    repository = FakeRepository()
    callback = repository.record_artifact
    staging = artifact_manager.create_workspace(
        "task-registered",
        tmp_path / "destination",
        auto_apply=False,
        create_zip=True,
        registration_callback=callback,
    )
    (staging / "result.txt").write_text("result\n", encoding="utf-8")
    artifact_manager.finalize_workspace(
        "task-registered",
        registration_callback=callback,
    )
    artifact_manager.set_retention(
        "task-registered",
        pinned=True,
        registration_callback=callback,
    )

    task_root = tmp_path / "artifacts" / "task-registered"
    disk_paths = {
        str(path.resolve())
        for path in task_root.rglob("*")
        if path.is_file()
    }
    registered_paths = {record[1] for record in repository.records}
    assert disk_paths == registered_paths
    latest = {}
    for record in repository.records:
        latest[record[1]] = record
    assert all(record[0] == "task-registered" for record in latest.values())
    assert all(record[2] for record in latest.values())
    assert all(record[3]["metadata"]["pinned"] is True for record in latest.values())
    assert all(record[3]["terminal_evidence"] is True for record in latest.values())
    approved = {
        Path(path).name
        for _task_id, path, _digest, metadata in latest.values()
        if metadata["approved"]
    }
    assert approved == {"project.zip", "result.txt"}
