from pathlib import Path

import pytest

from orchestrator import artifact_manager


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
