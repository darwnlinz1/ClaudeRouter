# -*- coding: utf-8 -*-
from pathlib import Path

import pytest

from orchestrator.file_agent import authorize_existing_file, build_project_tree
from orchestrator.worker_targets import (
    DirectoryWorkerTarget,
    UnsupportedWorkerTarget,
    classify_declared_worker_target,
    ensure_worker_target,
    expand_directory_worker_target,
    is_directory_shaped_target,
    is_runtime_only_artifact,
)


def test_build_project_tree_lists_files_and_skips_noise(tmp_path: Path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "main.py").write_text("print(1)\n", encoding="utf-8")
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_text("x", encoding="utf-8")
    (tmp_path / "node_modules").mkdir()
    (tmp_path / "node_modules" / "pkg.js").write_text("x", encoding="utf-8")

    tree = build_project_tree(tmp_path)
    assert "src/main.py" in tree
    assert ".git" not in tree
    assert "node_modules" not in tree


def test_project_tree_is_deterministic_and_bounded(tmp_path: Path):
    for name in ("c.py", "a.py", "b.py"):
        (tmp_path / name).write_text(name, encoding="utf-8")
    deep = tmp_path / "nested" / "too-deep"
    deep.mkdir(parents=True)
    (deep / "hidden.py").write_text("x", encoding="utf-8")
    (tmp_path / ("x" * 40 + ".py")).write_text("x", encoding="utf-8")

    tree = build_project_tree(
        tmp_path,
        max_entries=2,
        max_depth=1,
        max_path_length=32,
    )

    assert tree.splitlines()[:2] == ["a.py", "b.py"]
    assert "đã cắt bớt" in tree
    constrained = build_project_tree(
        tmp_path,
        max_entries=10,
        max_depth=1,
        max_path_length=32,
    )
    assert "hidden.py" not in constrained
    assert "x" * 40 not in constrained


def test_authorize_existing_file(tmp_path: Path):
    target = tmp_path / "a.py"
    target.write_text("x = 1\n", encoding="utf-8")
    rel, abs_path = authorize_existing_file(tmp_path, "a.py")
    assert rel == "a.py"
    assert abs_path == target.resolve()


def test_worker_targets_are_source_only_while_runtime_images_remain_outputs():
    assert classify_declared_worker_target("src/generate_background.py").allowed is True
    assert classify_declared_worker_target("docs/usage.md").allowed is True
    assert classify_declared_worker_target("empty/.gitkeep").allowed is True
    assert classify_declared_worker_target(".gitignore").allowed is True

    image = classify_declared_worker_target("assets/background.jpg")
    assert image.allowed is False
    assert image.runtime_only is True
    assert is_runtime_only_artifact("assets/background.jpg") is True

    assert classify_declared_worker_target("assets/logo.svg").allowed is False
    assert classify_declared_worker_target("data/state.sqlite3").allowed is False


def test_directory_fixture_target_is_not_an_unsupported_worker_target(tmp_path: Path):
    fixtures = tmp_path / "tests" / "fixtures"
    fixtures.mkdir(parents=True)
    (fixtures / "note.md").write_text("# note\n", encoding="utf-8")
    (fixtures / "sample.json").write_text("{}\n", encoding="utf-8")
    (fixtures / "image.png").write_bytes(b"\x89PNG\r\n")

    directory = classify_declared_worker_target("tests/fixtures/")
    assert directory.allowed is False
    assert directory.is_directory is True
    assert directory.runtime_only is False
    assert "not a concrete file" not in directory.reason
    assert is_directory_shaped_target("tests/fixtures/") is True
    assert is_directory_shaped_target("tests/fixtures/*") is True

    with pytest.raises(DirectoryWorkerTarget, match="directory target must be expanded"):
        ensure_worker_target("tests/fixtures/")
    with pytest.raises(DirectoryWorkerTarget):
        ensure_worker_target("tests/fixtures", absolute_path=fixtures)
    with pytest.raises(UnsupportedWorkerTarget, match="Unsupported Worker target"):
        ensure_worker_target("assets/background.jpg")

    expanded = expand_directory_worker_target(
        tmp_path,
        "tests/fixtures/",
        max_files=12,
    )
    assert expanded == ["tests/fixtures/note.md", "tests/fixtures/sample.json"]
