# -*- coding: utf-8 -*-
from pathlib import Path

from orchestrator.file_agent import authorize_existing_file, build_project_tree


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
