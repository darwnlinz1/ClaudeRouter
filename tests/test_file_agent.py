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


def test_authorize_existing_file(tmp_path: Path):
    target = tmp_path / "a.py"
    target.write_text("x = 1\n", encoding="utf-8")
    rel, abs_path = authorize_existing_file(tmp_path, "a.py")
    assert rel == "a.py"
    assert abs_path == target.resolve()
