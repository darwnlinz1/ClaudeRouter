# -*- coding: utf-8 -*-
from pathlib import Path

import pytest

from orchestrator import patch_engine


def _patch(search: str, replace: str) -> str:
    return (
        "<patch>\n<<<< SEARCH\n"
        f"{search}\n"
        "====\n"
        f"{replace}\n"
        ">>>> REPLACE\n</patch>"
    )


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "foo.py").write_text(
        "def a():\n    return 1\n\n\ndef b():\n    return 1\n", encoding="utf-8"
    )
    (tmp_path / "rules.json").write_text("{}", encoding="utf-8")
    (tmp_path / "state.json").write_text("{}", encoding="utf-8")
    (tmp_path / "DECISIONS.md").write_text("# DECISIONS.md\n", encoding="utf-8")
    return tmp_path


def test_rejects_protected_file_by_name(project: Path):
    with pytest.raises(patch_engine.ProtectedFileError):
        patch_engine.apply_patch(
            project, "rules.json", _patch("{}", '{"x": 1}'), frozenset({"rules.json"})
        )


def test_rejects_protected_file_even_with_path_prefix(project: Path):
    # A model trying to sneak past a naive string-equality check with a
    # relative-path variant of a protected filename must still be blocked.
    with pytest.raises(patch_engine.ProtectedFileError):
        patch_engine.apply_patch(
            project, "./state.json", _patch("{}", '{"x": 1}'), frozenset({"./state.json"})
        )


def test_rejects_case_alias_of_protected_file(project: Path):
    with pytest.raises(patch_engine.ProtectedFileError):
        patch_engine.apply_patch(
            project,
            "RULES.JSON",
            "<patch>\n<<<< SEARCH\n====\n{}\n>>>> REPLACE\n</patch>",
            frozenset({"RULES.JSON"}),
        )


def test_rejects_file_not_in_allowed_context(project: Path):
    (project / "src" / "secret.py").write_text("password = 'x'\n", encoding="utf-8")
    with pytest.raises(patch_engine.FileNotInContextError):
        patch_engine.apply_patch(
            project,
            "src/secret.py",
            _patch("password = 'x'", "password = 'y'"),
            frozenset({"src/foo.py"}),  # secret.py was never shown to the model
        )


def test_rejects_nonunique_anchor(project: Path):
    with pytest.raises(patch_engine.AnchorNotUniqueError) as exc_info:
        patch_engine.apply_patch(
            project,
            "src/foo.py",
            _patch("return 1", "return 2"),  # appears twice
            frozenset({"src/foo.py"}),
        )
    assert exc_info.value.occurrences == 2


def test_rejects_missing_anchor(project: Path):
    with pytest.raises(patch_engine.AnchorNotFoundError):
        patch_engine.apply_patch(
            project,
            "src/foo.py",
            _patch("return 999", "return 2"),
            frozenset({"src/foo.py"}),
        )


def test_successful_unique_patch_applies(project: Path):
    result = patch_engine.apply_patch(
        project,
        "src/foo.py",
        _patch("def a():\n    return 1", "def a():\n    return 42"),
        frozenset({"src/foo.py"}),
    )
    assert result.occurrences_before == 1
    new_content = (project / "src" / "foo.py").read_text(encoding="utf-8")
    assert "return 42" in new_content
    assert "def b():\n    return 1" in new_content  # untouched


def test_rejects_path_traversal_outside_root(project: Path, tmp_path: Path):
    outside = tmp_path.parent / "outside.txt"
    outside.write_text("secret\n", encoding="utf-8")
    traversal_path = "../outside.txt"
    with pytest.raises(patch_engine.FileNotInContextError):
        patch_engine.apply_patch(
            project,
            traversal_path,
            _patch("secret", "leaked"),
            frozenset({traversal_path}),  # even if "allowed", root check should catch it
        )


def test_creates_explicitly_allowed_new_file(project: Path):
    result = patch_engine.apply_patch(
        project,
        "src/new_module.py",
        "<patch>\n<<<< SEARCH\n====\nVALUE = 42\n>>>> REPLACE\n</patch>",
        frozenset({"src/new_module.py"}),
    )

    assert result.occurrences_before == 0
    assert (project / "src" / "new_module.py").read_text(encoding="utf-8") == "VALUE = 42"


def test_new_file_requires_empty_search_block(project: Path):
    with pytest.raises(patch_engine.FileMissingError):
        patch_engine.apply_patch(
            project,
            "src/new_module.py",
            _patch("invented old code", "VALUE = 42"),
            frozenset({"src/new_module.py"}),
        )
