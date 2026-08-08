from pathlib import Path

from orchestrator import patch_engine, safety


def test_new_file_snapshot_and_scoped_rollback(tmp_path: Path):
    target = "src/new_file.py"
    snapshot = safety.backup_commit(tmp_path, "before new file", target)
    patch_engine.apply_patch(
        tmp_path,
        target,
        "<patch>\n<<<< SEARCH\n====\nVALUE = 1\n>>>> REPLACE\n</patch>",
        frozenset({target}),
    )
    assert (tmp_path / target).exists()

    safety.rollback_to(tmp_path, snapshot, target)

    assert not (tmp_path / target).exists()


def test_gate_status_does_not_call_skipped_checks_passed(tmp_path: Path):
    python_file = tmp_path / "app.py"
    python_file.write_text("VALUE = 1\n", encoding="utf-8")
    text_file = tmp_path / "README.md"
    text_file.write_text("# Project\n", encoding="utf-8")

    assert safety.run_syntax_gate(python_file) == ("passed", "")
    assert safety.run_syntax_gate(text_file) == ("not_applicable", "")
    assert safety.run_sandbox_tests(tmp_path, None) == ("not_configured", "")
