from pathlib import Path

import pytest

from orchestrator import path_utils


def test_resolve_under_root_normalizes_separators(tmp_path: Path):
    normalized, resolved = path_utils.resolve_under_root(tmp_path, "./src\\app.py")

    assert normalized == "src/app.py"
    assert resolved == (tmp_path / "src" / "app.py").resolve()


@pytest.mark.parametrize(
    "candidate",
    ["../secret.txt", "src/../../secret.txt", "/absolute/path.txt", "C:/secret.txt"],
)
def test_resolve_under_root_rejects_escape(tmp_path: Path, candidate: str):
    with pytest.raises(path_utils.PathEscapeError):
        path_utils.resolve_under_root(tmp_path, candidate)


@pytest.mark.parametrize(
    "candidate",
    [
        ".env",
        ".env.production",
        "config/credentials.json",
        "certs/server.pem",
        "cookies/user.txt",
        ".ssh/id_rsa.pub",
    ],
)
def test_sensitive_files_are_rejected_from_llm_context(candidate: str):
    with pytest.raises(path_utils.SensitivePathError):
        path_utils.ensure_context_path_safe(candidate)


def test_env_example_is_allowed_in_llm_context():
    assert path_utils.ensure_context_path_safe(".env.example") == ".env.example"


def test_resolve_under_root_rejects_symlink_component(tmp_path: Path):
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    link_dir = tmp_path / "linked"
    try:
        link_dir.symlink_to(real_dir, target_is_directory=True)
    except OSError:
        pytest.skip("Symlink creation is unavailable on this Windows account")

    with pytest.raises(path_utils.PathEscapeError, match="symlink/junction"):
        path_utils.resolve_under_root(tmp_path, "linked/file.py")
