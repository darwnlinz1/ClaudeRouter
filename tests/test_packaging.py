from __future__ import annotations

import hashlib
import stat
import tomllib
import zipfile
from pathlib import Path

import pytest

from scripts import build_release


def _write_bundle(root: Path, *, script: str = "./assets/app.js") -> Path:
    assets = root / "assets"
    assets.mkdir(parents=True)
    (assets / "app.js").write_text("console.log('app');\n", encoding="utf-8")
    (assets / "app.css").write_text("body { color: white; }\n", encoding="utf-8")
    (root / "favicon.svg").write_text("<svg></svg>\n", encoding="utf-8")
    (root / "index.html").write_text(
        "\n".join(
            [
                "<!doctype html>",
                '<html><head><link rel="icon" href="./favicon.svg">',
                '<link rel="stylesheet" href="./assets/app.css"></head>',
                '<body><div id="root"></div>',
                f'<script type="module" src="{script}"></script>',
                "</body></html>",
            ]
        ),
        encoding="utf-8",
    )
    return root


def _write_wheel(
    wheel: Path,
    frontend: Path,
    *,
    extra: tuple[str, ...] = (),
    omit: tuple[str, ...] = (),
) -> None:
    members = {
        "server.py": "app = object()\n",
        "orchestrator/__init__.py": '__version__ = "test"\n',
    }
    members.update(
        {
            build_release.FRONTEND_PREFIX + path.relative_to(frontend).as_posix():
                path.read_text(encoding="utf-8")
            for path in frontend.rglob("*")
            if path.is_file()
        }
    )
    for name in omit:
        members.pop(name)
    for name in extra:
        members[name] = "legacy\n"
    with zipfile.ZipFile(wheel, "w") as archive:
        for name, content in sorted(members.items()):
            archive.writestr(name, content)


def test_validates_relative_react_bundle(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path / "dist")

    files = build_release.validate_frontend_bundle(bundle)

    assert files == {
        "assets/app.css",
        "assets/app.js",
        "favicon.svg",
        "index.html",
    }


@pytest.mark.parametrize(
    "script",
    [
        "https://cdn.example.test/app.js",
        "//cdn.example.test/app.js",
        "/assets/app.js",
        "../assets/app.js",
        "%2e%2e/assets/app.js",
        "assets/../assets/app.js",
        r"assets\app.js",
    ],
)
def test_rejects_non_package_index_assets(tmp_path: Path, script: str) -> None:
    bundle = _write_bundle(tmp_path / "dist", script=script)

    with pytest.raises(build_release.ReleaseValidationError):
        build_release.validate_frontend_bundle(bundle)


def test_rejects_missing_assets_and_source_maps(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path / "dist")
    (bundle / "assets" / "app.js").unlink()
    with pytest.raises(build_release.ReleaseValidationError, match="missing"):
        build_release.validate_frontend_bundle(bundle)

    (bundle / "assets" / "app.js").write_text("app\n", encoding="utf-8")
    (bundle / "assets" / "app.js.map").write_text("{}\n", encoding="utf-8")
    with pytest.raises(build_release.ReleaseValidationError, match="source map"):
        build_release.validate_frontend_bundle(bundle)


def test_rejects_symlinked_frontend_asset(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path / "dist")
    target = bundle / "assets" / "actual.js"
    target.write_text("app\n", encoding="utf-8")
    linked = bundle / "assets" / "app.js"
    linked.unlink()
    try:
        linked.symlink_to(target)
    except OSError:
        pytest.skip("symlinks are not available")

    with pytest.raises(build_release.ReleaseValidationError, match="link"):
        build_release.validate_frontend_bundle(bundle)


def test_validates_complete_backend_and_frontend_wheel(tmp_path: Path) -> None:
    frontend = _write_bundle(tmp_path / "dist")
    wheel = tmp_path / "release.whl"
    _write_wheel(wheel, frontend)

    build_release.validate_wheel(
        wheel,
        backend_members={"server.py", "orchestrator/__init__.py"},
        frontend_root=frontend,
    )


@pytest.mark.parametrize(
    "member",
    [
        "orchestrator/llm_client2.py",
        "orchestrator/cache/session.json",
        "orchestrator/state/task.json",
        "orchestrator/__pycache__/cli.pyc",
        "orchestrator/frontend_dist/assets/app.js.map",
        "api.js",
        "main.js",
    ],
)
def test_rejects_legacy_wheel_members(tmp_path: Path, member: str) -> None:
    frontend = _write_bundle(tmp_path / "dist")
    wheel = tmp_path / "release.whl"
    _write_wheel(wheel, frontend, extra=(member,))

    with pytest.raises(build_release.ReleaseValidationError, match="forbidden"):
        build_release.validate_wheel(
            wheel,
            backend_members={"server.py", "orchestrator/__init__.py"},
            frontend_root=frontend,
        )


def test_rejects_incomplete_backend_wheel(tmp_path: Path) -> None:
    frontend = _write_bundle(tmp_path / "dist")
    wheel = tmp_path / "release.whl"
    _write_wheel(wheel, frontend, omit=("server.py",))

    with pytest.raises(build_release.ReleaseValidationError, match="backend modules"):
        build_release.validate_wheel(
            wheel,
            backend_members={"server.py", "orchestrator/__init__.py"},
            frontend_root=frontend,
        )


def test_rejects_symlink_in_wheel(tmp_path: Path) -> None:
    frontend = _write_bundle(tmp_path / "dist")
    wheel = tmp_path / "release.whl"
    _write_wheel(wheel, frontend)
    symlink = zipfile.ZipInfo("orchestrator/frontend_dist/assets/link.js")
    symlink.create_system = 3
    symlink.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(wheel, "a") as archive:
        archive.writestr(symlink, "app.js")

    with pytest.raises(build_release.ReleaseValidationError, match="symlink"):
        build_release.validate_wheel(
            wheel,
            backend_members={"server.py", "orchestrator/__init__.py"},
            frontend_root=frontend,
        )


def test_source_date_epoch_controls_copied_timestamps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frontend = _write_bundle(tmp_path / "dist")
    packaged = tmp_path / "package" / "frontend_dist"
    monkeypatch.setenv("SOURCE_DATE_EPOCH", "1700000000")

    epoch = build_release.source_date_epoch()
    build_release.copy_frontend_bundle(frontend, packaged, epoch=epoch)

    assert epoch == 1700000000
    assert {
        int(path.stat().st_mtime)
        for path in packaged.rglob("*")
    } == {1700000000}


def test_rejects_invalid_source_date_epoch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOURCE_DATE_EPOCH", "not-a-timestamp")
    with pytest.raises(build_release.ReleaseValidationError, match="integer"):
        build_release.source_date_epoch()

    monkeypatch.setenv("SOURCE_DATE_EPOCH", "-1")
    with pytest.raises(build_release.ReleaseValidationError, match="negative"):
        build_release.source_date_epoch()


def test_clean_release_removes_stale_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = [tmp_path / "build", tmp_path / "dist", tmp_path / "frontend_dist"]
    for output in outputs:
        output.mkdir()
        (output / "stale.txt").write_text("stale\n", encoding="utf-8")
    monkeypatch.setattr(build_release, "BUILD", outputs[0])
    monkeypatch.setattr(build_release, "DIST", outputs[1])
    monkeypatch.setattr(build_release, "PACKAGED_FRONTEND", outputs[2])

    build_release.clean_release_directories()

    assert all(not output.exists() for output in outputs)


def test_production_frontend_configuration_has_no_source_maps() -> None:
    config = (build_release.FRONTEND / "vite.config.ts").read_text(encoding="utf-8")
    index = (build_release.FRONTEND / "index.html").read_text(encoding="utf-8")

    assert "sourcemap: false" in config
    assert 'src="./src/main.tsx"' in index
    assert "favicon.ico" not in index


def test_setuptools_packages_only_backend_and_compiled_frontend() -> None:
    with (build_release.ROOT / "pyproject.toml").open("rb") as source:
        config = tomllib.load(source)

    setuptools = config["tool"]["setuptools"]
    assert setuptools["include-package-data"] is False
    assert setuptools["packages"]["find"] == {
        "include": ["orchestrator"],
        "namespaces": False,
    }
    assert setuptools["package-data"]["orchestrator"] == [
        "frontend_dist/*",
        "frontend_dist/assets/*",
    ]
    assert "*.map" in setuptools["exclude-package-data"]["*"]


def test_checksum_file_uses_reproducible_newline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheel = tmp_path / "release.whl"
    wheel.write_bytes(b"wheel")
    monkeypatch.setattr(build_release, "DIST", tmp_path)

    build_release._write_checksum(wheel)

    checksum = (tmp_path / "SHA256SUMS").read_bytes()
    assert checksum.endswith(b"\n")
    assert b"\r\n" not in checksum


def test_canonical_wheel_order_and_metadata_are_reproducible(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.whl"
    second = tmp_path / "second.whl"
    with zipfile.ZipFile(first, "w") as archive:
        archive.writestr("b.py", "b\n")
        archive.writestr("a.py", "a\n")
    with zipfile.ZipFile(second, "w") as archive:
        archive.writestr("a.py", "a\n")
        archive.writestr("b.py", "b\n")

    for wheel in (first, second):
        build_release.canonicalize_wheel(wheel, epoch=1700000000)

    assert hashlib.sha256(first.read_bytes()).digest() == hashlib.sha256(
        second.read_bytes()
    ).digest()
    with zipfile.ZipFile(first) as archive:
        assert archive.namelist() == ["a.py", "b.py"]
        assert {info.date_time for info in archive.infolist()} == {
            (2023, 11, 14, 22, 13, 20)
        }
