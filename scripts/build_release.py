"""Build and validate a self-contained wheel with the compiled React dashboard."""

from __future__ import annotations

import hashlib
import os
import shutil
import stat
import subprocess
import sys
import time
import zipfile
from collections.abc import Callable, Iterator
from html.parser import HTMLParser
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "build"
DIST = ROOT / "dist"
FRONTEND = ROOT / "frontend"
FRONTEND_DIST = FRONTEND / "dist"
PACKAGED_FRONTEND = ROOT / "orchestrator" / "frontend_dist"
NPM = "npm.cmd" if os.name == "nt" else "npm"

FRONTEND_PREFIX = "orchestrator/frontend_dist/"
MINIMUM_ZIP_EPOCH = 315532800  # 1980-01-01, the earliest ZIP timestamp.
FORBIDDEN_RUNTIME_NAMES = frozenset(
    {
        "__pycache__",
        ".cache",
        "cache",
        "cache.json",
        "state",
        "state.json",
    }
)
FORBIDDEN_LEGACY_FILES = frozenset(
    {
        "api.js",
        "index.html",
        "llm_client2.py",
        "main.js",
    }
)
FRONTEND_SOURCE_SUFFIXES = frozenset({".jsx", ".ts", ".tsx"})


class ReleaseValidationError(RuntimeError):
    """Raised when a release input or wheel violates packaging policy."""


class _IndexParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.assets: list[str] = []
        self.module_scripts: list[str] = []
        self.has_root = False

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        attributes = {name.lower(): value for name, value in attrs}
        tag = tag.lower()
        if tag == "base":
            raise ReleaseValidationError("index.html must not contain a base URL")
        if tag == "div" and attributes.get("id") == "root":
            self.has_root = True

        asset: str | None = None
        if tag == "link":
            asset = attributes.get("href")
        elif tag in {
            "audio",
            "embed",
            "iframe",
            "img",
            "input",
            "script",
            "source",
            "track",
            "video",
        }:
            asset = attributes.get("src")
        elif tag == "object":
            asset = attributes.get("data")

        if asset is not None:
            self.assets.append(asset)
            if tag == "script" and (attributes.get("type") or "").lower() == "module":
                self.module_scripts.append(asset)

        for value in (
            attributes.get("srcset"),
            attributes.get("imagesrcset"),
        ):
            if value:
                self.assets.extend(
                    candidate.strip().split(maxsplit=1)[0]
                    for candidate in value.split(",")
                    if candidate.strip()
                )


def run(*command: str, cwd: Path = ROOT) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def _is_symlink_or_reparse(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return True
    return bool(
        path.is_symlink()
        or int(getattr(metadata, "st_file_attributes", 0))
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _remove_generated_path(path: Path) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    if _is_symlink_or_reparse(path):
        if path.is_dir():
            path.rmdir()
        else:
            path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def clean_release_directories() -> None:
    """Remove outputs so stale files cannot enter or select a release."""

    for path in (BUILD, DIST, PACKAGED_FRONTEND):
        _remove_generated_path(path)


def source_date_epoch() -> int | None:
    raw = os.environ.get("SOURCE_DATE_EPOCH")
    if raw is None:
        return None
    try:
        epoch = int(raw)
    except ValueError as error:
        raise ReleaseValidationError("SOURCE_DATE_EPOCH must be an integer") from error
    if epoch < 0:
        raise ReleaseValidationError("SOURCE_DATE_EPOCH must not be negative")
    return max(epoch, MINIMUM_ZIP_EPOCH)


def _walk_files(root: Path) -> Iterator[Path]:
    for child in sorted(root.iterdir(), key=lambda path: path.name):
        if _is_symlink_or_reparse(child):
            raise ReleaseValidationError(f"frontend bundle contains a link: {child}")
        if child.is_dir():
            yield from _walk_files(child)
        elif child.is_file():
            yield child
        else:
            raise ReleaseValidationError(f"frontend bundle contains a special file: {child}")


def _decode_relative_asset(raw: str) -> PurePosixPath:
    value = raw.strip()
    if not value:
        raise ReleaseValidationError("index.html contains an empty asset URL")
    if "\\" in value:
        raise ReleaseValidationError(f"index asset uses a backslash: {raw!r}")

    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc or value.startswith("//"):
        raise ReleaseValidationError(f"index asset must be local and relative: {raw!r}")
    if not parsed.path or parsed.path.startswith("/"):
        raise ReleaseValidationError(f"index asset must have a relative path: {raw!r}")

    decoded = parsed.path
    for _ in range(3):
        next_value = unquote(decoded)
        if next_value == decoded:
            break
        decoded = next_value
    while decoded.startswith("./"):
        decoded = decoded[2:]
    if "\\" in decoded or decoded.startswith("/"):
        raise ReleaseValidationError(f"index asset escapes its package: {raw!r}")
    segments = decoded.split("/")
    if any(segment in {"", ".", ".."} for segment in segments):
        raise ReleaseValidationError(f"index asset contains traversal: {raw!r}")
    if ":" in segments[0]:
        raise ReleaseValidationError(f"index asset is not a relative URL: {raw!r}")
    return PurePosixPath(*segments)


def _parse_index(index: str) -> tuple[set[PurePosixPath], set[PurePosixPath]]:
    parser = _IndexParser()
    try:
        parser.feed(index)
        parser.close()
    except ReleaseValidationError:
        raise
    except Exception as error:
        raise ReleaseValidationError("index.html is not valid HTML") from error
    if not parser.has_root:
        raise ReleaseValidationError("index.html does not contain the React root")

    assets = {_decode_relative_asset(asset) for asset in parser.assets}
    module_scripts = {
        _decode_relative_asset(asset) for asset in parser.module_scripts
    }
    if not module_scripts:
        raise ReleaseValidationError("index.html does not load a relative module script")
    if not any(path.suffix == ".js" for path in module_scripts):
        raise ReleaseValidationError("compiled index.html does not load JavaScript")
    return assets, module_scripts


def _validate_asset_files(
    assets: set[PurePosixPath],
    *,
    exists: Callable[[PurePosixPath], bool],
) -> None:
    for asset in sorted(assets, key=str):
        if not exists(asset):
            raise ReleaseValidationError(f"index asset is missing from the bundle: {asset}")


def validate_frontend_bundle(bundle_root: Path = FRONTEND_DIST) -> set[str]:
    """Validate a compiled Vite bundle and return its file names."""

    if not bundle_root.is_dir() or _is_symlink_or_reparse(bundle_root):
        raise ReleaseValidationError(
            f"compiled frontend must be a real directory: {bundle_root}"
        )
    resolved_root = bundle_root.resolve(strict=True)
    files: set[str] = set()
    for path in _walk_files(bundle_root):
        relative = path.relative_to(bundle_root).as_posix()
        suffix = path.suffix.lower()
        if suffix == ".map":
            raise ReleaseValidationError(f"production source map is forbidden: {relative}")
        if suffix in FRONTEND_SOURCE_SUFFIXES:
            raise ReleaseValidationError(f"frontend source file is forbidden: {relative}")
        files.add(relative)

    if "index.html" not in files:
        raise ReleaseValidationError("compiled frontend does not contain index.html")
    index = (bundle_root / "index.html").read_text(encoding="utf-8")
    assets, _ = _parse_index(index)

    def asset_exists(asset: PurePosixPath) -> bool:
        path = bundle_root.joinpath(*asset.parts)
        current = bundle_root
        for part in asset.parts:
            current /= part
            if _is_symlink_or_reparse(current):
                return False
        try:
            path.resolve(strict=True).relative_to(resolved_root)
        except (OSError, ValueError):
            return False
        return path.is_file()

    _validate_asset_files(assets, exists=asset_exists)
    return files


def _normalize_tree_mtime(root: Path, epoch: int) -> None:
    files = list(_walk_files(root))
    directories = sorted(
        (path for path in (root, *root.rglob("*")) if path.is_dir()),
        key=lambda path: path.as_posix(),
        reverse=True,
    )
    for path in [*files, *directories]:
        try:
            os.utime(path, (epoch, epoch), follow_symlinks=False)
        except NotImplementedError:
            # Windows lacks follow_symlinks=False for utime; the tree was link-checked.
            os.utime(path, (epoch, epoch))


def copy_frontend_bundle(
    source: Path = FRONTEND_DIST,
    destination: Path = PACKAGED_FRONTEND,
    *,
    epoch: int | None = None,
) -> None:
    """Copy only a validated, anchored Vite output into the Python package."""

    if source != FRONTEND_DIST:
        # Tests may pass an isolated source; production must use the anchored Vite output.
        source = source.resolve(strict=True)
    else:
        expected = FRONTEND / "dist"
        if source != expected:
            raise ReleaseValidationError("frontend source is not the anchored Vite dist")
    validate_frontend_bundle(source)
    _remove_generated_path(destination)
    destination.mkdir(parents=True)
    for path in _walk_files(source):
        relative = path.relative_to(source)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
        target.chmod(0o644)
    validate_frontend_bundle(destination)
    if epoch is not None:
        _normalize_tree_mtime(destination, epoch)


def _forbidden_wheel_member(path: PurePosixPath) -> bool:
    parts = path.parts
    if any(part in FORBIDDEN_RUNTIME_NAMES for part in parts):
        return True
    if path.name == "llm_client2.py" or path.suffix.lower() in {".map", ".pyc"}:
        return True
    if len(parts) == 1 and path.name in FORBIDDEN_LEGACY_FILES:
        return True
    if path.as_posix().startswith(FRONTEND_PREFIX):
        return (
            path.suffix.lower() in FRONTEND_SOURCE_SUFFIXES
            or "node_modules" in parts
            or "src" in parts
        )
    return False


def required_backend_members(source_root: Path = ROOT) -> set[str]:
    """Return importable backend sources which every wheel must contain."""

    package_root = source_root / "orchestrator"
    required = {"server.py", "orchestrator/__init__.py"}
    if not (source_root / "server.py").is_file() or not (
        package_root / "__init__.py"
    ).is_file():
        raise ReleaseValidationError("required backend entry points are missing")

    for path in sorted(package_root.rglob("*.py"), key=lambda item: item.as_posix()):
        relative = path.relative_to(source_root)
        member = PurePosixPath(*relative.parts)
        if _forbidden_wheel_member(member):
            raise ReleaseValidationError(f"legacy backend source is forbidden: {member}")
        if any(
            not (parent / "__init__.py").is_file()
            for parent in path.parents
            if parent != package_root and package_root in parent.parents
        ):
            continue
        required.add(member.as_posix())
    return required


def _validate_wheel_member_name(name: str) -> PurePosixPath:
    if not name or "\\" in name or name.startswith("/"):
        raise ReleaseValidationError(f"wheel contains an unsafe member: {name!r}")
    path = PurePosixPath(name.rstrip("/"))
    if not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ReleaseValidationError(f"wheel contains an unsafe member: {name!r}")
    if ":" in path.parts[0]:
        raise ReleaseValidationError(f"wheel contains an unsafe member: {name!r}")
    return path


def validate_wheel(
    wheel: Path,
    *,
    backend_members: set[str] | None = None,
    frontend_root: Path = PACKAGED_FRONTEND,
) -> None:
    """Reject incomplete, unsafe, stale, or non-production wheel contents."""

    expected_backend = (
        required_backend_members() if backend_members is None else backend_members
    )
    expected_frontend = {
        FRONTEND_PREFIX + relative for relative in validate_frontend_bundle(frontend_root)
    }

    with zipfile.ZipFile(wheel) as bundle:
        infos = bundle.infolist()
        raw_names = [info.filename for info in infos]
        if len(raw_names) != len(set(raw_names)):
            raise ReleaseValidationError("wheel contains duplicate members")

        files: set[str] = set()
        for info in infos:
            member = _validate_wheel_member_name(info.filename)
            mode = (info.external_attr >> 16) & 0o170000
            if mode == stat.S_IFLNK:
                raise ReleaseValidationError(f"wheel contains a symlink: {member}")
            if _forbidden_wheel_member(member):
                raise ReleaseValidationError(f"wheel contains a forbidden file: {member}")
            if not info.is_dir():
                files.add(member.as_posix())

        missing_backend = sorted(expected_backend - files)
        if missing_backend:
            raise ReleaseValidationError(
                f"wheel is missing backend modules: {missing_backend}"
            )

        actual_frontend = {
            name for name in files if name.startswith(FRONTEND_PREFIX)
        }
        if actual_frontend != expected_frontend:
            missing = sorted(expected_frontend - actual_frontend)
            unexpected = sorted(actual_frontend - expected_frontend)
            raise ReleaseValidationError(
                f"wheel frontend does not match Vite dist; "
                f"missing={missing}, unexpected={unexpected}"
            )

        index_member = FRONTEND_PREFIX + "index.html"
        try:
            index = bundle.read(index_member).decode("utf-8")
        except (KeyError, UnicodeDecodeError) as error:
            raise ReleaseValidationError(
                "wheel does not contain a valid compiled dashboard"
            ) from error
        assets, _ = _parse_index(index)
        _validate_asset_files(
            assets,
            exists=lambda asset: FRONTEND_PREFIX + asset.as_posix() in files,
        )


def canonicalize_wheel(wheel: Path, *, epoch: int | None) -> None:
    """Rewrite ZIP metadata and ordering deterministically."""

    with zipfile.ZipFile(wheel) as source:
        entries = [
            (info.filename, info.is_dir(), source.read(info))
            for info in source.infolist()
        ]
    entries.sort(key=lambda entry: entry[0])
    timestamp = time.gmtime(epoch or MINIMUM_ZIP_EPOCH)[:6]
    temporary = wheel.with_name(f".{wheel.name}.canonical")
    try:
        with zipfile.ZipFile(
            temporary,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
        ) as destination:
            for name, is_directory, content in entries:
                info = zipfile.ZipInfo(name, date_time=timestamp)
                info.create_system = 3
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = (
                    ((stat.S_IFDIR | 0o755) << 16) | 0x10
                    if is_directory
                    else (stat.S_IFREG | 0o644) << 16
                )
                destination.writestr(info, content, compresslevel=9)
        os.replace(temporary, wheel)
    finally:
        temporary.unlink(missing_ok=True)


def _write_checksum(wheel: Path) -> None:
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    (DIST / "SHA256SUMS").write_bytes(f"{digest}  {wheel.name}\n".encode("ascii"))


def main() -> int:
    clean_release_directories()
    epoch = source_date_epoch()
    required_backend_members()

    if os.environ.get("CI") or not (FRONTEND / "node_modules").is_dir():
        run(NPM, "ci", cwd=FRONTEND)
    run(NPM, "run", "build", cwd=FRONTEND)

    try:
        copy_frontend_bundle(epoch=epoch)
        run(sys.executable, "-m", "build", "--wheel")
        wheels = sorted(DIST.glob("*.whl"), key=lambda path: path.name)
        if len(wheels) != 1:
            raise ReleaseValidationError(
                f"wheel build must produce exactly one artifact, found {len(wheels)}"
            )
        wheel = wheels[0]
        canonicalize_wheel(wheel, epoch=epoch)
        try:
            validate_wheel(wheel)
        except Exception:
            wheel.unlink(missing_ok=True)
            raise
        _write_checksum(wheel)
    finally:
        _remove_generated_path(PACKAGED_FRONTEND)

    print(wheel)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
