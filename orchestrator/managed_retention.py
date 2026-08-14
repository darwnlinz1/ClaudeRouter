"""Crash-safe filesystem deletion for repository-managed retention records."""
from __future__ import annotations

import os
import stat
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence


class ManagedRetentionRepository(Protocol):
    """The short repository operations required by managed file retention."""

    def claim_managed_retention(
        self,
        *,
        now: datetime,
        stale_before: datetime,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        """Atomically claim eligible and stale ``deleting`` records."""

    def finalize_managed_retention(
        self,
        claim: Mapping[str, Any],
        *,
        deleted: bool,
        size_bytes: int,
        error: str | None,
        finalized_at: datetime,
    ) -> None:
        """Finalize one claim in a separate, short repository transaction."""


ClaimCallback = Callable[..., Sequence[Mapping[str, Any]]]
FinalizeCallback = Callable[..., None]


@dataclass(frozen=True, slots=True)
class RetentionReport:
    """Summary of one bounded retention pass."""

    claims: int = 0
    files_deleted: int = 0
    bytes_deleted: int = 0
    finalized: int = 0
    failures: int = 0
    errors: tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, Any]:
        return {
            "claims": self.claims,
            "files_deleted": self.files_deleted,
            "bytes_deleted": self.bytes_deleted,
            "finalized": self.finalized,
            "failures": self.failures,
            "errors": list(self.errors),
        }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_protected(claim: Mapping[str, Any]) -> bool:
    metadata = claim.get("metadata")
    if not isinstance(metadata, Mapping):
        metadata = {}
    return (
        claim.get("pinned") is True
        or claim.get("terminal_evidence") is True
        or metadata.get("pinned") is True
        or metadata.get("terminal_evidence") is True
    )


def _clear_readonly(path: Path) -> None:
    """Make a regular file writable without following a replacement symlink."""

    file_stat = path.lstat()
    if stat.S_ISLNK(file_stat.st_mode):
        raise ValueError("refusing to delete a managed-path symlink")
    os.chmod(path, file_stat.st_mode | stat.S_IWRITE)


def _delete_file(path: Path, roots: Iterable[Path]) -> tuple[bool, int]:
    """Delete one contained regular file and return (existed, bytes)."""

    if not path.is_absolute():
        raise ValueError("managed retention paths must be absolute")
    lexical = Path(os.path.abspath(path))
    allowed_roots = tuple(Path(root).resolve() for root in roots)
    if not allowed_roots:
        raise ValueError("no managed root configured for retention claim")

    selected_root: Path | None = None
    for root in allowed_roots:
        try:
            lexical.relative_to(root)
        except ValueError:
            continue
        if lexical != root:
            selected_root = root
            break
    if selected_root is None:
        raise ValueError("retention path escaped its managed root")

    try:
        file_stat = lexical.lstat()
    except FileNotFoundError:
        # A resumed stale claim may have completed unlink before its process
        # could finalize the repository row.
        return False, 0
    if stat.S_ISLNK(file_stat.st_mode):
        raise ValueError("refusing to delete a managed-path symlink")
    if not stat.S_ISREG(file_stat.st_mode):
        raise ValueError("managed retention only deletes regular files")

    resolved = lexical.resolve(strict=True)
    try:
        resolved.relative_to(selected_root)
    except ValueError as exc:
        raise ValueError("retention path escaped its managed root") from exc

    size_bytes = file_stat.st_size
    if not file_stat.st_mode & stat.S_IWRITE:
        _clear_readonly(lexical)
    try:
        lexical.unlink()
    except PermissionError:
        _clear_readonly(lexical)
        lexical.unlink()
    return True, size_bytes


class ManagedRetentionService:
    """Claim in the repository, unlink outside it, then finalize the claim."""

    def __init__(
        self,
        repository: object | None,
        managed_roots: Mapping[str, Path] | Iterable[Path],
        *,
        claim_callback: ClaimCallback | None = None,
        finalize_callback: FinalizeCallback | None = None,
    ) -> None:
        self._repository = repository
        if isinstance(managed_roots, Mapping):
            self._roots_by_kind = {
                str(kind): Path(root).resolve()
                for kind, root in managed_roots.items()
            }
            self._all_roots = tuple(dict.fromkeys(self._roots_by_kind.values()))
        else:
            self._roots_by_kind = {}
            self._all_roots = tuple(
                dict.fromkeys(Path(root).resolve() for root in managed_roots)
            )
        self._claim = claim_callback or self._repository_hook(
            "claim_managed_retention"
        )
        self._finalize = finalize_callback or self._repository_hook(
            "finalize_managed_retention"
        )

    def _repository_hook(self, name: str) -> Callable[..., Any] | None:
        hook = getattr(self._repository, name, None)
        return hook if callable(hook) else None

    def _roots_for(self, claim: Mapping[str, Any]) -> tuple[Path, ...]:
        if not self._roots_by_kind:
            return self._all_roots
        kind = str(claim.get("kind") or "")
        root = self._roots_by_kind.get(kind)
        return (root,) if root is not None else ()

    def run(
        self,
        *,
        now: datetime | None = None,
        stale_after: timedelta = timedelta(minutes=15),
        limit: int = 100,
    ) -> RetentionReport:
        """Run one bounded pass, including stale ``deleting`` claim recovery."""

        if limit < 1:
            raise ValueError("limit must be positive")
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        if self._claim is None or self._finalize is None:
            return RetentionReport(
                failures=1,
                errors=("repository retention hooks are unavailable",),
            )

        current = _aware_utc(now or _utc_now())
        try:
            claimed = tuple(
                self._claim(
                    now=current,
                    stale_before=current - stale_after,
                    limit=limit,
                )
            )
        except Exception as exc:
            return RetentionReport(
                failures=1,
                errors=(f"claim failed: {exc}",),
            )

        files_deleted = 0
        bytes_deleted = 0
        finalized = 0
        failures = 0
        errors: list[str] = []
        for raw_claim in claimed:
            claim = dict(raw_claim)
            deleted = False
            deleted_bytes = 0
            error: str | None = None
            path_value = claim.get("path")
            if not isinstance(path_value, str) or not path_value.strip():
                error = "claim has no path"
            elif _is_protected(claim):
                error = "repository claimed protected retention evidence"
            else:
                try:
                    existed, deleted_bytes = _delete_file(
                        Path(path_value),
                        self._roots_for(claim),
                    )
                    deleted = True
                    if existed:
                        files_deleted += 1
                        bytes_deleted += deleted_bytes
                except (OSError, ValueError) as exc:
                    error = str(exc)

            if error is not None:
                failures += 1
                errors.append(f"{path_value!s}: {error}")
            try:
                self._finalize(
                    claim,
                    deleted=deleted,
                    size_bytes=deleted_bytes,
                    error=error,
                    finalized_at=current,
                )
                finalized += 1
            except Exception as exc:
                failures += 1
                errors.append(f"{path_value!s}: finalize failed: {exc}")

        return RetentionReport(
            claims=len(claimed),
            files_deleted=files_deleted,
            bytes_deleted=bytes_deleted,
            finalized=finalized,
            failures=failures,
            errors=tuple(errors),
        )


def run_managed_retention(
    repository: object | None,
    managed_roots: Mapping[str, Path] | Iterable[Path],
    **kwargs: Any,
) -> RetentionReport:
    """Convenience entry point for one managed retention pass."""

    return ManagedRetentionService(repository, managed_roots).run(**kwargs)
