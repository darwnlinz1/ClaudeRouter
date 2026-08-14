import os
import stat
from datetime import datetime, timedelta, timezone

from orchestrator.managed_retention import ManagedRetentionService


class FakeRepository:
    def __init__(self, claims):
        self.claims = claims
        self.claim_arguments = None
        self.finalized = []

    def claim_managed_retention(self, **arguments):
        self.claim_arguments = arguments
        return self.claims

    def finalize_managed_retention(self, claim, **outcome):
        path = claim.get("path")
        if outcome["deleted"] and path:
            assert not os.path.exists(path)
        self.finalized.append((claim, outcome))


def test_claim_delete_finalize_clears_readonly_and_resumes_stale_claims(tmp_path):
    log_root = tmp_path / "logs"
    artifact_root = tmp_path / "artifacts"
    log_root.mkdir()
    artifact_root.mkdir()
    log_path = log_root / "agent.log"
    artifact_path = artifact_root / "result.zip"
    log_path.write_bytes(b"log bytes")
    artifact_path.write_bytes(b"artifact bytes")
    expected_bytes = log_path.stat().st_size + artifact_path.stat().st_size
    os.chmod(log_path, stat.S_IREAD)
    stale_missing = log_root / "already-deleted.log"
    repository = FakeRepository(
        [
            {"claim_id": "log", "kind": "log", "path": str(log_path.resolve())},
            {
                "claim_id": "artifact",
                "kind": "artifact",
                "path": str(artifact_path.resolve()),
            },
            {
                "claim_id": "stale",
                "kind": "log",
                "path": str(stale_missing.resolve()),
                "state": "deleting",
            },
        ]
    )
    now = datetime(2026, 8, 12, tzinfo=timezone.utc)

    report = ManagedRetentionService(
        repository,
        {"log": log_root, "artifact": artifact_root},
    ).run(now=now, stale_after=timedelta(minutes=5))

    assert report.as_dict() == {
        "claims": 3,
        "files_deleted": 2,
        "bytes_deleted": expected_bytes,
        "finalized": 3,
        "failures": 0,
        "errors": [],
    }
    assert repository.claim_arguments == {
        "now": now,
        "stale_before": now - timedelta(minutes=5),
        "limit": 100,
    }
    assert len(repository.finalized) == 3
    assert all(outcome["deleted"] for _claim, outcome in repository.finalized)


def test_missing_repository_hooks_never_delete_files(tmp_path):
    managed_root = tmp_path / "managed"
    managed_root.mkdir()
    path = managed_root / "keep.log"
    path.write_text("keep", encoding="utf-8")

    report = ManagedRetentionService(object(), [managed_root]).run()

    assert path.is_file()
    assert report.files_deleted == 0
    assert report.failures == 1
    assert report.errors == ("repository retention hooks are unavailable",)


def test_protected_and_escaped_claims_are_finalized_without_deletion(tmp_path):
    managed_root = tmp_path / "managed"
    managed_root.mkdir()
    protected = managed_root / "terminal.log"
    protected.write_text("terminal", encoding="utf-8")
    escaped = tmp_path / "outside.log"
    escaped.write_text("outside", encoding="utf-8")
    repository = FakeRepository(
        [
            {
                "claim_id": "terminal",
                "kind": "log",
                "path": str(protected.resolve()),
                "terminal_evidence": True,
            },
            {
                "claim_id": "escaped",
                "kind": "log",
                "path": str(escaped.resolve()),
            },
        ]
    )

    report = ManagedRetentionService(
        repository,
        {"log": managed_root},
    ).run()

    assert protected.is_file()
    assert escaped.is_file()
    assert report.files_deleted == 0
    assert report.finalized == 2
    assert report.failures == 2
    assert all(
        outcome["deleted"] is False for _claim, outcome in repository.finalized
    )
    assert all(outcome["error"] for _claim, outcome in repository.finalized)
