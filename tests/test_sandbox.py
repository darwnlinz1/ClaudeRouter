import json
import os
import sys
import threading
import time
from pathlib import Path

import pytest

from orchestrator.sandbox import (
    STRONG_ISOLATION_CAPABILITIES,
    BackendAttestation,
    CommandPolicy,
    IsolationLevel,
    ProcessOnlyBackend,
    SandboxCapability,
    SandboxOutcome,
    SandboxPolicyError,
    SandboxRunner,
)


class _AttestedTestBackend(ProcessOnlyBackend):
    """Unit-test backend; production must supply a real OS isolation backend."""

    def attest(self, root: Path) -> BackendAttestation:
        del root
        return BackendAttestation(
            backend_name="test-strong",
            available=True,
            isolation_level=IsolationLevel.STRONG,
            capabilities=STRONG_ISOLATION_CAPABILITIES
            | frozenset({SandboxCapability.PROCESS_GROUP}),
            details="test-only strong backend attestation",
        )


def _trusted_process_runner(tmp_path: Path, **kwargs) -> SandboxRunner:
    return SandboxRunner(
        tmp_path,
        isolation_level=IsolationLevel.PROCESS_ONLY,
        trusted_internal=True,
        **kwargs,
    )


def test_strong_isolation_fails_closed_without_backend(tmp_path: Path):
    marker = tmp_path / "must-not-run"
    result = SandboxRunner(
        tmp_path,
        timeout_seconds=5,
    ).run(
        [
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({str(marker)!r}).write_text('ran')",
        ]
    )

    assert result.returncode is None
    assert result.outcome is SandboxOutcome.UNAVAILABLE
    assert result.blocked
    assert result.requested_isolation is IsolationLevel.STRONG
    assert result.actual_isolation is IsolationLevel.NONE
    assert "no attested strong-isolation backend" in (result.blocked_reason or "")
    assert not marker.exists()


def test_process_only_never_satisfies_strong_isolation(tmp_path: Path):
    result = SandboxRunner(
        tmp_path,
        isolation_level=IsolationLevel.STRONG,
        backend=ProcessOnlyBackend(),
    ).run([sys.executable, "-c", "raise SystemExit('must not run')"])

    assert result.outcome is SandboxOutcome.UNAVAILABLE
    assert result.actual_isolation is IsolationLevel.PROCESS_ONLY
    assert SandboxCapability.FILESYSTEM_CONTAINMENT not in (
        result.backend_attestation.capabilities
    )
    if os.name == "nt":
        assert "Windows process-group" in result.isolation_details


def test_process_only_mode_requires_explicit_internal_trust(tmp_path: Path):
    blocked = SandboxRunner(
        tmp_path,
        isolation_level=IsolationLevel.PROCESS_ONLY,
    ).run([sys.executable, "-c", "print('must not run')"])
    allowed = _trusted_process_runner(tmp_path).run(
        [sys.executable, "-c", "print('ok')"]
    )

    assert blocked.outcome is SandboxOutcome.BLOCKED
    assert "trusted internal" in (blocked.blocked_reason or "")
    assert allowed.outcome is SandboxOutcome.COMPLETED
    assert allowed.actual_isolation is IsolationLevel.PROCESS_ONLY
    assert allowed.stdout.strip() == "ok"


def test_command_policy_enforces_deny_and_allow_lists(tmp_path: Path):
    with pytest.raises(SandboxPolicyError, match="denied"):
        _trusted_process_runner(tmp_path).run(
            ["powershell", "-Command", "echo unsafe"]
        )

    policy = CommandPolicy(allowed_commands=frozenset({"python"}))
    with pytest.raises(SandboxPolicyError, match="allowlist"):
        _trusted_process_runner(tmp_path, policy=policy).run(["git", "--version"])


def test_cwd_must_remain_beneath_runner_root(tmp_path: Path):
    outside = tmp_path.parent
    runner = _trusted_process_runner(tmp_path)

    with pytest.raises(SandboxPolicyError, match="escapes"):
        runner.run([sys.executable, "-c", "pass"], cwd=outside)


def test_timeout_and_output_limit_are_structured(tmp_path: Path):
    timeout = _trusted_process_runner(
        tmp_path, timeout_seconds=0.2, max_output_bytes=128
    ).run(
        [
            sys.executable,
            "-c",
            "import sys,time; sys.stdout.write('started\\n'); "
            "sys.stdout.flush(); time.sleep(5)",
        ]
    )
    assert timeout.timed_out
    assert timeout.outcome is SandboxOutcome.TIMED_OUT
    assert "started" in timeout.stdout

    bounded = _trusted_process_runner(
        tmp_path, timeout_seconds=5, max_output_bytes=128
    ).run([sys.executable, "-c", "import sys; sys.stdout.write('x' * 10000)"])
    assert bounded.returncode == 0
    assert bounded.output_truncated
    assert len(bounded.stdout.encode("utf-8")) <= 128


def test_child_environment_does_not_inherit_secrets(monkeypatch, tmp_path: Path):
    secrets = {
        "GITHUB_TOKEN": "github-secret",
        "AWS_SECRET_ACCESS_KEY": "aws-secret",
        "HTTPS_PROXY": "http://proxy.invalid",
        "SSH_AUTH_SOCK": "ssh-agent",
        "GIT_ASKPASS": "credential-helper",
        "SESSION_COOKIE": "cookie-secret",
        "MODEL_API_KEY": "generic-api-key-secret",
    }
    for key, value in secrets.items():
        monkeypatch.setenv(key, value)

    result = _trusted_process_runner(tmp_path).run(
        [
            sys.executable,
            "-c",
            "import json,os; print(json.dumps(dict(os.environ), sort_keys=True))",
        ]
    )

    child_environment = json.loads(result.stdout)
    assert not secrets.keys() & child_environment.keys()
    assert Path(child_environment["HOME"]).parent.parent == tmp_path
    assert Path(child_environment["TEMP"]).parent.parent == tmp_path
    assert child_environment["HOME"] != os.environ.get("HOME")

    with pytest.raises(SandboxPolicyError, match="not allowed"):
        _trusted_process_runner(tmp_path).run(
            [sys.executable, "-c", "pass"],
            env={"GITHUB_TOKEN": "explicit-secret"},
        )


def test_cancellation_kills_process_tree_promptly(tmp_path: Path):
    marker = tmp_path / "descendant-survived"
    child_code = (
        "import time; from pathlib import Path; time.sleep(1.5); "
        f"Path({str(marker)!r}).write_text('survived')"
    )
    parent_code = (
        "import subprocess,sys,time; "
        f"subprocess.Popen([sys.executable, '-c', {child_code!r}]); "
        "print('started', flush=True); time.sleep(30)"
    )
    cancellation = threading.Event()
    timer = threading.Timer(0.2, cancellation.set)
    timer.start()
    started = time.monotonic()
    try:
        result = _trusted_process_runner(
            tmp_path,
            timeout_seconds=10,
            cancellation_poll_seconds=0.02,
        ).run(
            [sys.executable, "-c", parent_code],
            cancelled=cancellation.is_set,
        )
    finally:
        timer.cancel()

    assert result.cancelled
    assert result.outcome is SandboxOutcome.CANCELLED
    assert time.monotonic() - started < 3
    time.sleep(1.6)
    assert not marker.exists()
