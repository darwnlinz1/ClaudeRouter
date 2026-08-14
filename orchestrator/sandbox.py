"""Bounded command execution with explicit, attested isolation capabilities.

Process grouping is useful for timeout and cancellation cleanup, but it is not
an OS sandbox.  Strong isolation therefore has to be supplied by a backend
which attests filesystem, network, and process-tree containment.  In the
absence of such a backend strong requests fail closed without launching a
child process.
"""
from __future__ import annotations

import os
import re
import signal
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping, Protocol, Sequence


class IsolationLevel(str, Enum):
    NONE = "none"
    # PROCESS is retained for compatibility with persisted policy records.
    PROCESS = "process"
    PROCESS_ONLY = "process-only"
    PLATFORM_BEST_EFFORT = "platform-best-effort"
    STRONG = "strong"


class SandboxOutcome(str, Enum):
    COMPLETED = "completed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"
    UNAVAILABLE = "unavailable"


class SandboxCapability(str, Enum):
    PROCESS_GROUP = "process_group"
    PROCESS_TREE_TERMINATION = "process_tree_termination"
    FILESYSTEM_CONTAINMENT = "filesystem_containment"
    NETWORK_CONTAINMENT = "network_containment"
    ENVIRONMENT_SANITIZATION = "environment_sanitization"


STRONG_ISOLATION_CAPABILITIES = frozenset(
    {
        SandboxCapability.PROCESS_TREE_TERMINATION,
        SandboxCapability.FILESYSTEM_CONTAINMENT,
        SandboxCapability.NETWORK_CONTAINMENT,
        SandboxCapability.ENVIRONMENT_SANITIZATION,
    }
)


class SandboxPolicyError(ValueError):
    pass


class SandboxBackendUnavailable(RuntimeError):
    pass


DEFAULT_DENIED_COMMANDS = frozenset(
    {
        "bash",
        "cmd",
        "del",
        "diskpart",
        "erase",
        "format",
        "mkfs",
        "poweroff",
        "powershell",
        "pwsh",
        "reboot",
        "rm",
        "rmdir",
        "sh",
        "shutdown",
        "su",
        "sudo",
    }
)

INHERITED_ENV_ALLOWLIST = frozenset(
    {
        "COMSPEC",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "PATH",
        "PATHEXT",
        "PYTHONIOENCODING",
        "PYTHONUTF8",
        "SYSTEMDRIVE",
        "SYSTEMROOT",
        "TZ",
        "WINDIR",
    }
)

_DENIED_ENV_EXACT = frozenset(
    {
        "ALL_PROXY",
        "AWS_ACCESS_KEY_ID",
        "AWS_PROFILE",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AZURE_CLIENT_SECRET",
        "CLOUDSDK_AUTH_ACCESS_TOKEN",
        "GIT_ASKPASS",
        "GIT_CONFIG",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_SYSTEM",
        "GIT_CREDENTIAL_HELPER",
        "GIT_SSH",
        "GIT_SSH_COMMAND",
        "GH_TOKEN",
        "GITHUB_TOKEN",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "SSH_AGENT_PID",
        "SSH_ASKPASS",
        "SSH_AUTH_SOCK",
    }
)
_DENIED_ENV_PATTERN = re.compile(
    r"(?:^|_)(?:API_?KEY|AUTH|COOKIE|CREDENTIAL|PASSWORD|PRIVATE_?KEY|"
    r"PROXY|SECRET|TOKEN)(?:$|_)",
    re.IGNORECASE,
)


def _command_name(value: str) -> str:
    name = Path(value).name.casefold()
    for suffix in (".exe", ".cmd", ".bat", ".com"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


@dataclass(frozen=True)
class CommandPolicy:
    allowed_commands: frozenset[str] | None = None
    denied_commands: frozenset[str] = DEFAULT_DENIED_COMMANDS

    def validate(self, command: Sequence[str]) -> tuple[str, ...]:
        normalized = tuple(str(part) for part in command)
        if not normalized or not normalized[0].strip():
            raise SandboxPolicyError("command must include an executable")
        if any("\x00" in part for part in normalized):
            raise SandboxPolicyError("command arguments may not contain NUL")
        executable = _command_name(normalized[0])
        denied = {_command_name(item) for item in self.denied_commands}
        if executable in denied:
            raise SandboxPolicyError(f"command is denied by policy: {executable}")
        if self.allowed_commands is not None:
            allowed = {_command_name(item) for item in self.allowed_commands}
            if executable not in allowed:
                raise SandboxPolicyError(
                    f"command is not in the allowlist: {executable}"
                )
        return normalized


def _is_denied_env_name(name: str) -> bool:
    upper = name.upper()
    return (
        upper in _DENIED_ENV_EXACT
        or upper.startswith(("AWS_", "AZURE_", "CLOUDSDK_", "GIT_", "GOOGLE_", "SSH_"))
        or _DENIED_ENV_PATTERN.search(upper) is not None
    )


def _sandbox_environment(
    home: Path,
    temp: Path,
    overrides: Mapping[str, str] | None,
) -> dict[str, str]:
    """Build a minimal child environment without inheriting credentials."""

    environment = {
        key.upper(): value
        for key, value in os.environ.items()
        if key.upper() in INHERITED_ENV_ALLOWLIST and not _is_denied_env_name(key)
    }
    if overrides is not None:
        for raw_key, raw_value in overrides.items():
            key = str(raw_key)
            if (
                key.upper() not in INHERITED_ENV_ALLOWLIST
                or _is_denied_env_name(key)
            ):
                raise SandboxPolicyError(
                    f"environment variable is not allowed in sandbox: {key}"
                )
            environment[key.upper()] = str(raw_value)

    home_value = str(home)
    temp_value = str(temp)
    environment.update(
        {
            "HOME": home_value,
            "USERPROFILE": home_value,
            "APPDATA": str(home / "appdata"),
            "LOCALAPPDATA": str(home / "local-appdata"),
            "XDG_CACHE_HOME": str(home / ".cache"),
            "XDG_CONFIG_HOME": str(home / ".config"),
            "XDG_DATA_HOME": str(home / ".local" / "share"),
            "TEMP": temp_value,
            "TMP": temp_value,
            "TMPDIR": temp_value,
        }
    )
    return environment


@dataclass(frozen=True, slots=True)
class BackendAttestation:
    backend_name: str
    available: bool
    isolation_level: IsolationLevel
    capabilities: frozenset[SandboxCapability] = frozenset()
    details: str = ""

    def satisfies(self, requested: IsolationLevel) -> bool:
        if not self.available:
            return False
        if requested is IsolationLevel.STRONG:
            return (
                self.isolation_level is IsolationLevel.STRONG
                and STRONG_ISOLATION_CAPABILITIES.issubset(self.capabilities)
            )
        if requested in {
            IsolationLevel.PROCESS,
            IsolationLevel.PROCESS_ONLY,
            IsolationLevel.PLATFORM_BEST_EFFORT,
        }:
            return SandboxCapability.PROCESS_TREE_TERMINATION in self.capabilities
        return True


@dataclass(frozen=True, slots=True)
class SandboxLaunch:
    command: tuple[str, ...]
    cwd: Path
    env: Mapping[str, str]
    start_new_session: bool = False
    creationflags: int = 0


class SandboxBackend(Protocol):
    """Execution backend which prepares a launch and attests its guarantees."""

    def attest(self, root: Path) -> BackendAttestation:
        ...

    def prepare_launch(
        self,
        command: tuple[str, ...],
        cwd: Path,
        env: Mapping[str, str],
    ) -> SandboxLaunch:
        ...

    def terminate_tree(
        self,
        process: subprocess.Popen[bytes],
    ) -> None:
        ...


class ProcessOnlyBackend:
    """Process grouping for trusted internal commands; not a security sandbox."""

    def __init__(self, *, grouped: bool = True) -> None:
        self.grouped = grouped

    def attest(self, root: Path) -> BackendAttestation:
        del root
        capabilities = {SandboxCapability.ENVIRONMENT_SANITIZATION}
        if self.grouped:
            capabilities.update(
                {
                    SandboxCapability.PROCESS_GROUP,
                    SandboxCapability.PROCESS_TREE_TERMINATION,
                }
            )
        details = (
            "Windows process-group and process-tree controls; "
            "no OS/filesystem/network sandbox"
            if os.name == "nt" and self.grouped
            else "new process session and process-tree controls; "
            "no OS/filesystem/network sandbox"
            if self.grouped
            else "no process grouping or OS/filesystem/network sandbox"
        )
        return BackendAttestation(
            backend_name="process-only",
            available=True,
            isolation_level=(
                IsolationLevel.PROCESS_ONLY if self.grouped else IsolationLevel.NONE
            ),
            capabilities=frozenset(capabilities),
            details=details,
        )

    def prepare_launch(
        self,
        command: tuple[str, ...],
        cwd: Path,
        env: Mapping[str, str],
    ) -> SandboxLaunch:
        return SandboxLaunch(
            command=command,
            cwd=cwd,
            env=MappingProxyType(dict(env)),
            start_new_session=self.grouped and os.name == "posix",
            creationflags=(
                getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                if self.grouped and os.name == "nt"
                else 0
            ),
        )

    def terminate_tree(self, process: subprocess.Popen[bytes]) -> None:
        _terminate_process(process, self.grouped)


class UnavailableStrongBackend:
    """Default backend for strong requests until an OS backend is wired."""

    def attest(self, root: Path) -> BackendAttestation:
        del root
        return BackendAttestation(
            backend_name="unavailable",
            available=False,
            isolation_level=IsolationLevel.NONE,
            details="no attested strong-isolation backend is configured",
        )

    def prepare_launch(
        self,
        command: tuple[str, ...],
        cwd: Path,
        env: Mapping[str, str],
    ) -> SandboxLaunch:
        del command, cwd, env
        raise SandboxBackendUnavailable(
            "no attested strong-isolation backend is configured"
        )

    def terminate_tree(self, process: subprocess.Popen[bytes]) -> None:
        _terminate_process(process, False)


@dataclass(frozen=True)
class SandboxResult:
    command: tuple[str, ...]
    cwd: Path
    returncode: int | None
    stdout: str
    stderr: str
    timed_out: bool
    output_truncated: bool
    duration_seconds: float
    requested_isolation: IsolationLevel
    actual_isolation: IsolationLevel
    isolation_details: str
    outcome: SandboxOutcome = SandboxOutcome.COMPLETED
    backend_attestation: BackendAttestation | None = None
    cancelled: bool = False
    blocked_reason: str | None = None

    @property
    def output(self) -> str:
        return self.stdout + self.stderr

    @property
    def blocked(self) -> bool:
        return self.outcome in {
            SandboxOutcome.BLOCKED,
            SandboxOutcome.UNAVAILABLE,
        }


class _BoundedCapture:
    def __init__(self, limit: int) -> None:
        self.remaining = limit
        self.stdout: list[bytes] = []
        self.stderr: list[bytes] = []
        self.truncated = False
        self.lock = threading.Lock()

    def add(self, stream_name: str, chunk: bytes) -> None:
        with self.lock:
            accepted = chunk[: self.remaining]
            self.remaining -= len(accepted)
            if accepted:
                getattr(self, stream_name).append(accepted)
            if len(accepted) != len(chunk):
                self.truncated = True

    def text(self, stream_name: str) -> str:
        return b"".join(getattr(self, stream_name)).decode(
            "utf-8", errors="replace"
        )


def _drain(stream: object, capture: _BoundedCapture, stream_name: str) -> None:
    try:
        while True:
            chunk = stream.read(64 * 1024)  # type: ignore[attr-defined]
            if not chunk:
                return
            capture.add(stream_name, chunk)
    except (OSError, ValueError):
        return


def _terminate_process(process: subprocess.Popen[bytes], grouped: bool) -> None:
    if process.poll() is not None:
        return
    if grouped and os.name == "posix":
        try:
            kill_process_group = getattr(os, "killpg")
            kill_process_group(process.pid, getattr(signal, "SIGKILL", 9))
        except (OSError, ProcessLookupError):
            pass
    elif grouped and os.name == "nt":
        # CREATE_NEW_PROCESS_GROUP is not an OS sandbox. taskkill is merely a
        # best-effort descendant cleanup for timed-out test commands.
        try:
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            pass
    try:
        process.kill()
    except OSError:
        pass


class SandboxRunner:
    def __init__(
        self,
        root: Path,
        *,
        isolation_level: IsolationLevel | str = IsolationLevel.STRONG,
        backend: SandboxBackend | None = None,
        trusted_internal: bool = False,
        policy: CommandPolicy | None = None,
        timeout_seconds: float | None = None,
        max_output_bytes: int = 1024 * 1024,
        cancellation_poll_seconds: float = 0.05,
    ) -> None:
        self.root = Path(root).resolve(strict=True)
        if not self.root.is_dir():
            raise NotADirectoryError(self.root)
        self.requested_isolation = IsolationLevel(isolation_level)
        self.trusted_internal = bool(trusted_internal)
        if backend is None:
            backend = (
                UnavailableStrongBackend()
                if self.requested_isolation is IsolationLevel.STRONG
                else ProcessOnlyBackend(
                    grouped=self.requested_isolation is not IsolationLevel.NONE
                )
            )
        self.backend = backend
        self.backend_attestation = backend.attest(self.root)
        self.policy = policy or CommandPolicy()
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_output_bytes < 1:
            raise ValueError("max_output_bytes must be positive")
        if cancellation_poll_seconds <= 0:
            raise ValueError("cancellation_poll_seconds must be positive")
        self.timeout_seconds = timeout_seconds
        self.max_output_bytes = max_output_bytes
        self.cancellation_poll_seconds = cancellation_poll_seconds

    def _contained_cwd(self, cwd: Path | None) -> Path:
        candidate = (cwd or self.root).resolve(strict=True)
        if not candidate.is_dir():
            raise NotADirectoryError(candidate)
        if candidate != self.root and self.root not in candidate.parents:
            raise SandboxPolicyError(
                f"working directory escapes sandbox root: {candidate}"
            )
        return candidate

    def describe_isolation(self) -> tuple[IsolationLevel, str]:
        """Report the controls this runner can honestly provide."""

        return (
            self.backend_attestation.isolation_level,
            self.backend_attestation.details,
        )

    def isolation_request_satisfied(self) -> bool:
        return self.backend_attestation.satisfies(self.requested_isolation)

    def _not_executed_result(
        self,
        command: tuple[str, ...],
        cwd: Path,
        *,
        outcome: SandboxOutcome,
        reason: str,
        started: float,
        cancelled: bool = False,
    ) -> SandboxResult:
        return SandboxResult(
            command=command,
            cwd=cwd,
            returncode=None,
            stdout="",
            stderr="",
            timed_out=False,
            output_truncated=False,
            duration_seconds=time.monotonic() - started,
            requested_isolation=self.requested_isolation,
            actual_isolation=self.backend_attestation.isolation_level,
            isolation_details=self.backend_attestation.details,
            outcome=outcome,
            backend_attestation=self.backend_attestation,
            cancelled=cancelled,
            blocked_reason=reason,
        )

    def run(
        self,
        command: Sequence[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        timeout_seconds: float | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> SandboxResult:
        started = time.monotonic()
        checked = self.policy.validate(command)
        working_directory = self._contained_cwd(cwd)
        timeout = self.timeout_seconds if timeout_seconds is None else timeout_seconds
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout_seconds must be positive")
        process_only_request = self.requested_isolation in {
            IsolationLevel.NONE,
            IsolationLevel.PROCESS,
            IsolationLevel.PROCESS_ONLY,
            IsolationLevel.PLATFORM_BEST_EFFORT,
        }
        if process_only_request and not self.trusted_internal:
            return self._not_executed_result(
                checked,
                working_directory,
                outcome=SandboxOutcome.BLOCKED,
                reason=(
                    "process-only execution is restricted to trusted internal commands"
                ),
                started=started,
            )
        if not self.isolation_request_satisfied():
            return self._not_executed_result(
                checked,
                working_directory,
                outcome=SandboxOutcome.UNAVAILABLE,
                reason=(
                    "requested isolation is unavailable: "
                    f"{self.backend_attestation.details}"
                ),
                started=started,
            )
        if cancelled is not None and cancelled():
            return self._not_executed_result(
                checked,
                working_directory,
                outcome=SandboxOutcome.CANCELLED,
                reason="command cancelled before execution",
                started=started,
                cancelled=True,
            )

        with tempfile.TemporaryDirectory(
            prefix=".orchestrator-sandbox-",
            dir=self.root,
        ) as sandbox_directory:
            sandbox_local = Path(sandbox_directory)
            sandbox_home = sandbox_local / "home"
            sandbox_temp = sandbox_local / "temp"
            for local_path in (
                sandbox_home,
                sandbox_temp,
                sandbox_home / "appdata",
                sandbox_home / "local-appdata",
                sandbox_home / ".cache",
                sandbox_home / ".config",
                sandbox_home / ".local" / "share",
            ):
                local_path.mkdir(parents=True, exist_ok=True)
            environment = _sandbox_environment(sandbox_home, sandbox_temp, env)
            launch = self.backend.prepare_launch(
                checked,
                working_directory,
                environment,
            )
            capture = _BoundedCapture(self.max_output_bytes)
            process = subprocess.Popen(
                launch.command,
                cwd=launch.cwd,
                env=dict(launch.env),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                start_new_session=launch.start_new_session,
                creationflags=launch.creationflags,
            )
            assert process.stdout is not None and process.stderr is not None
            readers = (
                threading.Thread(
                    target=_drain,
                    args=(process.stdout, capture, "stdout"),
                    daemon=True,
                ),
                threading.Thread(
                    target=_drain,
                    args=(process.stderr, capture, "stderr"),
                    daemon=True,
                ),
            )
            for reader in readers:
                reader.start()

            timed_out = False
            was_cancelled = False
            deadline = None if timeout is None else started + timeout
            while process.poll() is None:
                if cancelled is not None and cancelled():
                    was_cancelled = True
                    self.backend.terminate_tree(process)
                    break
                remaining = (
                    None if deadline is None else deadline - time.monotonic()
                )
                if remaining is not None and remaining <= 0:
                    timed_out = True
                    self.backend.terminate_tree(process)
                    break
                wait_for = self.cancellation_poll_seconds
                if remaining is not None:
                    wait_for = min(wait_for, max(remaining, 0.001))
                try:
                    process.wait(timeout=wait_for)
                except subprocess.TimeoutExpired:
                    continue

            if process.poll() is None:
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    _terminate_process(process, False)
                    try:
                        process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        pass
            for reader in readers:
                reader.join(timeout=2)
            for stream, reader in zip((process.stdout, process.stderr), readers):
                if reader.is_alive():
                    try:
                        stream.close()
                    except OSError:
                        pass
                    reader.join(timeout=1)

            outcome = (
                SandboxOutcome.CANCELLED
                if was_cancelled
                else SandboxOutcome.TIMED_OUT
                if timed_out
                else SandboxOutcome.COMPLETED
            )
            return SandboxResult(
                command=checked,
                cwd=working_directory,
                returncode=process.returncode,
                stdout=capture.text("stdout"),
                stderr=capture.text("stderr"),
                timed_out=timed_out,
                output_truncated=capture.truncated,
                duration_seconds=time.monotonic() - started,
                requested_isolation=self.requested_isolation,
                actual_isolation=self.backend_attestation.isolation_level,
                isolation_details=self.backend_attestation.details,
                outcome=outcome,
                backend_attestation=self.backend_attestation,
                cancelled=was_cancelled,
                blocked_reason=(
                    "command cancelled" if was_cancelled else None
                ),
            )
