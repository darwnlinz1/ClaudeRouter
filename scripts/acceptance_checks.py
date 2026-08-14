"""Small, deterministic acceptance checks that do not mutate production state."""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from orchestrator.event_schema import TYPESCRIPT_ARTIFACT_PATH, generate_typescript  # noqa: E402

PROVIDER_GUARD_FILES = (
    "orchestrator/**/*.py",
    "server.py",
    "run.py",
    "api.js",
    "main.js",
    "pyproject.toml",
)
PROVIDER_FORBIDDEN_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("Anthropic API-key environment variable", re.compile(r"\bANTHROPIC_(?:API_KEY|AUTH_TOKEN)\b")),
    ("Anthropic Messages endpoint", re.compile(r"(?:api\.anthropic\.com|/v1/messages)\b", re.I)),
    ("Anthropic SDK Messages call", re.compile(r"\b(?:anthropic\.)?messages\.create\s*\(", re.I)),
    ("API-key HTTP header", re.compile(r"\bx-api-key\b", re.I)),
    ("provider API-key CLI flag", re.compile(r"--(?:anthropic-)?api-key\b", re.I)),
    (
        "provider API-key parameter",
        re.compile(r"\bapi_key\s*(?::[^=,\n)]+)?(?:=|,|\))", re.I),
    ),
)


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def check_event_schema() -> int:
    artifact = ROOT / TYPESCRIPT_ARTIFACT_PATH
    expected = generate_typescript().encode("utf-8")
    if not artifact.is_file():
        print(f"FAIL: generated event artifact is missing: {artifact.relative_to(ROOT)}")
        return 1
    actual = artifact.read_bytes()
    if actual != expected:
        print(
            "FAIL: generated event schema drift detected; regenerate with "
            "orchestrator.event_schema.write_typescript_artifact"
        )
        print(f"expected_sha256={_sha256(expected)}")
        print(f"actual_sha256={_sha256(actual)}")
        return 1
    print(f"PASS: generated event schema is byte-for-byte current ({_sha256(actual)})")
    return 0


def _provider_files() -> list[Path]:
    files: set[Path] = set()
    for pattern in PROVIDER_GUARD_FILES:
        files.update(path for path in ROOT.glob(pattern) if path.is_file())
    return sorted(files)


def check_cookie_only_provider() -> int:
    findings: list[str] = []
    files = _provider_files()
    for path in files:
        relative = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8", errors="replace")
        for line_number, line in enumerate(text.splitlines(), start=1):
            for label, pattern in PROVIDER_FORBIDDEN_PATTERNS:
                if pattern.search(line):
                    findings.append(f"{relative}:{line_number}: {label}: {line.strip()}")
    if findings:
        print("FAIL: cookie-only provider guard found forbidden API-key/Messages paths")
        print("\n".join(findings))
        return 1
    print(f"PASS: cookie-only provider guard scanned {len(files)} production/config files")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "check",
        choices=("event-schema", "cookie-only-provider"),
    )
    args = parser.parse_args()
    if args.check == "event-schema":
        return check_event_schema()
    return check_cookie_only_provider()


if __name__ == "__main__":
    raise SystemExit(main())
