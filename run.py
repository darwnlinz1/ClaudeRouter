
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from orchestrator.orchestrator import run_session


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", required=True, type=Path, help="Project root directory")
    parser.add_argument("--task", required=True, help="Task description for this session")
    parser.add_argument(
        "--files",
        nargs="*",
        default=[],
        help="Optional seed file paths relative to --root (omit for folder-only)",
    )
    parser.add_argument(
        "--test-cmd",
        default=None,
        help="Optional shell test command run after each patch, e.g. 'pytest -x' "
        "(quoted as one string; split on spaces)",
    )
    parser.add_argument("--max-turns", type=int, default=None)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    test_cmd = args.test_cmd.split() if args.test_cmd else None
    kwargs = {}
    if args.max_turns is not None:
        kwargs["max_turns"] = args.max_turns

    result = run_session(
        root=args.root,
        task_description=args.task,
        source_files=args.files,
        test_cmd=test_cmd,
        **kwargs,
    )

    print(f"\nSession stopped: {result.stopped_reason}")
    for i, turn in enumerate(result.turns, 1):
        status = "OK" if turn.accepted else "REJECTED"
        print(f"  turn {i}: [{status}] {turn.tool_name} -- {turn.detail}")
    print(f"\nFinal state.json: {args.root / 'state.json'}")
    print(f"DECISIONS.md:      {args.root / 'DECISIONS.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
