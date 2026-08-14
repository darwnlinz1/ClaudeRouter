"""Explain, per workstream, what actually gated its workers.

Answers the question "why were so many workers spawned but only one or two
running?" from the recorded plan rather than from theory.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

DB = Path(
    os.environ.get(
        "ORCHESTRATOR_DB_PATH", Path.home() / ".ai_orchestrator" / "orchestrator.sqlite3"
    )
)


def main() -> int:
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row

    if len(sys.argv) > 1:
        task_id = sys.argv[1]
    else:
        row = con.execute(
            "SELECT task_id, MAX(sequence) AS seq FROM events GROUP BY task_id "
            "ORDER BY MAX(created_at) DESC LIMIT 1"
        ).fetchone()
        task_id = row["task_id"]
    print(f"task {task_id}\n")

    plan_row = con.execute(
        "SELECT plan_json FROM plans WHERE task_id = ? ORDER BY revision DESC LIMIT 1",
        (task_id,),
    ).fetchone()
    if not plan_row:
        print("no plan recorded")
        return 1
    plan = json.loads(plan_row["plan_json"])

    streams = plan.get("workstreams") or []
    print(f"{'workstream':<32} {'items':>5} {'depends on'}")
    chain = 0
    for stream in streams:
        deps = stream.get("dependencies") or []
        if deps:
            chain += 1
        print(
            f"{str(stream.get('id')):<32} {len(stream.get('work_items') or []):>5} "
            f"{', '.join(deps) if deps else '-- none --'}"
        )

    total_items = sum(len(stream.get("work_items") or []) for stream in streams)
    print(f"\n{len(streams)} workstreams, {total_items} work items")
    print(f"{chain} of {len(streams)} workstreams must wait for an upstream workstream")

    independent = [s for s in streams if not (s.get("dependencies") or [])]
    print(
        f"\nAt the start only {len(independent)} workstream(s) can run: "
        f"{', '.join(str(s.get('id')) for s in independent) or 'none'}"
    )
    concurrent_items = sum(len(s.get('work_items') or []) for s in independent)
    print(f"That allows at most {concurrent_items} worker(s) in the first wave.")

    fanout = (plan.get("metadata") or {}).get("fanout") or {}
    manager = fanout.get("manager") or {}
    print(
        f"\nplanning caps: managers max={manager.get('max')} selected={manager.get('selected')} "
        f"execution_slots={manager.get('execution_slots')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
