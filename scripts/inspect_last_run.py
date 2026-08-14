"""Read-only forensic dump of the most recent hierarchy run.

Answers: how many logical agents were planned, how many actually issued a model
request, which accounts were leased, and why anything stopped short.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

DB = Path(
    os.environ.get(
        "ORCHESTRATOR_DB_PATH", Path.home() / ".ai_orchestrator" / "orchestrator.sqlite3"
    )
)


def loads(raw: object) -> dict:
    if not raw:
        return {}
    try:
        value = json.loads(raw)  # type: ignore[arg-type]
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def main() -> int:
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row

    if len(sys.argv) > 1:
        task_id = sys.argv[1]
        task = con.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
    else:
        task = con.execute("SELECT * FROM tasks ORDER BY updated_at DESC LIMIT 1").fetchone()
        task_id = task["task_id"]
    meta = loads(task["metadata_json"]) if task else {}
    print(f"task {task_id}")
    print(f"  status={task['status'] if task else 'no row in state repository yet'}")
    for key in sorted(meta):
        if any(k in key for k in ("manager", "worker", "parallel", "fanout", "coder", "tester")):
            print(f"  meta.{key}={meta[key]}")

    print("\n--- agent_identities ---")
    ids = con.execute(
        "SELECT * FROM agent_identities WHERE task_id = ? ORDER BY created_at", (task_id,)
    ).fetchall()
    by_role: Counter[str] = Counter()
    for row in ids:
        by_role[row["role"]] += 1
    print(f"total={len(ids)} by role: {dict(by_role)}")
    for row in ids:
        m = loads(row["metadata_json"])
        extra = {k: m[k] for k in ("workstream_id", "work_item_id", "account_ref") if k in m}
        print(f"  {row['role']:<9} {row['logical_agent_id']:<40} {extra}")

    print("\n--- workstreams (latest revision) ---")
    ws = con.execute(
        "SELECT * FROM workstreams WHERE task_id = ? AND revision = "
        "(SELECT MAX(revision) FROM workstreams WHERE task_id = ?)",
        (task_id, task_id),
    ).fetchall()
    print(f"count={len(ws)}")
    for row in ws:
        s = loads(row["state_json"])
        print(
            f"  {row['workstream_id']:<28} status={s.get('status')} "
            f"agent={s.get('agent_instance_id') or s.get('manager_agent_id')} "
            f"items={len(s.get('work_items') or [])}"
        )

    print("\n--- work_items (latest revision) ---")
    wi = con.execute(
        "SELECT * FROM work_items WHERE task_id = ? AND revision = "
        "(SELECT MAX(revision) FROM work_items WHERE task_id = ?)",
        (task_id, task_id),
    ).fetchall()
    print(f"count={len(wi)}")
    for row in wi:
        s = loads(row["state_json"])
        print(
            f"  {row['workstream_id']:<24} {row['work_item_id']:<26} "
            f"status={s.get('status')} agents={s.get('agent_ids')}"
        )

    print("\n--- attempts ---")
    at = con.execute("SELECT * FROM attempts WHERE task_id = ?", (task_id,)).fetchall()
    print(f"count={len(at)}")
    for row in at:
        s = loads(row["state_json"])
        print(
            f"  {row['attempt_id']:<34} ws={row['workstream_id']} item={row['work_item_id']} "
            f"n={row['attempt_number']} state={s.get('state') or s.get('status')}"
        )

    print("\n--- events ---")
    rows = con.execute(
        "SELECT * FROM events WHERE task_id = ? ORDER BY sequence", (task_id,)
    ).fetchall()
    kinds: Counter[str] = Counter()
    agents: dict[str, dict] = {}
    accounts_by_agent: dict[str, set[str]] = defaultdict(set)
    requests_by_agent: Counter[str] = Counter()
    terminal: list[tuple[str, str, str]] = []

    for row in rows:
        etype = row["event_type"]
        kinds[etype] += 1
        env = loads(row["envelope_json"])
        p = env.get("payload") if isinstance(env.get("payload"), dict) else {}
        p = {**env, **p}
        aid = p.get("logical_agent_id") or p.get("agent_instance_id")
        if aid:
            info = agents.setdefault(aid, {"role": None, "status": None, "reason": None})
            info["role"] = p.get("role") or info["role"]
            info["status"] = p.get("status") or info["status"]
            info["reason"] = p.get("failure_reason") or p.get("reason") or info["reason"]
            acct = p.get("account_ref") or p.get("account")
            if acct and acct != "[REDACTED]":
                accounts_by_agent[aid].add(str(acct))
            if "model_request_started" in etype:
                requests_by_agent[aid] += 1
        if any(k in etype for k in ("blocked", "failed", "skipped", "cancelled", "error")):
            terminal.append(
                (
                    str(aid),
                    etype,
                    str(p.get("failure_reason") or p.get("reason") or p.get("error") or "-")[:160],
                )
            )

    print(f"total={len(rows)}")
    for name, count in kinds.most_common(40):
        print(f"  {count:5d}  {name}")

    print("\n--- called vs planned ---")
    print(f"agents seen in events: {len(agents)}")
    called = {a for a, n in requests_by_agent.items() if n > 0}
    print(f"agents with model_request_started: {len(called)}")
    never = sorted(set(agents) - called)
    print(f"agents that NEVER issued a model request: {len(never)}")
    for aid in never[:40]:
        info = agents[aid]
        print(f"    {aid} role={info['role']} status={info['status']} reason={info['reason']}")

    all_accounts = {a for v in accounts_by_agent.values() for a in v}
    print(f"\ndistinct account_ref in events: {len(all_accounts)}")
    owner: dict[str, list[str]] = defaultdict(list)
    for aid, accts in accounts_by_agent.items():
        for acct in accts:
            owner[acct].append(aid)
    collisions = {a: v for a, v in owner.items() if len(v) > 1}
    print(f"accounts shared by >1 agent: {len(collisions)}")
    for acct, aids in list(collisions.items())[:10]:
        print(f"    {acct}: {aids}")

    if terminal:
        print("\n--- terminal / error events ---")
        for aid, etype, reason in terminal[:40]:
            print(f"    {aid} {etype} :: {reason}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
