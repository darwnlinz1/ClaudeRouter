"""Show which logical agents hold an account and which do not."""

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

task_id = sys.argv[1]
con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
rows = con.execute(
    "SELECT event_type, envelope_json FROM events WHERE task_id = ? ORDER BY sequence",
    (task_id,),
).fetchall()

agents: dict[str, dict] = {}
for event_type, envelope in rows:
    event = json.loads(envelope)
    payload = event.get("payload") or {}
    merged = {**event, **payload}
    agent_id = merged.get("logical_agent_id") or merged.get("agent_instance_id")
    if not agent_id:
        continue
    info = agents.setdefault(
        agent_id, {"role": None, "label": None, "account": None, "calls": 0, "status": None}
    )
    info["role"] = merged.get("role") or info["role"]
    info["label"] = merged.get("agent_label") or info["label"]
    info["status"] = merged.get("status") or info["status"]
    account = merged.get("account")
    if account and account != "[REDACTED]":
        info["account"] = account
    if event_type == "model_request_started":
        info["calls"] += 1

print(f"{'label':<14} {'role':<9} {'calls':>5}  {'status':<16} account")
for agent_id, info in agents.items():
    print(
        f"{str(info['label'] or agent_id[:14]):<14} {str(info['role']):<9} "
        f"{info['calls']:>5}  {str(info['status'])[:16]:<16} {info['account'] or '-- none --'}"
    )

without = [i for i in agents.values() if not i["account"]]
print(f"\n{len(agents)} agents, {len(without)} without an account")
