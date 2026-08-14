"""Print the preflight issues recorded for a task."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from collections import Counter
from pathlib import Path

DB = Path(
    os.environ.get(
        "ORCHESTRATOR_DB_PATH", Path.home() / ".ai_orchestrator" / "orchestrator.sqlite3"
    )
)

task_id = sys.argv[1]
con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
rows = con.execute(
    "SELECT event_type, envelope_json FROM events WHERE task_id = ? "
    "AND event_type IN ('plan_preflight_failed', 'plan_preflight_partial') ORDER BY sequence",
    (task_id,),
).fetchall()

for event_type, envelope in rows:
    payload = (json.loads(envelope).get("payload") or {}) if envelope else {}
    issues = payload.get("issues") or []
    print(f"== {event_type}: {len(issues)} issue(s)")
    for code, count in Counter(issue.get("code") for issue in issues).most_common():
        print(f"   {count:3d}  {code}")
    for issue in issues:
        print(
            f"   - [{issue.get('workstream_id')}] {issue.get('code')}: "
            f"{str(issue.get('message'))[:220]}"
        )
