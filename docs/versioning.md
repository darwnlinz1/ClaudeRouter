# Compatibility and versioning

The current local HTTP surface is `/api/*`. Additive response fields and new
routes are backward compatible. Removing a route, removing an event field,
changing an event field type, or making an existing optional event field
required is breaking.

Events carry an envelope `version`; persisted schemas are migrated separately
through the SQLite schema version. Unknown events and additive event fields are
preserved during replay.

`docs/compatibility-baseline.json` is the reviewed contract snapshot. CI runs:

```powershell
python scripts/check_compatibility.py
```

When a deliberate breaking release is approved:

1. bump the affected API/event major version and package major version;
2. provide an event upcaster or migration;
3. document client and rollback impact;
4. update generated TypeScript contracts and tests;
5. update the baseline with `--update` in the same reviewed change.

Never update the baseline merely to make CI green.
