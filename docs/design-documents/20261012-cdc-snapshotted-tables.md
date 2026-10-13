# Record snapshotted tables in the CDC position

## Summary

Add an optional `snapshotted_tables` field to `cdc_position`, carried on every CDC record, so a
restart can detect tables whose existing rows were never snapshotted instead of silently skipping
them.

## Context

A snapshot runs only on a fresh start. After #158 (restart from a `cdc_position` no longer
re-snapshots), two operator actions lose data silently:

1. adding a table to `tables` after the pipeline entered CDC: its pre-existing rows are never
   delivered; only later changes are;
2. switching `snapshot.enabled` from `false` to `true` on a pipeline that ran CDC-only.

Both violate invariant 3 (no record dropped without delivery or DLQ) and neither logs anything.
Before #158 a full re-snapshot on every restart happened to mask this.

## Decision

`CdcPosition.SnapshottedTables *[]string` (`snapshotted_tables`, sorted table names).

- Absent (`nil`) means the position predates the field: coverage is unknown.
- Present and empty means known: nothing was snapshotted (snapshot disabled from the start).
- A run that snapshots stamps the set of tables it snapshotted. A CDC-only restart carries the
  persisted set unchanged, so a later `snapshot.enabled=true` still sees the truth.

On `Open` with a `cdc_position` and the snapshot enabled:

| Persisted set | Tables not in it | Result |
| --- | --- | --- |
| known | none | continue |
| known | some, `tables` is an explicit list of plain names | fail: `mysql.snapshot.tables_added_after_cdc`, config path `tables`, names the tables and the fix |
| known | some, `tables` has `*` or a regex or an exclude | WARN with the same code; the persisted set is carried unchanged so the warning repeats until resolved |
| unknown (legacy) | n/a | WARN that coverage is unverified, assume the current tables were covered, start recording |

With the snapshot disabled nothing is checked.

Explicit means every rule is an include made of characters with no regex meaning. Table rules are
unanchored regexes, so a plain name can still match longer names; such tables are treated as
explicit and fail, which is the safe side.

The fix an operator takes: start a new pipeline for the new tables, reset the pipeline's position
to snapshot everything again (duplicates expected), or remove the tables from `tables`.

## Alternatives

- **Always re-snapshot only the new tables.** Needs per-table positions across CDC, a merge of
  two streams and ordering rules, which is a feature, not a safety fix. Deferred; this design does
  not preclude it.
- **Store a hash of the table set.** Constant size, but the error could not name the tables and a
  WARN could not say which tables are affected. Kept as the fallback if the list size below proves
  a problem.
- **Fail for patterns too.** Rejected: a pattern like `*` legitimately matches tables created
  later, and refusing to start on every new table is worse than a loud warning for the pipelines
  that use it.
- **Bump the position `version`.** Rejected: older builds would refuse the position
  (`ErrPositionVersionUnsupported`), making rollback impossible. The field is additive.

## Failure modes

- **Crash anywhere:** the field is part of the position, written atomically with it by the
  engine; nothing new is persisted separately.
- **Position size:** the list rides on every CDC record. 1000 tables at about 30 bytes is about
  30 KB per record. Acceptable for tens to hundreds of tables; revisit (hash fallback) if users
  report pipelines with many thousands of tables.
- **Table removed, then re-added:** the set still contains it, so no error, but changes made while
  it was out of `tables` were never captured. Not detected; operators must treat removal as a
  position reset.
- **Snapshot interrupted, then restarted with more tables:** the mid-snapshot resume snapshots
  every current table; the set stamped on CDC is taken from the run that finished the snapshot.
- **Mid-snapshot resume changes `tables`:** no check needed; the snapshot covers the new tables.
- **Legacy position:** cannot be verified; the WARN says so. An operator who knows tables were
  added since can reset.

## Upgrade and rollback

- N+1 reading an N position: field absent, treated as unknown, WARN, begins recording. Tested
  (`TestUpgrade_LegacyCDCPosition_WarnsAndStartsRecording`, `TestCdcPosition_SnapshottedTables_AdditiveCompat`).
- N reading an N+1 position: the field is ignored by the JSON decoder and dropped on N's next
  write, returning the position to unknown. Tested with a struct that lacks the field.
- `version` stays 1. Rollback is a plain downgrade; the only loss is the protection.

## Observability

WARN logs carry the code `mysql.snapshot.tables_added_after_cdc` and the table names; the startup
error carries the same code. Alert on that code in pipeline errors and logs.

## Related

- `20260724-snapshot-cdc-position-handoff.md`
- conduit-connector-mysql #158, #195
