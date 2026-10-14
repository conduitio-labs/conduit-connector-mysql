# Record table coverage in the CDC position

## Summary

Add an optional `table_coverage` object to `cdc_position`, carried on every CDC record, so a
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

`CdcPosition.TableCoverage` (`table_coverage`), three disjoint sorted sets:

- `snapshotted`: the pipeline snapshotted these tables.
- `unverified`: tables adopted from a position written by an older build, which did not record
  coverage. They are assumed, never claimed, to have been snapshotted. The connector warns about
  them (`mysql.snapshot.tables_unverified`) on every start.
- `cdc_only`: tables added by a pattern after CDC began. Their existing rows were not snapshotted.
  Warned about once, then recorded.

Absent (`nil`) means the position predates the field (coverage unknown). Present and empty means
known: nothing has been snapshotted (snapshot disabled).

**Size.** The coverage rides on every CDC record at about 24 bytes per name. Above 200 tables
(`MaxListedTables`) the sets are replaced by a SHA-256 over the sorted union, a count and a
`has_unverified` flag, which is about 160 bytes at any size.

On `Open` with a `cdc_position`, the carried coverage is first restricted to the tables currently
configured (a table that left is forgotten). Then, with the snapshot enabled:

| Persisted coverage | Tables not in it | Result |
| --- | --- | --- |
| known | none | continue |
| known | some, `tables` is an explicit list of plain names | fail `mysql.snapshot.tables_added_after_cdc`, config path `tables`, naming the tables and the fix |
| known | some, `tables` has `*`, a regex, an anchored name or an exclude | WARN once with the same code and record them as `cdc_only`; existing rows are not snapshotted |
| unknown (legacy) | n/a | WARN `mysql.snapshot.tables_unverified`; adopt the current tables as `unverified`; tables added later are detected |
| hashed | set differs from the hash | explicit list: fail with the same code, but the message says only that "the configured table set changed" (a removal looks like an addition, so no direction is claimed) and the fix is to restore the previous list or reset the position, never "remove them". Pattern: one WARN with the same wording, then the new set is recorded |

With the snapshot disabled nothing is checked; the restricted coverage is carried so a later
`snapshot.enabled=true` still sees the truth, and a legacy position stays legacy.

Explicit means every rule is an include made of characters with no regex meaning. Table rules are
unanchored regexes, so a plain name can still match longer names, and such tables are treated as
explicit and fail, which is the safe side. The escape is to anchor the rule (`^orders$`), which
makes it a pattern.

The fix an operator takes: start a new pipeline for the new tables, reset the pipeline's position
to snapshot everything again (duplicates expected), or remove the tables from `tables`.

## Consequences that need saying plainly

- **Tables matched later by a pattern are CDC-only.** Their existing rows are not snapshotted and
  are never delivered; only changes from the restart onwards are.
- **Table dropped and recreated across restarts, explicit list: error.** A table absent at one
  restart is forgotten, so when it is back it counts as added. This is deliberate: changes made
  while it was outside `tables` were never captured, and the connector cannot tell a recreated
  table from a new one.
- **Hashed coverage is coarser.** With more than 200 tables an explicit list that merely loses a
  table errors, because a subset cannot be verified from a hash.
- **The hash encoding is versioned.** The hash input is each name's length (uvarint) followed by
  its bytes, so no table name, newline included, can make two different sets hash alike. The
  position records `hash_version`; a hash whose version this build does not compute (or that has
  none) reads as "changed", never as "unchanged". The first encoding joined names with a newline
  and was ambiguous; it did not ship in a release.
- **Legacy positions never become verified.** `unverified` is only cleared by a position reset.

## Alternatives

- **Always re-snapshot only the new tables.** Needs per-table positions across CDC, a merge of
  two streams and ordering rules, which is a feature, not a safety fix. Deferred; this design does
  not preclude it.
- **Hash only.** Constant size, but the error could not name the tables. Kept only above the
  threshold.
- **Fail for patterns too.** Rejected: a pattern like `*` legitimately matches tables created
  later, and refusing to start on every new table is worse than a loud one-time warning for the
  pipelines that use it.
- **Bump the position `version`.** Rejected: older builds would refuse the position
  (`ErrPositionVersionUnsupported`), making rollback impossible. The field is additive.
- **Adopt a legacy position's tables as snapshotted.** Rejected in review: it would assert
  something nobody verified.

## Failure modes

- **Crash anywhere:** the field is part of the position, written atomically with it by the engine;
  nothing new is persisted separately.
- **Snapshot interrupted, then restarted with more tables:** the mid-snapshot resume snapshots
  every current table; the coverage stamped on CDC is taken from the run that finished the snapshot.
- **Mid-snapshot resume changes `tables`:** no check needed; the snapshot covers the new tables.

## Upgrade and rollback

- N+1 reading an N position: field absent, treated as unknown, adopted as unverified with a
  warning. Tested (`TestUpgrade_LegacyCDCPosition_AdoptsTablesAsUnverified`).
- N reading an N+1 position: the field is ignored by the JSON decoder and dropped on N's next write,
  returning the position to unknown. Tested with a struct that lacks the field
  (`TestCdcPosition_TableCoverage_AdditiveCompat`).
- `version` stays 1. Rollback is a plain downgrade; the only loss is the protection.

## Observability

Both WARN codes (`mysql.snapshot.tables_added_after_cdc`, `mysql.snapshot.tables_unverified`) and
the startup error carry the table names where the position allows. Alert on those codes.

## Related

- `20260724-snapshot-cdc-position-handoff.md`
- conduit-connector-mysql #158, #195
