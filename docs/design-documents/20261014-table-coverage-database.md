# Record the database in the table coverage

## Summary

The table coverage in a CDC position (`20261012-cdc-snapshotted-tables.md`) records table names
without the database they belong to. A DSN pointed at another database with tables of the same
names therefore compares as unchanged, the restart skips the snapshot, and that database's
existing rows are never delivered. This adds an optional `database` field to `table_coverage` and
refuses a restart whose DSN names a different database. Issue #218.

## Context

On `Open` with a `cdc_position` and the snapshot enabled, `resolveCoverage` compares the configured
tables with the recorded ones. Table names are only unique within a database. The connector
resolves tables with `information_schema.tables WHERE table_schema = <dsn database>` and filters
CDC with `^<dsn database>.<table>$`, so after a DSN change it reads a different set of tables under
the same names. The coverage says they were all snapshotted, and nothing is logged. Invariant 3.

The defect predates #217 and comes from #195. No release contains #195 (latest is v0.3.0), so
positions that carry a coverage exist only from development builds.

## Constraints

- The position is serialized. The field must be additive: older builds must still read the
  position (no `version` bump, which would make rollback impossible, as in the coverage design),
  and absent must mean "written before the field existed".
- The fix must not refuse pipelines that did not change their database.
- Error messages must not echo the DSN, which can hold credentials.

## Decision

`TableCoverage.Database` (`database`, omitted when empty) is the database named in the DSN when the
coverage was written. Every coverage the connector writes records it: fresh start, mid-snapshot
resume, legacy adoption, and every coverage carried forward from a CDC restart with the snapshot
enabled. `Restrict` and `WithCDCOnly` keep it.

On a CDC restart with the snapshot enabled, the database is checked before the table set:

| Recorded database | Result |
| --- | --- |
| same as the DSN's | continue with the table checks as before |
| different | fail `mysql.snapshot.database_changed`, config path `dsn`, for explicit lists **and** patterns |
| absent (older build) | WARN `mysql.snapshot.database_assumed` once, adopt the DSN's database, record it, then the table checks |

With the snapshot disabled nothing is checked and the recorded database is carried unchanged, so a
later `snapshot.enabled: true` still compares against the database that was actually covered. This
matches the existing rule for tables. A position with no coverage at all (released builds) keeps
its existing path: tables adopted as unverified, and now recorded with the DSN's database.

The comparison is exact. On a server with `lower_case_table_names` 1 or 2, a change of letter case
alone names the same database but reads as changed; the error says so. That is the safe direction.

### Why patterns fail too

The coverage design lets patterns warn about added tables because a pattern legitimately matches
tables created later, whose rows CDC sees from creation. That reason does not hold for a database
switch: every table in the other database existed before CDC was reading it, and none of their
rows was snapshotted. Warning and continuing would skip all of them. Refusing is the only result
that keeps invariant 3. The escapes are the same as for tables: point the DSN back, start a new
pipeline, reset the position, or set `snapshot.enabled: false` to say existing rows are not wanted.

### Legacy coverage without a database: accept once

A coverage without `database` was written by a development build between #195 and this change.

- **Accept once (chosen).** Adopt the DSN's database with a WARN that names it, then record it.
  The only case it gets wrong is a DSN changed in the same restart as the upgrade, the warning
  names the assumed database so that case is visible, and the population is development builds
  only.
- **Fail closed (rejected).** Every pipeline on a development build would have to reset its
  position, which re-snapshots everything and delivers duplicates, to guard against a coincidence
  the warning already exposes.

### Server identity: not recorded here

Recording `@@server_uuid` or the DSN's host and port was considered and rejected for the coverage:

- A different server makes the binlog file and offset meaningless, whether or not the snapshot is
  enabled. That is a position-validity problem, not a coverage one, and belongs on `CdcPosition`
  as its own check. It is already a documented limit (no GTID, failover breaks resume).
- A failover to a replica changes `server_uuid` while the data, and so the coverage, is still
  right. Folding it into coverage would report "tables not snapshotted" for the wrong reason.
- Host and port are not identity: a DNS name, a proxy, an IP address or a port-forward changes
  them with the same server behind, and would refuse healthy pipelines.

Follow-up, #220: record the server identity on `CdcPosition` and refuse a position from another server.

## Alternatives

- **Qualify every name as `db.table`.** Same effect for list form, but it changes the meaning of
  existing entries and needs a migration of every recorded name; the hash form would need a new
  hash version. One field is simpler and additive.
- **Hash the database into the coverage hash.** Only covers the hash form, and makes the error
  unable to name the two databases.
- **Hash of DSN host, port and database** (suggested in the issue). Rejected for the host and port
  reasons above.

## Failure modes

- **Crash anywhere:** the field is part of the position and is written atomically with it by the
  engine. Nothing is persisted separately.
- **DSN changed while the snapshot is still running:** the mid-snapshot resume snapshots the
  current tables, and the coverage stamped on CDC records the current database. Not affected.
- **DSN changed and snapshot disabled:** not checked, by design; the operator opted out of
  existing rows.
- **Database dropped and recreated with the same name:** not detected (same name). Same class as a
  table dropped and recreated, which the coverage cannot see either.
- **Data copied to a new database and the DSN switched:** refused although the rows were
  delivered. The fix text says to reset and expect duplicates.
- **Empty database in the DSN:** the connector resolves no tables, so there is nothing to compare.

## Upgrade and rollback

- N+1 reading an N position with coverage but no database: adopted once, with a warning
  (`TestUpgrade_CoverageWithoutDatabase_IsAdoptedOnce`, against MySQL).
- N reading an N+1 position: the JSON decoder ignores `database`, and N drops it on its next write,
  returning to the pre-fix behaviour (`TestTableCoverage_DatabaseAdditiveCompat`).
- `version` stays 1. Rollback is a plain downgrade; the only loss is this protection.

## Observability

`mysql.snapshot.database_changed` (startup error, names both databases and the tables) and
`mysql.snapshot.database_assumed` (WARN, once). Alert on both codes.

## Related

- `20261012-cdc-snapshotted-tables.md`
- conduit-connector-mysql #195, #217, #218, #220
