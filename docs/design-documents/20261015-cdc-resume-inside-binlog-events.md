# CDC resume inside binlog events

## Summary

A CDC restart in the middle of a multi-row binlog event lost rows on the next restart, and a restart inside a
later rows event of one large statement could not start at all. This change makes every CDC position carry a
point canal can resume from (`replay_from`), and makes a restart drop exactly the events and rows that were
delivered before the saved position, including inside compressed transactions
(`binlog_transaction_compression=ON`). Positions from older versions resume by reading their binlog file from the
start. Fixes found by the SIGKILL chaos harness; tracked in ConduitIO/conduit#3005.

## Context

A CDC position is the end offset of the rows event (`pos`), the row inside it (`idx`), and the end of the
previous captured rows event (`prev`). A restart ran canal from `prev` and dropped the first `idx+1` rows of
whatever event came first. Three defects, all observed against MySQL 8.0.39 on `main` at `4676f45`, all present
since #120 (v0.1.4):

1. **Loss on the second restart.** After a restart from `(E, idx k)`, the records of the rest of event E were
   stamped `prev = E's own end` instead of the start of E. A second restart from one of them ran canal from the
   end of E, so the rest of E was never read, and dropped `idx+1` rows of the event after it. Observed: of 8
   rows to deliver, 6 were lost (`TestCDCIterator_SecondResumeMidEventLosesNothing`).
2. **Index restarted at 0.** The rows after the resume point were numbered from 0, not from their place in E.
   Positions went backwards (invariant 2) and fed defect 1.
3. **No restart inside a later rows event of one statement.** MySQL logs a large statement as one `Table_map`
   event followed by several rows events (`binlog_row_event_max_size`, 8 KiB by default). `prev` of a record in
   the second rows event is the end of the first, after the `Table_map`; canal cannot decode the event from
   there (`invalid table id, no corresponding table map event`) and every restart halts. Observed with a
   3000-row INSERT (`TestCDCIterator_ResumeInsideLaterRowsEventOfOneStatement`).

Also fixed: all records of one event shared one metadata map, which the SDK middleware writes per record from
its batching goroutine while earlier records are already with the caller (a data race under `-race`, a fatal
concurrent map access at worst).

A fourth defect was found in review of the first version of this change: **compressed transactions**. With
`binlog_transaction_compression=ON`, MySQL writes a transaction as one `Transaction_payload` event; canal decodes
it and hands over its sub-events (BEGIN, `Table_map`, rows events, commit), each with offset 0, and it syncs
offset 0 at their BEGIN and commit. Positions then said `pos: 0`, and `replay_from` was offset 0 as well, which
MySQL serves as the start of the file. Observed on MySQL 8.0.39: a restart inside a compressed transaction
delivered the whole binlog file again (12,194 records for 3 to deliver in the reviewer's probe); a position from
an older version re-read its file and decoded every earlier compressed row again, against the table's current
columns, which after an `ADD COLUMN` panicked (index out of range in `buildPayload`) on every restart.

Constraints: no position may resume with a gap (invariant 3); positions are monotonic (invariant 2); the
position format change must be readable by the previous release and must not make a downgrade lose rows; a
restart must work for positions already persisted by released versions, including the ones defect 1 wrote.

## Decision

- Every CDC record carries `replay_from`, a point canal can restart from and decode everything after: the last
  position canal synced with an offset (after a commit, a BEGIN, a DDL or a rotation; read from `canal/sync.go`
  in the go-mysql fork the connector pins), or the end of the last MySQL GTID event, which is the start of the
  transaction after it. Offset-0 syncs (inside a compressed transaction) are skipped. For an uncompressed event
  this is its transaction's BEGIN, as before; for an event inside a compressed transaction it is the start of
  the `Transaction_payload` event, from which canal decodes the whole transaction again. The handler tracks it
  in `OnPosSynced` and `OnGTID`, both on canal's goroutine; the call `canal.Close` makes from the tearing-down
  goroutine has a nil header and returns before touching any state.
- An event inside a compressed transaction is named by its number among the offset-0 rows events after
  `replay_from` (`compressed.ordinal`, 1-based), with a fingerprint of the captured table set
  (`compressed.tables`), because canal filters the events of other tables before the handler sees them, so
  the count depends on the set. A restart from `replay_from` counts the same way and drops events before the
  ordinal and the first `idx+1` rows of the event at it. With another table set the ordinal is not comparable
  and the transaction is delivered again from its start.
- A restart for an event with an offset runs canal from `replay_from` and drops every rows event that ends
  before `pos`, every offset-0 event while the saved event is still ahead (it lies before it), and the first
  `idx+1` rows of the event that ends at `pos`. Any offset past `pos` (a later rows event, GTID, commit, DDL or
  rotation) ends the dropping: the saved event did not come (its table is no longer captured) and everything
  after is new. This bound matters for compressed workloads, where a later rows event with an offset may
  never come.
- A saved event that is not where the position says (index past its event's rows; a compressed ordinal that
  is not reached before the next offset) halts the source with `mysql.cdc.resume_event_not_found` instead of
  guessing.
- A row image with fewer values than the table now has columns (an event written before an `ADD COLUMN`, read
  after it: canal decodes with the current schema) halts with `mysql.cdc.row_columns_mismatch` instead of
  panicking (invariant 6).
- `idx` is the row's index in the whole event, and `prev` after a resume is the saved position's `prev`.
- Positions without `replay_from` (older versions) run canal from the start of their binlog file, offset 4, and
  drop by offset the same way, including the compressed transactions before the saved event. If `prev` equals
  the position itself, which only defect 1 writes, `idx` is unreliable and the whole event is delivered again.
  An older position inside a compressed transaction (`pos: 0`) has nothing to find, so its file is delivered
  again from its start; the older versions did the same on every restart (they resumed from `prev`, which was
  offset 0 too). A `replay_from` without an offset, which only an unreleased build of this change wrote, is
  treated as missing.
- Records of one event get their own copy of the metadata.

## Alternatives

- **Fix only `prev` and `idx`.** Smallest change, closes the loss, but leaves defect 3: a pipeline stopped
  inside a large statement stays down. Rejected because the stuck pipeline has no remedy short of a
  re-snapshot.
- **Run canal from the event's own `Table_map`.** The handler never sees `Table_map` events or rows-event start
  offsets in a form it can persist; it would need `SHOW BINLOG EVENTS` per event or replication-level parsing.
  Rejected as more moving parts than the transaction-level point canal already maintains.
- **Bump the CDC envelope version and change the meaning of `prev`.** A previous release reading such a
  position would run from the transaction start and drop `idx+1` rows of the wrong event, a loss on downgrade.
  Rejected; `replay_from` is additive and `prev` keeps its meaning, now computed correctly.
- **Compressed: the last synced position with an offset as `replay_from`, and deliver the compressed
  transaction again.** Smallest fix (the reviewer's prototype). With compression on for every transaction the
  only syncs with an offset are rotations and DDL, so `replay_from` stays at the file start and every restart
  delivers the file again: the 12,194-record symptom remains for exactly the configuration that enables
  compression globally. Rejected for the GTID-event point.
- **Compressed: deliver the transaction again from its start instead of counting.** Bounded duplicates, no
  ordinal. Rejected because a restart inside a large compressed transaction would redeliver all of it every
  time; the ordinal with a table-set fingerprint makes it exact and falls back to this when the set changed.
- **Compressed: count per table, or count without the fingerprint.** Canal drops the events of uncaptured tables
  before the handler, so after the set shrank an ordinal would point past the saved event and drop new rows.
  Rejected; with the fingerprint a changed set never trusts the ordinal.
- **Refuse older positions and ask for a re-snapshot.** Safe but punishes every upgraded pipeline for a defect
  most of them never hit. Rejected; reading one binlog file from its start is a one-time cost.

## Failure modes

| Failure | Behavior |
| --- | --- |
| kill -9 in the middle of an event, any number of times | Each restart drops exactly the delivered rows; covered by the chaos harness and the second-resume test |
| Restart inside a later rows event of one statement | Runs from the transaction point; the `Table_map` is read again |
| Older position, correct `prev` | File read from offset 4, events before `pos` dropped, then exact |
| Older position written by defect 1 | File read from offset 4, the saved event delivered again in full: duplicates, never a gap |
| Binlog file of the position purged | Unchanged: the existing start-position error |
| Saved event no longer arrives (table excluded) | Dropping stops at the first later offset (rows event, GTID, commit, DDL, rotation) |
| kill -9 inside a compressed transaction, any number of times | Restart from the transaction's start, drop by ordinal and index: exact |
| Restart inside a compressed transaction after the captured tables changed | The transaction is delivered again from its start (logged): duplicates, never a gap |
| Compressed transactions before the saved event (offset 0) | Dropped while the saved event is ahead |
| Older position, compressed transactions earlier in its file | Dropped; the earlier panic on rows older than an `ADD COLUMN` cannot happen for them |
| Older position inside a compressed transaction | The file is delivered again from its start (logged), as the older versions did on every restart |
| Saved index past its event, or compressed ordinal not found | Halt with `mysql.cdc.resume_event_not_found` (should be impossible) |
| Event written before an `ADD COLUMN`, read after it (connector lagging across DDL) | Halt with `mysql.cdc.row_columns_mismatch`; it used to panic. Fix: reset the position. A canal limitation (it decodes with the current schema), not new |
| MySQL 8.4 tagged GTIDs (`GTID_TAGGED_LOG_EVENT`) | canal does not report them, so `replay_from` stays at the last synced offset: correct, a longer re-read |
| MariaDB | Unchanged from before: only MySQL GTID events move the point; MariaDB has no compressed transactions of this kind. Not verified |
| Downgrade to a previous release | It ignores `replay_from` and `compressed` and resumes from `prev` and `idx`, which are now correct for it; its defects 3 and 4 remain |

## Upgrade and rollback

The field is additive and the envelope version stays 1, so the previous release reads new positions. The first
restart after the upgrade reads the current binlog file from its start (up to `max_binlog_size`, 1 GiB by
default) and logs a warning saying so. Rollback is a revert; positions written meanwhile resume on the old code
as they did before.

## Observability

A warning at `Open` when a position forces a read from the file start or delivers events again, naming the file
and offset: an older position, one written by defect 1, an older position inside a compressed transaction, and a
compressed position counted under another table set. A saved event that is not found halts with
`mysql.cdc.resume_event_not_found`; a row image narrower than the table halts with
`mysql.cdc.row_columns_mismatch`. Both are `*Error` values with the code, the file and offset, and a fix.

## Tests

Integration tests against MySQL 8.0.39, each failing on `main` without the fix: shared metadata, index after a
mid-event resume, the second-resume loss, resume inside a later rows event of one statement. An upgrade test
for both kinds of older position, unit tests for the plan and the position ordering, and the SIGKILL chaos
harness (`test/chaos`, #229), which kills the source mid-snapshot, mid-checkpoint, at the snapshot boundary, twice
inside one large event and at a random instant, and checks no gap, exact resume and monotonic positions.

Compressed transactions (`cdc_compressed_resume_integration_test.go`, compression turned on per session, so they
run on the default CI server): resume inside a compressed event, inside the third of three compressed
transactions, at the end of one, uncompressed rows after a compressed transaction and the reverse; two restarts
inside one 3000-row compressed statement (several rows events, all offset 0); a changed table set; an older
position with a compressed transaction and an `ADD COLUMN` before it; the upgrade test above with a compressed
transaction earlier in the file. Halts: a row image narrower than the table, and a saved index past its event.

Known, not fixed here: compressed transactions are decoded by go-mysql without the parser options canal sets
(`parseTime` among them), so `DATETIME`/`TIMESTAMP` values arrive as strings instead of times when compression is
on. That is a go-mysql defect (`TransactionPayloadEvent.decodePayload` builds a fresh parser) and is tracked
separately; it is also why a CI row with compression on globally fails today for reasons unrelated to resume.
