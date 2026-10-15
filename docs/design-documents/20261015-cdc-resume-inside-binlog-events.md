# CDC resume inside binlog events

## Summary

A CDC restart in the middle of a multi-row binlog event lost rows on the next restart, and a restart inside a
later rows event of one large statement could not start at all. This change makes every CDC position carry a
point canal can resume from (`replay_from`), and makes a restart drop exactly the events and rows that were
delivered before the saved position. Positions from older versions resume by reading their binlog file from the
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

Constraints: no position may resume with a gap (invariant 3); positions are monotonic (invariant 2); the
position format change must be readable by the previous release and must not make a downgrade lose rows; a
restart must work for positions already persisted by released versions, including the ones defect 1 wrote.

## Decision

- Every CDC record carries `replay_from`: canal's synced position when the event was handled. Canal saves it
  after a commit (XID), a BEGIN, a DDL and a rotation (read from `canal/sync.go` in the go-mysql fork the
  connector pins), so it is a transaction-level point at or before the event, from which the event's
  `Table_map` is read again. The later-rows-event test proves the point decodes on MySQL 8.0.39.
- A restart runs canal from `replay_from` and drops every rows event that ends before `pos`, and the first
  `idx+1` rows of the event that ends at `pos`. An event whose saved position never comes (its table is no
  longer captured) ends the dropping: everything after it is new. An event inside a compressed transaction has
  offset 0 (#213) and cannot be placed, so it is delivered (at worst again).
- `idx` is the row's index in the whole event, and `prev` after a resume is the saved position's `prev`.
- Positions without `replay_from` (older versions) run canal from the start of their binlog file, offset 4, and
  drop by offset the same way. If `prev` equals the position itself, which only defect 1 writes, `idx` is
  unreliable and the whole event is delivered again.
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
| Saved event no longer arrives (table excluded) | Dropping stops at the first later event |
| Compressed transaction (offset 0) between `replay_from` and the saved event | Delivered again; existing issue #213 |
| Downgrade to a previous release | It ignores `replay_from` and resumes from `prev` and `idx`, which are now correct for it; its defect 3 remains |

## Upgrade and rollback

The field is additive and the envelope version stays 1, so the previous release reads new positions. The first
restart after the upgrade reads the current binlog file from its start (up to `max_binlog_size`, 1 GiB by
default) and logs a warning saying so. Rollback is a revert; positions written meanwhile resume on the old code
as they did before.

## Observability

A warning at `Open` when an older position forces a read from the file start, naming the file, and a second
wording when the saved event is delivered again in full. An impossible offset (the saved index past the end of
its event) is logged at error level with the position.

## Tests

Integration tests against MySQL 8.0.39, each failing on `main` without the fix: shared metadata, index after a
mid-event resume, the second-resume loss, resume inside a later rows event of one statement. An upgrade test
for both kinds of older position, unit tests for the plan and the position ordering, and the SIGKILL chaos
harness (`test/chaos`), which kills the source mid-snapshot, mid-checkpoint, at the snapshot boundary, twice
inside one large event and at a random instant, and checks no gap, exact resume and monotonic positions.
