# Snapshot to CDC position handoff: persisting the CDC start position

## Summary

The MySQL source connector loses the CDC start binlog position (`P0`) on a mid-snapshot restart,
causing **silent data loss** during initial sync. `P0` is captured in memory under a read lock at
snapshot start but is never persisted; snapshot records carry only the snapshot position. On a
crash mid-snapshot the connector reparses a position with no CDC start, captures a *fresh* master
position `P1 > P0`, and resumes the snapshot only for not-yet-copied rows. Any write to an
already-copied row in the binlog window `(P0, P1]` is delivered by neither the resumed snapshot
(which skips it) nor CDC (which starts after it). This violates Invariant 3 (at-least-once,
including restart paths).

The fix persists `P0` by embedding it in every snapshot record's position (a new optional
`cdc_start` field on `SnapshotPosition`). On restart the connector seeds the CDC iterator from the
persisted `P0` instead of capturing a fresh position, so the window `(P0, restart]` is replayed
from the binlog. Replay produces duplicates for already-copied rows, which is acceptable under the
at-least-once floor. The position change is additive and backward compatible for upgrade; a
mid-snapshot **downgrade** reintroduces the pre-fix behavior and is called out as unsafe.

## Context

### The non-transactional snapshot makes `P0` load-bearing

The connector's initial sync is deliberately **not** a single consistent-read transaction. The
sequence in `newCombinedIterator` (`combined_iterator.go`) is:

1. `lockTables` runs `FLUSH TABLES ... WITH READ LOCK` (`combined_iterator.go:96`, `:179`).
2. `snapshotIterator.setupWorkers` records each table's min/max primary-key bounds under the lock
   (`combined_iterator.go:103`).
3. When there is no persisted CDC position, `cdcIterator.obtainStartPosition()` captures the
   current master binlog position `P0 = canal.GetMasterPos()` and stores it **only in memory** on
   the `cdcIterator` (`combined_iterator.go:109-115`, `cdc_iterator.go:73-87`).
4. The lock is **released** (`combined_iterator.go:117`), and only then are the snapshot workers
   started (`combined_iterator.go:123`) and the CDC iterator started (`:127`).

Because the lock is released before any rows are read, the snapshot is a **non-transactional**,
chunk-by-chunk copy that runs concurrently with live writes. The only thing that reconciles a write
that landed after the snapshot copied a given row is **CDC replay starting from `P0`**. `P0` is the
exact binlog coordinate at which the row snapshot is consistent; replaying the binlog from `P0`
forward re-applies every concurrent change on top of the copied rows, yielding eventual
consistency. `P0` is therefore **load-bearing for correctness**, not a throughput optimization. If
CDC starts anywhere after `P0`, the events between `P0` and that later point are never delivered.

### `P0` is never persisted during the snapshot phase

Snapshot records carry only the snapshot position. `snapshotIterator.buildRecord` sets the record
position to `s.lastPosition.ToSDKPosition()` (`snapshot_iterator.go:222-225`), and
`SnapshotPosition.ToSDKPosition()` marshals `Position{SnapshotPosition: &p}` with `CdcPosition` left
`nil` (`common/position.go:42-49`, `:33-36`). Nothing in the snapshot phase writes the in-memory
`P0` into any persisted position. It exists only on the live `cdcIterator` struct and dies with the
process.

### The restart path recaptures a fresh, later position

On restart, `Source.Open` parses the last persisted position (`source.go:158-166`). A mid-snapshot
position has `snapshot_position` set and `cdc_position` absent, so `pos.CdcPosition == nil` and
`startCdcPosition: nil` is passed to `newCombinedIterator` (`source.go:168-180`). With
`startCdcPosition == nil`, the combined iterator calls `obtainStartPosition()` again
(`combined_iterator.go:109-110`), capturing a **new** master position `P1`, which is at or after the
current end of the binlog — strictly later than `P0` by the duration of the first run plus the
downtime.

Meanwhile the resumed snapshot only re-reads rows past the persisted `LastRead`. Each fetch worker
sets its start bound to the persisted `LastRead` (`fetch_worker.go:126-130`) and, because a start
position means "already read this key", excludes it with `squirrel.Gt` (`fetch_worker.go:159`,
`:210-216`). Rows with `pk <= LastRead` are never re-read.

### Net data-loss window

For any row already copied in the first run (`pk <= LastRead`), a concurrent `UPDATE`, `DELETE`, or
low-key `INSERT` recorded in the binlog window `(P0, P1]`:

- is **not** re-delivered by the resumed snapshot — the row key is `<= LastRead`, excluded by `Gt`;
- is **not** delivered by CDC — CDC starts at `P1`, after the event.

The change is **silently lost**. A lost `DELETE` leaves a phantom row downstream forever; a lost
`UPDATE` leaves a stale value; a lost low-key `INSERT` is a missing row. `combined_iterator_integration_test.go:47-72`
demonstrates the exact concurrent-write pattern (insert users, create iterator, update the users,
read snapshot then CDC) and passes only because it never restarts the iterator — the in-memory `P0`
survives.

**Blast radius:** initial sync only. Once the first CDC record is acked, the persisted position
carries `cdc_position`, `pos.CdcPosition != nil` on restart, and steady-state CDC resumes correctly.
The bug is confined to the snapshot phase and the snapshot to CDC handoff.

## Goals / Non-goals

**Goals**

- Guarantee at-least-once delivery across a mid-snapshot restart (uphold Invariant 3).
- Persist `P0` durably for the entire snapshot phase so restart resumes CDC from `P0`.
- Keep the position format readable by version N and N+1 (additive, backward compatible on upgrade).
- Preserve `P0` through position `Clone()` so the *resumed* snapshot keeps stamping it (no
  second-crash regression).
- Convert the expired-binlog resume from a silent stall into a loud fail-stop with a stable error
  code, since persist-`P0` heightens expired-binlog exposure on resume.
- Ship the SIGKILL-mid-snapshot regression test (and a double-crash variant) that would have caught
  this.

**Non-goals**

- Making the snapshot transactional / removing the reliance on CDC replay (see Alternative A).
- Eliminating duplicate delivery on resume — duplicates are acceptable under at-least-once and are
  the safe trade against a gap.
- Fixing the separate "CDC cold-start position durability" gap when the snapshot yields zero records
  or is disabled (see Failure modes). That is a distinct ticket.
- Any change to `conduit-connector-protocol` or the opencdc record shape.

## Constraints

- MySQL `GetMasterPos()` returns a single server-wide binlog coordinate; there is no per-table
  binlog position. `P0` is global to the instance.
- Conduit persists the position of **acked** records only. To persist `P0` throughout the snapshot,
  every snapshot record must carry it (there is no side channel to persist a bare position).
- The opencdc `Position` has no envelope version field today (`common/position.go:33-36`); evolution
  is by additive, `omitempty` JSON fields. Adding a hard version field is a heavier, separate change.
- Binlog retention is finite. Resuming from `P0` requires `P0`'s binlog file to still exist.
- Changing serialized position format is Tier-1: it must round-trip across N/N+1 with an
  upgrade/downgrade test (CLAUDE.md data-integrity discipline).

## Alternatives considered

### A. Make the snapshot transactional (consistent read), dropping the `P0` reliance

Replace lock-then-release with a `REPEATABLE READ` transaction (`START TRANSACTION WITH CONSISTENT
SNAPSHOT`) so every chunk reads the same MVCC view. CDC would then only need to start at the
snapshot's transaction boundary, and already-read rows would need no reconciliation.

**Why it loses.** It does not remove the need to persist a start position — it *moves* it: CDC must
still begin at the exact binlog coordinate of the consistent snapshot, and that coordinate must
survive a restart, so the persistence problem is identical. It is also a much larger, higher-risk
change to the fetch-worker model: a single long-lived transaction must span the entire (possibly
multi-hour) multi-table snapshot, holding a read view open across all workers and defeating the
current independent-per-table chunking. It changes locking, connection lifecycle, and memory
behavior on large tables. It is the right long-term direction but is a redesign, not a data-loss
fix, and it still needs this document's persistence mechanism underneath it. Deferred.

### B. Persist `P0` in a separate top-level position field vs. embedding in the snapshot position

Add `P0` as its own top-level `Position` field (e.g. `Position.CdcStartPosition`) rather than nesting
it inside `SnapshotPosition`.

**Why it loses (mildly).** Both are additive and would work, but a top-level field muddies the
`SnapshotPosition`/`CdcPosition` discriminator that `source.go:168-172` and the iterators rely on:
readers branch on which of the two is non-nil to decide snapshot vs. CDC mode. A third top-level
field forces every reader to reason about combinations (`CdcStartPosition` set while `CdcPosition`
also set?). Nesting `cdc_start` inside `SnapshotPosition` keeps it scoped to exactly the phase that
owns it — it is meaningful only while a snapshot is in progress, and it naturally disappears from the
position once steady-state CDC takes over and emits `cdc_position`. Chosen shape is B-nested.

### C. Start CDC before/concurrently with the snapshot and buffer events

Start the CDC stream at `P0` immediately and buffer/interleave its events with snapshot output,
persisting CDC progress from the start.

**Why it loses.** It introduces an in-memory (or on-disk) buffer of unbounded size for the entire
snapshot duration, a new ordering/merge problem between snapshot and CDC records for the same key,
and materially more complexity in the ack/position model — all to avoid duplicates that at-least-once
already permits. It trades a simple, correct "replay from `P0` on restart" for a stateful buffering
subsystem. Rejected as speculative complexity (YAGNI) for no correctness gain over the chosen fix.

## Decision

Embed the captured CDC start position `P0` in every snapshot record's position, and seed the CDC
iterator from it on restart.

**Position type.** Add an optional field to `SnapshotPosition` (`common/position.go`):

```go
type SnapshotPosition struct {
    Snapshots SnapshotPositions `json:"snapshots,omitempty"`

    // CDCStart is the server-wide binlog position (P0) captured under the read
    // lock at snapshot start. It is stamped on every snapshot record so that a
    // mid-snapshot restart resumes CDC from P0 rather than a fresh master
    // position, closing the (P0, P1] data-loss window. It is nil for positions
    // written before this field existed and for steady-state CDC positions.
    CDCStart *CdcPosition `json:"cdc_start,omitempty"`
}
```

**Capture and thread-through.** In `newCombinedIterator`, after `obtainStartPosition()` succeeds
(`combined_iterator.go:109-115`), copy the captured `cdcIterator.position` into the snapshot iterator
before `snapshotIterator.start(ctx)` so `buildRecord` can stamp it. When `startCdcPosition != nil`
(restart with a known `P0`), stamp that same value. The stamping happens in
`snapshotIterator.buildRecord` (`snapshot_iterator.go:222-225`): set
`s.lastPosition.CDCStart = P0` once, then `ToSDKPosition()` carries it on every record.

**Critical ordering property.** `CDCStart` must be populated *before* the first snapshot record is
built, so that the very first record that advances `LastRead` already carries `P0`. This removes any
window in which `LastRead` is persisted without `cdc_start`. Because `P0` is captured under the lock
(before workers start) and `buildRecord` runs only after `start`, this ordering holds by
construction; it is asserted in code and covered by a test.

**Restart seeding.** In `Source.Open` (`source.go:158-172`), after parsing the position:

- `pos.CdcPosition != nil` -> steady-state CDC; pass `startCdcPosition = pos.CdcPosition` (unchanged).
- else if `pos.SnapshotPosition != nil && pos.SnapshotPosition.CDCStart != nil` -> mid-snapshot
  restart with a known `P0`; pass `startCdcPosition = pos.SnapshotPosition.CDCStart` **and**
  `startSnapshotPosition = pos.SnapshotPosition`. The combined iterator then skips
  `obtainStartPosition()` (the `startCdcPosition == nil` guard at `combined_iterator.go:109` is
  false) and seeds CDC from `P0`.
- else -> no persisted `P0` (fresh start, or a legacy position written by version N); fall back to
  today's behavior (`obtainStartPosition()` under the lock). Safe: see Failure modes.

Note the snapshot iterator is always reconstructed on `Open` (`combined_iterator.go:94-107`), even on
a `P0`-resume: the connector re-locks tables and re-runs `setupWorkers` to recompute per-table bounds
from the persisted `LastRead`. The `P0`-resume path differs only in skipping `obtainStartPosition()`
and seeding CDC from the persisted `P0`; the lock/worker-setup lifecycle is unchanged.

On restart from `P0`, CDC replays `(P0, restart]` from the binlog. Already-copied rows may receive
both a snapshot record and one or more CDC records; the resumed snapshot may also re-emit rows at the
chunk boundary. These are **duplicates**, explicitly acceptable under the at-least-once floor
(Invariant 3). Per-key ordering is preserved because CDC replays events in binlog order, and
downstream last-write-wins by key converges.

Invariant comment to add at the seeding site in `Source.Open` and at the stamping site in
`buildRecord`:

```go
// Invariant 3: persist the CDC start position (P0) on every snapshot record so a
// mid-snapshot restart resumes CDC from P0. Resuming from a fresh master position
// would silently drop writes to already-copied rows in (P0, P1].
```

**In-scope requirement — `Clone()` must copy `CDCStart`.** `SnapshotPosition.Clone()`
(`common/position.go:51-56`) today copies only `Snapshots`. It is used on the restart path:
`newSnapshotIterator` clones `startPosition` into `lastPosition` (`snapshot_iterator.go:95`) and
`setupWorkers` clones per worker (`:116`). If `Clone()` drops `CDCStart`, the resumed snapshot's
`lastPosition` loses `P0`, so records emitted during the *resumed* snapshot carry no `cdc_start` — and
a **second** mid-snapshot crash reintroduces the exact original bug. The design does **not** survive
on `buildRecord` field-assignment alone; `Clone()` must be updated to copy `CDCStart`, with a unit
test asserting the clone preserves it. This is required, not incidental.

**In-scope requirement — surface `canalRunErrC` at `ReadN`/startup (convert the stall into a
fail-stop).** As documented in Failure modes, an expired-binlog (or any early `RunFrom`) failure
currently stalls because the error is only drained in `Teardown`. Because persist-`P0` heightens
expired-binlog exposure on resume, this fix must also wire `canalRunErrC` into the read path:
`ReadN` (and/or a startup readiness check) must select on `canalRunErrC` and return a stable,
actionable error code instead of blocking forever. This turns silent-hang into loud fail-stop —
which is the desired behavior and makes the "fail-stop" framing actually true. Honest tradeoff:
persist-`P0` increases the odds of hitting a purged binlog on resume; that is mitigated by (a) this
clean fail-stop and (b) the observability signal below (resume-from-`P0` age vs. binlog retention),
so operators can size retention against expected downtime.

## Position format change and migration

This is a serialized-format change and is the Tier-1-critical part of this design.

**Shape of the change.** Additive: one new optional field, `cdc_start`, on `SnapshotPosition`, marked
`omitempty`. No existing field changes type, name, or meaning. No envelope version field is
introduced (the `Position` struct has none today); evolution follows the connector's existing
additive-JSON convention.

**Forward compatibility (N reads N+1's position).** A version-N connector deserializes an N+1
snapshot position with `encoding/json`; the unknown `cdc_start` key is ignored, and N behaves exactly
as it does today. No parse error, no crash.

**Backward compatibility (N+1 reads N's position).** An N-written snapshot position has no
`cdc_start`; `pos.SnapshotPosition.CDCStart` unmarshals to `nil`. N+1 falls back to
`obtainStartPosition()` — identical to current behavior. Upgrading a running connector is therefore
safe at the envelope level: it never fails to parse and never mis-seeds.

**The upgrade caveat (must be documented in release notes).** Upgrading N -> N+1 does **not**
retroactively fix a snapshot that was already in progress under N. The records N already emitted
carry no `cdc_start`; if the process is restarted onto N+1 while such a position is the latest
persisted one, N+1 sees `CDCStart == nil` and takes a fresh master position — the pre-fix window
still applies for that one already-in-flight snapshot. The fix is fully effective for any snapshot
that **starts** on N+1. Operators who want the guarantee for an in-progress initial sync should
restart the snapshot from scratch (clear the position) after upgrading. This is called out in the
changelog and the runbook.

**The downgrade hazard (unsafe, documented).** Downgrading N+1 -> N mid-snapshot drops the `cdc_start`
field (N cannot read it) and reintroduces the exact data-loss bug. This is no worse than running N
throughout, but it means **downgrade during an initial sync is unsafe**. Documented in release notes
with the standard remediation: do not downgrade mid-snapshot; if a downgrade is required, let the
snapshot complete and CDC begin first, or re-snapshot afterward.

**Upgrade/downgrade test (release gate).** Add a serialization compat test:

1. Marshal an N+1 snapshot position with `cdc_start` populated; unmarshal it with the N-shaped struct
   (no `CDCStart` field) and assert no error and that the snapshot fields are intact.
2. Marshal an N-shaped snapshot position (no `cdc_start`); unmarshal with N+1's struct and assert
   `CDCStart == nil` and that N+1's restart path falls back to `obtainStartPosition`.
3. Golden-file round-trip: a stored N position JSON and a stored N+1 position JSON both deserialize
   under N+1 to the expected `Position`, guarding against accidental field renames.

## Failure modes

**Crash before `P0` is persisted at all (before the first snapshot record is acked).** No position is
persisted (or the previous run's position, if any). On restart `pos` is `nil` (or CDC), so the
connector starts a **fresh** snapshot: new read lock, new `P0'`, all rows re-read from the beginning.
No row was persisted as "already copied", so there is no gap. Safe. This is why stamping `P0` on the
*first* record (before `LastRead` is ever persisted) is the load-bearing property.

**Crash mid-snapshot (the primary case).** Persisted position has `snapshots` advanced **and**
`cdc_start = P0`. Restart resumes each table from its `LastRead` and seeds CDC from `P0`. Writes in
`(P0, restart]` to already-copied rows are replayed by CDC. Fixed. Duplicates possible; acceptable.

**Crash after snapshot completes but before the first CDC record is acked.** The latest persisted
position is a snapshot position with all tables at `SnapshotEnd` and `cdc_start = P0`. On restart each
worker recomputes `w.end` from the current `MAX(pk)` (`fetch_worker.go:118-132`), so if rows with keys
above the original `SnapshotEnd` were inserted meanwhile, `Gt(LastRead)` re-reads them — harmless
duplicates, not "no records". The resumed snapshot then completes and CDC seeds from `P0`. Today this
same case takes a fresh `P1` and loses `(P0, P1]`; the fix closes it too.

**Empty tables (some).** `setupWorkers` skips empty tables (`snapshot_iterator.go:124-130`) but still
captures `P0` under the lock. Non-empty tables carry `cdc_start` on their records as normal. Safe.

**All tables empty / snapshot yields zero records.** With every table empty, `len(workers) == 0` and
`ReadN` returns `ErrSnapshotIteratorDone` immediately (`snapshot_iterator.go:149-152`) — no snapshot
record is ever emitted, so `cdc_start` is never persisted. `P0` still lives in memory for the running
process, so there is no loss unless the process is restarted after the empty-snapshot transition but
before the first CDC record is acked. That residual window is identical to the snapshot-disabled case
below and is the separate "CDC cold-start durability" concern — out of scope here, noted so it is not
mistaken for this bug.

**Snapshot disabled (`snapshot.enabled=false`).** No snapshot phase; the connector goes straight to
CDC and `getStartPosition` takes the current master position (`cdc_iterator.go:121-138`). There is no
snapshot gap because nothing was copied. There is still a pre-existing CDC-cold-start race (a crash
before the first CDC ack recaptures a fresh master), which this design does not address and does not
worsen.

**Multi-table.** `P0` is a single server-wide coordinate stored once at `SnapshotPosition.CDCStart`,
not per table. Each table resumes from its own `LastRead`; one CDC stream seeds from the single `P0`.
Correct because the binlog is global to the instance. `cdc_start` deliberately lives at the
`SnapshotPosition` level, not inside `TablePosition`.

**`P1` rotation / binlog file rotation between runs.** Irrelevant to correctness once we seed from
`P0`: `canal.RunFrom(P0)` follows rotation forward. `P1` is no longer read on the mid-snapshot path.

**`P0`'s binlog purged/expired by restart time.** If the crash-to-restart gap exceeds binlog
retention, `P0`'s file is gone and `canal.RunFrom(P0)` fails. **Today this is not a clean failure: it
stalls.** `start()` launches `canal.RunFrom` in a goroutine and returns `nil` regardless
(`cdc_iterator.go:104-118`); the `RunFrom` error is sent to `canalRunErrC`, which is drained **only**
in `Teardown` (`:115`, `:185`). `ReadN` then blocks indefinitely on `parsedRecordsC`/`canalDoneC`
(`:144-156`) — the connector hangs with no error surfaced. **Persist-`P0` makes this worse, not
neutral:** the pre-fix path always resumed from a *fresh, current* master position, so an expired
binlog was nearly impossible on resume; seeding from a persisted `P0` that may be hours older
materially increases the chance of requesting a purged binlog. This is a real new exposure, not
"strictly better than today", and it must be handled in scope — see the Decision (error surfacing)
and Observability (resume-age vs. retention signal). Remediation: increase
`binlog_expire_logs_seconds` / retention, or clear the position to re-snapshot.

## Observability

- Structured log at snapshot start recording the captured `P0` (`{name, pos}`) and whether it was
  freshly captured or seeded from a persisted `cdc_start`. This makes the handoff auditable.
- Log line at restart in `Source.Open` stating which branch was taken: steady-state CDC, mid-snapshot
  resume from persisted `P0`, or legacy/no-`P0` fallback.
- A distinct, stable error code for "cannot resume CDC: start binlog no longer available",
  surfaced through `ReadN`/startup by draining `canalRunErrC` on the read path (see Decision), with
  the failing binlog file, the requested `P0`, and the suggested fix (increase retention or
  re-snapshot). This is what converts today's silent stall into a loud fail-stop. Errors are API
  (CLAUDE.md): machine-actionable for agents.
- Resume-exposure signal: on a resume-from-`P0`, log/emit the age of `P0` (time or binlog distance
  from the current master position) so operators can compare it against binlog retention and size
  retention against expected downtime. This is the primary mitigation for the heightened
  expired-binlog exposure that persist-`P0` introduces.
- Counter/metric for records replayed on resume (duplicate volume) so operators can see the cost of a
  restart, and a gauge/log of the `(P0, restart]` binlog distance.

## Regression tests

The bug survived because every existing integration test tears down **gracefully**
(`combined_iterator_integration_test.go:44`, `is.NoErr(iterator.Teardown(ctx))`), which preserves the
in-memory `P0`. A test that would have caught this must simulate a hard crash — an abrupt process
kill that abandons in-memory state — between persisting snapshot progress and completing CDC.

**Primary test: SIGKILL mid-snapshot with concurrent writes.**

1. Create a table and insert enough rows that the snapshot spans multiple fetch chunks (fetch size
   set small so `LastRead` advances well before completion).
2. Start the source; read and **ack** snapshot records for the first chunk only, so a snapshot
   position with an advanced `LastRead` (and, post-fix, `cdc_start`) is persisted.
3. While the snapshot is paused mid-way, issue an `UPDATE` and a `DELETE` against rows already copied
   (primary key `<= LastRead`), plus a low-key `INSERT` with `pk <= LastRead`. The table must use an
   **explicit, non-auto-increment primary key** so a low-key `INSERT` is possible (auto-increment
   keys cannot go backward, which would make this step unreproducible). These land in the binlog
   window that the bug drops.
4. **Hard-kill** the connector process (SIGKILL / abandon the iterator without `Teardown`) so `P0` in
   memory is lost. This requires a new test helper that runs the source in a child process (or an
   equivalent in-process harness that discards iterator state without calling `Teardown`), because
   the current suite only has graceful-teardown helpers.
5. Restart the source from the persisted position. Read to completion (resumed snapshot, then CDC).
6. Assert the `UPDATE`, `DELETE`, and low-key `INSERT` are all delivered (as CDC records replayed from
   `P0`). Pre-fix, they are absent — the test fails, proving it catches the bug. Post-fix, they are
   present (duplicates of the snapshot copy are allowed; assert presence, not exact-once).

**Supporting tests.**

- **Double-crash test (guards the `Clone()` fix).** Start, ack first chunk, SIGKILL, restart; then
  during the **resumed** snapshot ack another chunk and SIGKILL again; issue an `UPDATE`/`DELETE` to
  already-copied rows in the second window; restart a third time and read to completion. Assert the
  second-window writes are delivered. This fails if `Clone()` drops `CDCStart` (resumed-snapshot
  records would carry no `cdc_start`), which the single-crash test does not catch.
- `Clone()` unit test: clone a `SnapshotPosition` with `CDCStart` set and assert the clone preserves
  it (guards against a future edit re-dropping the field).
- Ordering property: assert `cdc_start` is present on the **first** emitted snapshot record, not just
  later ones (guards the load-bearing "persist `P0` before `LastRead`" invariant).
- Crash after snapshot completion, before first CDC ack: kill at the handoff, restart, assert
  `(P0, restart]` writes are delivered.
- Serialization upgrade/downgrade tests from the migration section.
- All-empty-tables and snapshot-disabled cases: assert no panic and documented behavior (no false
  guarantee claimed).

**Harness note.** The suite needs a real process-kill helper (child-process runner or a
teardown-skipping wrapper). This is the reusable piece the chaos requirement (CLAUDE.md `tests/chaos`)
will build on; it is introduced here scoped to this bug.

## Rollout

- Land as a Tier-1 fix with the primary SIGKILL regression test verified to fail without the code
  change and pass with it.
- Changelog and README: document the additive `cdc_start` position field, the upgrade caveat
  (in-progress snapshots are not retroactively fixed), and the downgrade hazard (unsafe mid-snapshot).
- Operations runbook entry: symptom (missing/stale/phantom rows after an initial-sync restart) ->
  diagnosis (pre-fix version, or a downgrade/in-progress-upgrade) -> remediation (upgrade and
  re-snapshot; ensure binlog retention exceeds expected downtime).
- No `conduit-connector-protocol` change; no coordinated multi-repo release required.

## Related

- Postmortem for the sibling ack/position class of bug in core Conduit:
  `docs/postmortems/20260723-source-ack-persist-ordering.md` (in `ConduitIO/conduit`) — same failure
  family (a position/ack durability gap on the restart path) in a different connector/engine surface.
- Found by the WS5 adversarial verification pass (initial-sync restart correctness review of the
  MySQL source).
- Data-integrity Invariant 3 (at-least-once, including restart, error, and shutdown paths),
  `ConduitIO/conduit` `CLAUDE.md`.
