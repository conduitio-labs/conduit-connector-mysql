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

This fix **fully closes** the mid-snapshot bug (#180). It deliberately does **not** attempt the
related **CDC cold-start** gap (empty snapshot or `snapshot.enabled=false`), which is a documented,
scoped-out known gap: the only mechanism available in this connector — emitting a synthetic
checkpoint record — leaks a MySQL-source-specific, malformed record into broker-neutral pipelines
and breaks destinations, so it is not worth doing for a mechanism that could only *mitigate* (never
close) the gap. The clean fix is a position-only checkpoint in the SDK
(`ConduitIO/conduit-connector-sdk#378`), which persists a position with zero stream pollution;
cold-start closure is deferred to it. See the Cold-start durability section.

A hard `Version` field is also added to the `Position` envelope: a position whose version exceeds the
connector's max-known version is refused with a stable error code, making a future downgrade
detectable rather than silently mis-read (forward-looking from this release onward).

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

- Guarantee at-least-once delivery across a mid-snapshot restart (uphold Invariant 3). This **fully
  closes** the mid-snapshot bug (#180).
- Persist `P0` durably for the entire snapshot phase so restart resumes CDC from `P0`.
- Keep the position format readable by version N and N+1 (additive, backward compatible on upgrade).
- **Add a `Position` envelope version field** so a future higher-versioned position is detected and
  refused on downgrade rather than silently mis-read.
- Preserve `P0` through position `Clone()` so the *resumed* snapshot keeps stamping it (no
  second-crash regression).
- Convert the expired-binlog resume from a silent stall into a loud fail-stop with a stable error
  code, since persist-`P0` heightens expired-binlog exposure on resume.
- Ship the regression tests that would have caught this: SIGKILL-mid-snapshot and double-crash.

**Non-goals**

- **Closing the CDC cold-start durability gap** (empty snapshot and `snapshot.enabled=false`). This
  is a documented known gap, deferred to the SDK position-only-checkpoint primitive
  (`ConduitIO/conduit-connector-sdk#378`). The only in-connector mechanism — a synthetic checkpoint
  record — breaks broker-neutral destinations and is rejected (see Cold-start durability).
- Making the snapshot transactional / removing the reliance on CDC replay (see Alternative A).
- Eliminating duplicate delivery on resume — duplicates are acceptable under at-least-once and are
  the safe trade against a gap.
- Any change to `conduit-connector-protocol` or the opencdc record shape.

## Constraints

- MySQL `GetMasterPos()` returns a single server-wide binlog coordinate; there is no per-table
  binlog position. `P0` is global to the instance.
- MySQL binlog replication has **no server-side cursor/slot**. Unlike Postgres logical replication —
  whose replication slot persists `confirmed_flush_lsn` server-side, so the server anchors the
  resume position and there is no cold-start gap — the MySQL connector is *solely* responsible for
  durably persisting its binlog position. This is the structural reason the cold-start gap exists
  here and not in the Postgres connector.
- Conduit persists the position of **acked** records only. The SDK `Source` interface is
  `Open`/`ReadN`/`Ack`/`Teardown` (SDK v0.14.1 `source.go:45-100`) — there is **no** position-only
  checkpoint, heartbeat, or side channel. To persist any position (including `P0`), a record
  carrying it must be emitted and acked. This is why the cold-start gap cannot be closed cleanly in
  the connector today and is deferred to SDK #378 (see Cold-start durability).
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

**Position type.** Add an envelope `Version` field to `Position` and an optional `CDCStart` field to
`SnapshotPosition` (`common/position.go`):

```go
// CurrentPositionVersion is the version stamped on every position this connector
// writes. Bump it whenever a position-format change is not safely readable by the
// previous reader. Absent/0 on read means a legacy pre-version position (treat as v1).
const CurrentPositionVersion = 1

type Position struct {
    // Version is the position-envelope format version. 0/absent = legacy. A reader
    // refuses any Version greater than CurrentPositionVersion (see restart seeding).
    Version          int               `json:"version,omitempty"`
    SnapshotPosition *SnapshotPosition `json:"snapshot_position,omitempty"`
    CdcPosition      *CdcPosition      `json:"cdc_position,omitempty"`
}

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

**Writers stamp the version.** Both `SnapshotPosition.ToSDKPosition` and `CdcPosition.ToSDKPosition`
(`common/position.go:42-49`, `:112-119`) set `Version = CurrentPositionVersion` on the `Position`
they marshal, so every position this connector writes is version-stamped. `cdc_start` stays
`omitempty`, so the wire size for CDC positions is unchanged.

**Version gate on read.** `ParseSDKPosition` (`common/position.go:58-64`) is the single parse
chokepoint. After unmarshalling, it enforces the version:

- `Version == 0` (absent) -> legacy pre-version position; proceed on the legacy path (a snapshot
  position without `cdc_start` falls back to `obtainStartPosition`, as today).
- `1 <= Version <= CurrentPositionVersion` -> parse normally.
- `Version > CurrentPositionVersion` -> **refuse** with a stable error code
  (`ErrPositionVersionUnsupported`), reporting the found and max-known versions and the fix (upgrade
  the connector). Do not silently proceed. This is what makes a *future* downgrade detectable.

Honest scope of the version field: it cannot catch the **current** N -> N+1 downgrade, because
version-N connectors predate the field and simply ignore the unknown `version` key. Its value is
forward-looking: from this version-aware release (call it N+1) onward, if a later release N+2 writes
`Version = 2` and the pipeline is rolled back to N+1, N+1 refuses the N+2 position instead of
mis-reading it. The benefit begins at N+1; it is not retroactive. Stated plainly so it is not
overclaimed.

**Restart seeding.** In `Source.Open` (`source.go:158-172`), after `ParseSDKPosition` (which has
already applied the version gate above):

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

### CDC cold-start durability (empty snapshot / `snapshot.enabled=false`)

**This is a scoped-out known gap.** This fix does **not** attempt to close it. The mid-snapshot bug
is fully closed; cold-start durability is deferred to the SDK (`ConduitIO/conduit-connector-sdk#378`).
The rest of this section states the gap honestly and explains why no in-connector mechanism is worth
shipping.

**The gap.** When there is no snapshot phase to carry `P0` — every table empty, or
`snapshot.enabled=false` — the connector captures `P0` (`getStartPosition` -> current master,
`cdc_iterator.go:121-138`) but persists nothing until the first CDC record is acked. If events occur
in `(P0, restart]` and the process crashes before that first ack, the persisted position is `nil`,
restart takes a fresh master `P1 > P0`, and the intervening events are lost. (If the DB is idle so no
events occur, there is nothing to lose — the gap only bites when events happened but were not acked.)

**Why cold-start cannot borrow the mid-snapshot safety net.** The mid-snapshot fix tolerates a crash
before the first ack because a `nil` position triggers a *full re-snapshot* that re-anchors a fresh
`P0'` and re-reads every row — nothing is lost. That re-read safety net is why the primary
mid-snapshot bug (#180) is **fully closed**. CDC has no equivalent "re-read from scratch": a fresh
master **skips** binlog history and cannot recover it. So cold-start would need `P0` durable *before*
the read advances past `P0` — but making `P0` durable requires a record ack (see below), and a crash
can always land before that first ack.

**Why the SDK gives no clean mechanism.** SDK v0.14.1's `Source` interface is
`Open`/`ReadN`/`Ack`/`Teardown` (`source.go:45-100`) — there is no position-only checkpoint,
heartbeat, or open-with-position-write hook, and none in the source middleware. The **only** way to
persist a position is to emit a record and have it acked. (Contrast Postgres, whose server-side
replication slot anchors the LSN and needs no such record — see Constraints.)

**Why the only available in-connector mechanism is rejected.** The single mechanism the SDK permits —
emitting a synthetic "checkpoint" record carrying `P0` at cold-start — is **downstream-visible and
actively breaks destinations**, so it is not shipped:

- The synthetic record has no `collection` metadata and a non-JSON key, so the **MySQL destination's**
  `batchRecords` / `GetCollection` errors with `ErrMetadataFieldNotFound` on any
  cold-start-source -> destination pipeline. It breaks the very connector family it lives in.
- Worse, it is a **MySQL-source-specific record leaking into a broker-neutral pipeline**. A cold-start
  MySQL source -> Postgres / S3 / Kafka destination would ship the same malformed, source-specific
  record to destinations that know nothing about it. Patching every destination to filter a
  source-specific metadata key is untenable — especially for a mechanism that could only *mitigate*
  the gap (a crash before the checkpoint's own ack still loses `(P0, restart]`), never close it.

**Deferred to SDK #378 (the clean fix).** The gap closes properly once the SDK exposes a
**position-only checkpoint** — persist a position without emitting a downstream record — tracked as
`ConduitIO/conduit-connector-sdk#378`. That has zero stream pollution and is broker-neutral. Until it
lands, cold-start is a documented known gap: a crash-before-first-CDC-ack on an empty-snapshot or
`snapshot.enabled=false` source can lose events in `(P0, restart]`. This is noted in the README /
runbook so operators can weigh it (e.g. prefer running an initial snapshot, which is fully covered).

## Position format change and migration

This is a serialized-format change and is the Tier-1-critical part of this design.

**Shape of the change.** Two additive fields: an envelope `version` on `Position` and an optional
`cdc_start` on `SnapshotPosition`, both `omitempty`. No existing field changes type, name, or
meaning. The current writer stamps `version = CurrentPositionVersion` (= 1) on every position.

**Forward compatibility (N reads N+1's position).** A version-N connector deserializes an N+1
position with `encoding/json`; the unknown `cdc_start` and `version` keys are ignored, and N behaves
exactly as it does today. No parse error, no crash. (N has no version gate, so it cannot *refuse* —
see the honest scope note in Decision.)

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

1. Marshal an N+1 snapshot position with `cdc_start` + `version` populated; unmarshal it with the
   N-shaped struct (no `CDCStart`/`Version` fields) and assert no error and that the snapshot fields
   are intact (proves N tolerates the additive fields).
2. Marshal an N-shaped snapshot position (no `cdc_start`, no `version`); unmarshal with N+1's struct
   and assert `Version == 0` (legacy), `CDCStart == nil`, and that N+1's restart path falls back to
   `obtainStartPosition`.
3. **Version-refusal path:** hand `ParseSDKPosition` a position with `version =
   CurrentPositionVersion + 1` and assert it returns `ErrPositionVersionUnsupported` (not a silent
   parse). This is the downgrade-detection guarantee.
4. Golden-file round-trip: a stored N position JSON and a stored N+1 position JSON both deserialize
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
record carries `cdc_start`. This is a **CDC cold-start** and a **documented known gap, NOT mitigated**
by this fix: a crash before the first CDC record is acked loses events in `(P0, restart]` (see
Cold-start durability). Unlike mid-snapshot there is no re-read safety net, and the only in-connector
mechanism (a synthetic record) breaks broker-neutral destinations. Deferred to SDK #378.

**Snapshot disabled (`snapshot.enabled=false`).** No snapshot phase; the connector goes straight to
CDC (`getStartPosition` -> current master, `cdc_iterator.go:121-138`). Same **CDC cold-start known
gap** as above — not addressed here, deferred to SDK #378. Only the primary mid-snapshot case is
fully closed.

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
- Expired-binlog fail-stop test: seed a persisted `cdc_start = P0` whose binlog file is no longer
  available, restart, and assert `ReadN`/startup returns the stable error code instead of hanging
  (guards the `canalRunErrC` surfacing; distinguish the benign `replication.ErrSyncClosed` on
  teardown from a real `RunFrom` failure).
- Serialization upgrade/downgrade tests from the migration section (including the version-refusal
  path: `version = CurrentPositionVersion + 1` -> `ErrPositionVersionUnsupported`).

**Harness note.** The suite needs a real process-kill helper (child-process runner or a
teardown-skipping wrapper). This is the reusable piece the chaos requirement (CLAUDE.md `tests/chaos`)
will build on; it is introduced here scoped to this bug.

## Rollout

- Land as a Tier-1 fix with the primary SIGKILL regression test verified to fail without the code
  change and pass with it.
- Changelog and README: document the additive `version` + `cdc_start` position fields, the upgrade
  caveat (in-progress snapshots are not retroactively fixed), the downgrade hazard (unsafe
  mid-snapshot), and the **CDC cold-start known gap** (empty snapshot / `snapshot.enabled=false` can
  lose `(P0, restart]` on a crash before the first CDC ack; deferred to SDK #378 — prefer running an
  initial snapshot, which is fully covered).
- Operations runbook entry: symptom (missing/stale/phantom rows after an initial-sync restart) ->
  diagnosis (pre-fix version, or a downgrade/in-progress-upgrade, or the cold-start known gap) ->
  remediation (upgrade and re-snapshot; ensure binlog retention exceeds expected downtime).
- No `conduit-connector-protocol` change and no opencdc record-shape change; no coordinated
  multi-repo release required.

## Related

- Postmortem for the sibling ack/position class of bug in core Conduit:
  `docs/postmortems/20260723-source-ack-persist-ordering.md` (in `ConduitIO/conduit`) — same failure
  family (a position/ack durability gap on the restart path) in a different connector/engine surface.
- Found by the WS5 adversarial verification pass (initial-sync restart correctness review of the
  MySQL source).
- Data-integrity Invariant 3 (at-least-once, including restart, error, and shutdown paths),
  `ConduitIO/conduit` `CLAUDE.md`.

## Open questions

None open. The cold-start durability gap is scoped out of this fix and tracked in the SDK as
`ConduitIO/conduit-connector-sdk#378` (position-only checkpoint). Earlier cold-start questions
(destination-down blocking, checkpoint opt-out) are moot — the synthetic-checkpoint mechanism they
concerned has been dropped (see Decision log).

## Decision log

- **2026-07-24 — DeVaris sign-off with two amendments.** (1) Fold the CDC cold-start durability gap
  into this fix (no longer a separate ticket / non-goal): chosen mechanism is a synthetic checkpoint
  record carrying `P0`, gated on its own ack, with the record's downstream visibility flagged as an
  accepted tradeoff. (2) Add a hard `Position.Version` envelope field now, with a read-path refusal
  of any version above the connector's max-known version (forward-looking downgrade detection).
  Both are Tier-1 position-format surface and are reflected in the Decision, Migration, Failure
  modes, and Regression tests sections above.
- **2026-07-24 — cold-start tradeoff resolutions.** After seeing the concrete mechanism: (a) the
  synthetic checkpoint record is accepted, on by default, with no opt-out config (destinations filter
  on `mysql.checkpoint=true`); (b) on destination-down at cold-start the connector blocks
  indefinitely with a loud periodic log, no configurable timeout (a timeout would reopen the loss
  window). Design is fully signed off for implementation.
- **2026-07-24 — cold-start is a MITIGATION, not a full fix (implementation feedback).**
  Implementation surfaced that the checkpoint gate cannot make the cold-start window zero: durability
  needs a record ack, and a crash before the first checkpoint ack still loses events in `(P0, P0'']`.
  DeVaris chose to **keep** the checkpoint as a mitigation (not drop it), accepting the documented
  irreducible residual, because it strictly improves on pre-fix ("read nothing until `P0` durable"
  vs. "read then lose"). The primary mid-snapshot bug (#180) remains **fully closed** (re-snapshot
  safety net). An SDK position-only-checkpoint feature request is filed as the tracked path to zero
  cold-start exposure: ConduitIO/conduit-connector-sdk#378. All
  cold-start framing in this doc says "mitigated, not eliminated" to match.
- **2026-07-25 — cold-start mechanism DROPPED entirely (supersedes the three entries above).**
  DeVaris dropped the synthetic-checkpoint mechanism. Rationale: the checkpoint record is
  downstream-visible *and actively breaks destinations*. It has no `collection` metadata and a
  non-JSON key, so the MySQL destination's `batchRecords` / `GetCollection` errors with
  `ErrMetadataFieldNotFound` on any cold-start-source -> destination pipeline; worse, it is a
  MySQL-source-specific record leaking into broker-neutral pipelines (a cold-start MySQL source ->
  Postgres/S3/Kafka destination would ship the same malformed record to destinations that know
  nothing about it). Patching every destination to filter a source-specific key is untenable for a
  mechanism that only *mitigates* (never closes) the gap. **Decision:** drop it; cold-start is now a
  documented known gap (an explicit non-goal), closed properly later via the SDK position-only
  checkpoint (`ConduitIO/conduit-connector-sdk#378`), which persists a position with zero stream
  pollution. This doc's mid-snapshot fix (#180, fully closed) and the `Version` field are unchanged;
  all cold-start content is reframed as a deferred known gap with no in-connector mechanism.
