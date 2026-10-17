# Debezium-compatible output for MySQL CDC

## Summary

A team that runs Debezium MySQL on Kafka Connect and moves to Conduit should not have to touch a
consumer. This design adds an opt-in Debezium output mode to the MySQL source. The mode reproduces
topic names, the key and value structure (the Debezium envelope), the `source` block
(`io.debezium.connector.mysql.Source`), headers, Schema Registry subjects and schemas, tombstones,
and the Debezium encodings for decimals, time types, `BIT`, `JSON`, enums and `BIGINT UNSIGNED`.
The default OpenCDC output does not change.

The mode is split across the same three places as the Postgres mode
(`docs/design-documents/20261012-debezium-compatible-output.md` in ConduitIO/conduit), each doing
the one thing it has the information to do:

1. **MySQL source** (`debezium.*` settings, connector v0.5.0): decodes binlog and snapshot rows into
   Debezium-typed values and emits **exactly one record per binlog row event**, carrying a
   Connect-typed schema descriptor (in-record, never in the consumers' registry), the `source`
   block as metadata, and the Debezium topic name. Only the source can do this without losing
   information (column type, scale, nullability, `UNSIGNED`, `binlog_row_image=FULL` before images).
2. **`debezium.encode` builtin processor** (Conduit core, PR #3183): builds the envelope,
   serializes key and value with the Avro (Confluent wire format, Schema Registry) or JSON
   converter, applies subject naming, and expands one record into the **message plan** Debezium
   would have produced for that row change: one message for an insert, a delete plus a tombstone,
   delete + tombstone + create for a primary-key change. It runs last, so processors placed before
   it still see structured data.
3. **Kafka destination** (Conduit core): writes a record's message plan as one unit (all messages
   produced with per-message results tracked by index, the record acknowledged only when every
   message succeeded).

`conduit migrate kafka-connect` reads the Debezium and converter settings from the Kafka Connect
config and sets all three. The reference for "correct" is real Debezium, pinned by version, in the
same diff harness as the Postgres lane (`tests/debezium`). This document does not try to
re-document Debezium by hand; behavior stated here was read from Debezium's MySQL type-mapping
documentation and source, and items marked **(V)** remain open and the harness decides them. The
implementation follows the harness, not this text.

Risk tier: **Tier 1** for the implementation (serialization formats, schema handling, the source
record-shaping on the data path). This document is Tier 3 (docs). DeVaris sign-off and a
fresh-context review are required before any implementation PR opens.

Roadmap item: v0.21: MySQL CDC certification, issue #3005. The **type** half of this work
(`types.mode=strict`, NULL/DECIMAL/BIGINT UNSIGNED) ships in v0.21 under the type-semantics design
(ConduitIO/conduit PR #3040) and is the v0.21 certification gate for the record-schema contract.
The **envelope/format** half this document specifies is the v0.22 piece; until it ships, the
migration harness `debezium-mysql` family keeps its blocking `kc.output_shape_differs` note.

## Context

### What exists (inventory)

| Asset | Where | State | Use here |
| --- | --- | --- | --- |
| MySQL source v0.4.0 | this repo | Emits default OpenCDC records: operation `create`/`update`/`delete`/`snapshot`, key = PK `StructuredData` (or a synthetic `"<file>_<pos>"` string for keyless tables), `Payload.Before`/`After` = `StructuredData` of column name → value. Metadata: `opencdc.collection` (bare table name), `opencdc.createdAt`, `mysql.server.id` (CDC only), and `opencdc.*Schema.subject`/`.version`. Values are Avro-formatted through `schema.go`. | Source of truth for what changes. |
| Avro schema mapper | `schema.go` | Maps MySQL types to Avro and formats values. Known lossy: `NULL` → zero value, `DECIMAL`/`NUMERIC` → `double`, `BIGINT UNSIGNED` above 2^63-1 → passes through unconverted, `DATE`/`TIME`/`DATETIME`/`TIMESTAMP` → string, `BIT` → `fixed[8]`. | **Not reused** by the Debezium mode. The Debezium mode decodes to a lossless typed form and maps to Connect types. |
| CDC iterator | `cdc_iterator.go` | canal-based. One record per binlog row, `before`/`after` from the FULL row image, positions `cdc_position` with `name`/`pos`/`idx`/`prev`/`replay_from`/`compressed`/`table_coverage`. | Position, key derivation, and resume logic are reused; only the value typing and metadata change. |
| Snapshot iterator | `snapshot_iterator.go`, `fetch_worker.go` | Key-ordered snapshot, one record per row, `snapshot_position` with per-table type-exact keys. | Reused; snapshot rows become `op: r`. |
| Position envelopes | `common/position.go`, `common/keyvalue.go` | Versioned `snapshot_position` (v2) and `cdc_position` (v1) envelopes; type-exact snapshot keys. | Unchanged by the mode (see _Versioning_). |
| `debezium.encode` processor | ConduitIO/conduit PR #3183 | Postgres envelope/tombstone/naming/serialization. Takes `debezium.compat`, builds `<topic>.Envelope`, expands to message plans. | Reused, parameterized for the MySQL `source` block. |
| `unwrap.debezium` processor | ConduitIO/conduit | Decodes a Debezium event into an OpenCDC record. Opposite direction. | Reference for field names only. |
| Migration harness `debezium-mysql` family | `tests/migrate/corpus/debezium-mysql/` | Four offline cases (`dbz-mysql-tutorial`, `-avro`, `-config-provider`, `-regexrouter`) over `io.debezium.connector.mysql.MySqlConnector`, all carrying the blocking `kc.output_shape_differs` note and `topic.prefix` marked `approximated, blocking: true`. | What this mode removes: the blocking note and the `topic.prefix` approximation. |

### What the connector already guarantees (v0.4.0)

Read from the README and code; these are the invariants the mode must not weaken.

- **At-least-once**, positions carried on records and persisted by the engine (invariants 1–3).
- **Snapshot→CDC handoff** via a durable `P0` binlog position captured under the read lock
  (`docs/design-documents/20260724-snapshot-cdc-position-handoff.md`).
- **CDC resume** in the middle of a multi-row binlog event, inside a compressed transaction, and
  across file rotations (`docs/design-documents/20261015-cdc-resume-inside-binlog-events.md`).
- **`binlog_format=ROW` and `binlog_row_image=FULL` are required**; a skipped-column row image halts
  with `mysql.cdc.row_image_skipped_columns` (invariant 6). `binlog_row_value_options=PARTIAL_JSON`
  halts with `mysql.cdc.partial_json_unsupported`.
- **Row/column mismatch halts** with `mysql.cdc.row_columns_mismatch`; the connector re-reads the
  live table columns once on a mismatch (`refreshStaleTable`) and halts if they still do not fit,
  rather than mis-mapping values into the wrong columns.

### Findings from reading the code (the gaps the mode fills)

1. **No Debezium envelope.** The source emits default OpenCDC records. A Debezium consumer expects
   `<prefix>.<db>.<table>.Envelope` with `before`/`after`/`op`/`source`/`ts_ms`. The migration
   harness records this as `kc.output_shape_differs`.
2. **`opencdc.collection` is the bare table name.** Debezium topics include the database name
   (`<prefix>.<db>.<table>`), and two databases can each hold a table of the same name. The mode
   needs `db` and the topic prefix.
3. **Lossy value typing.** `NULL` becomes the column's zero value, `DECIMAL` becomes `float64`,
   `BIGINT UNSIGNED` overflows the Avro `long`. Debezium carries `NULL` and `DECIMAL` exactly
   (string or `Decimal` logical type) and `BIGINT UNSIGNED` as `INT64`/`Decimal(20,0)`/string.
   The type-semantics design (#3040) fixes the lossy forms for the default output; the Debezium mode
   needs the same lossless decode with Connect logical types on top.
4. **No `source` block, no `ts_ms`, no tombstone, no PK-change expansion.** These are Debezium
   semantics the envelope produces; the source must supply the metadata and the encoder expands.
5. **DDL is not parsed into a schema history.** Debezium MySQL stores a schema history (in Kafka)
   and emits schema-change events to a `<prefix>` topic. This connector has no schema history: it
   re-reads the live table schema on a mismatch and halts on a count or type-family change. The mode
   does not add a schema-history store; this is the largest intentional difference (see D-list and
   _Schema changes_).
6. **No GTID, no heartbeat.** Debezium MySQL fills `source.gtid` and emits heartbeat topics. This
   connector uses file/offset positions and no heartbeat (README "Limits"). `source.gtid` is null in
   this mode and the heartbeat topic is not produced.

## Constraints

- Invariants 1 to 4 and 6 apply (CLAUDE.md). In particular: the source emits no more than one record
  per binlog row event (its flush gate and `cdc_position` `idx` assume it, invariant 2); a record is
  acked only when every message derived from it is durably produced (invariant 1); nothing is
  dropped, coerced or truncated silently (invariants 3 and 6).
- The default output, its positions and its metadata do not change (see _Default-output isolation_).
- Byte compatibility is the goal for keys, values, schemas and headers; where bytes cannot match, a
  documented difference with a comparison rule.
- Serialized formats are public contracts: versioned, with an upgrade path and test.
- No new dependency without justification. No performance claim without a committed benchi result.
- The Kafka destination is broker-neutral and database-neutral; MySQL semantics stay out of it.

## Goals

- A consumer written for Debezium MySQL events keeps working with no change: same topics, same keys
  (including partitioning), same envelope, same subjects, same schema evolution behavior, same
  tombstones.
- Where Conduit cannot match, the difference is listed (see _Intentional differences_), reported by
  `migrate`, and tested. No silent coercion or truncation (invariant 6).
- The default output and its positions are unchanged.

## Non-goals

- Other databases (the Postgres mode is `docs/design-documents/20261012-debezium-compatible-output.md`).
- Debezium's **schema history** (`schema.history.internal.kafka.*`) and the schema-change topic. The
  connector keeps its re-read-and-halt approach; `migrate` reports these keys unsupported
  (non-blocking, because the live-schema re-read already covers the value-mapping concern — see
  _Schema changes_).
- Debezium SMTs and Debezium-specific processors that reshape the envelope (`ExtractNewRecordState`,
  `ReplaceField`, outbox `EventRouter`): the SMT pack owns those; `migrate` reports them blocking
  when combined with the envelope output.
- GTID-based positions, failover resume, heartbeats (existing "No GTID support" / "No heartbeat"
  limits; they become D-list entries, not new work).
- Protobuf, JSON Schema and Apicurio converters. Avro (Confluent) and JsonConverter only.
- Incremental snapshots (`signal.*`, `snapshot.mode=incremental`).
- A configurable Debezium emulation matrix beyond the targets below.
- Performance claims; the cost of the mode is measured with a committed benchi config, no number
  appears here.

## Where the mode lives

### Decision

The same split as Postgres: source-side typed output (one record per binlog row event), a trailing
`debezium.encode` processor that expands a record into its message plan, and a Kafka destination
that writes a plan as one acked unit. The `debezium.encode` processor becomes family-parameterized:
it already takes a `debezium.compat` and reproduces the source schema from the record's
`debezium.*` metadata and descriptor; for MySQL it produces the `io.debezium.connector.mysql.Source`
block (connector `mysql`) and the MySQL compat boundaries, reusing the envelope, tombstone,
PK-change and naming machinery unchanged.

Two sentences: the MySQL source emits Debezium-typed records because only it can decode exactly;
the `debezium.encode` processor turns each into the converter bytes and the message plan Debezium
would have produced, and the Kafka destination writes the plan and acks the record only when all of
it is durable.

The seam earns its place for the same reasons as Postgres (it is the OpenCDC record with a
documented content, not a new interface), and it has two real producers: Postgres (v0.21) and MySQL
(v0.22).

### Alternatives and why each loses

The five alternatives from the Postgres design (processor-only over the unchanged OpenCDC output;
source-emits-finished-bytes; format on the Kafka destination; run real Debezium through the
`conduit-kafka-connect-wrapper`; fan-out in the engine) lose for the identical reasons and are not
repeated here. One MySQL-specific alternative is added:

**F. Emit the envelope directly from the source (skip `debezium.encode`).** The simplest flow for
MySQL-only, and it loses on composition and reuse: bytes are opaque to processors (masking,
filtering, routing by metadata stop working between decode and encode), it puts Schema Registry
client code in the source, and it duplicates the envelope/tombstone/naming/serialization machinery
that `debezium.encode` already owns for Postgres. Rejected for the same reason as Postgres
alternative B, and because the second producer of the seam is committed (v0.22).

### One binlog row change, several messages

Debezium emits more than one message for a single row change in three cases, and the mode
reproduces all of them exactly as the Postgres design specifies (the mechanism is the encoder's
message plan, the source never exceeds one record per change):

| Binlog row change | Debezium messages |
| --- | --- |
| `DELETE` with `tombstones.on.delete=true` | delete event, then a tombstone (same key, null value) |
| `UPDATE` that changes the primary key | delete event (header `__debezium.newkey`), then a tombstone copying that header, then a create event (header `__debezium.oldkey`) |
| (no truncate/message equivalents for MySQL) | — |

MySQL has no `TRUNCATE` binlog row events in the way Postgres has a truncate WAL record (a MySQL
`TRUNCATE` is a DDL `QueryEvent`, which this connector does not decode; see _Schema changes_), and no
logical-decoding message primitive. So the plan has at most three messages (the PK-change case), and
the `op` values the mode produces are `c`, `u`, `d`, `r` only.

**PK-change detection.** The connector requires `binlog_row_image=FULL`, so an UPDATE carries the
full before image including the old primary key. Detection mirrors Debezium: the update is a
primary-key change only when an old key exists **and** differs from the new key (compared column by
column, typed, in key order); an equal or absent old key is a plain update. Because the image is
FULL, the old key is always available for a key-changing update, so the `REPLICA IDENTITY`
before-image ambiguity that Postgres has does not exist here. The one residual case is a keyless
table (no primary key): Debezium keys keyless-table events on `message.key.columns` or a null key;
this connector uses a synthetic position key today (see _Key_).

## Design

### Settings

MySQL source (new, all rejected unless `debezium.enabled` is true; unknown values fail validation
with `debezium.config_invalid`):

| Setting | Default | Mirrors | Affects |
| --- | --- | --- | --- |
| `debezium.enabled` | `false` | | switches the output mode |
| `debezium.compat` | latest boundary | Debezium version | source-block shape, envelope `ts_us`/`ts_ns`, Avro `allowed` sets (see _Versions_) |
| `debezium.format.version` | `1` | | Conduit's own emulation revision; see _Versioning_ |
| `debezium.topic.prefix` | required | `topic.prefix` | topic, `source.name`, schema names |
| `debezium.topic.delimiter` | `.` | `topic.delimiter` | topic |
| `debezium.decimal.handling.mode` | `precise` | same | `DECIMAL`/`NUMERIC` typing |
| `debezium.bigint.unsigned.handling.mode` | `long` | same | `BIGINT UNSIGNED` typing |
| `debezium.time.precision.mode` | `adaptive_time_microseconds` | same | `DATE`/`TIME`/`DATETIME` typing |
| `debezium.binary.handling.mode` | `bytes` | same | `BINARY`/`VARBINARY`/`BLOB`/`BIT(>1)` typing |
| `debezium.column.include.list` / `.exclude.list` | none | same | schema and values, applied in the source so the descriptor matches |
| `debezium.skipped.operations` | (none) | same | `c`/`u`/`d` filtering |
| `debezium.message.key.columns` | none | same | key for tables without a PK |
| `debezium.schema.name.adjustment.mode` / `debezium.field.name.adjustment.mode` | `none` | same | Connect schema names / column names |

`debezium.encode` processor settings and the Kafka destination behavior are unchanged from the
Postgres design (converters, `tombstones.on.delete`, subject strategies, `schema.use.latest`, the
message-plan frame `kafka.messages.version=1`, the runtime `kafka.acks_required` check). The
registry URL and credentials are engine settings. `debezium.encode` gains exactly one
MySQL-specific input: the `source` block is built from the record's `debezium.source.*` metadata
using the MySQL field set (below) instead of the Postgres one.

**The mode requires the strict type decode.** The Debezium mode's values come from the lossless
decode that the type-semantics design (#3040) specifies for `types.mode=strict`: `NULL` stays null,
`DECIMAL`/`NUMERIC` are decoded to an exact decimal, `BIGINT UNSIGNED` to `uint64`. The
`debezium.*.handling.mode` settings then choose the **Connect** type for those decoded values
(Decimal logical type / `double` / string for decimals; `INT64` / `Decimal(20,0)` / string for
unsigned bigints), which is Debezium's own surface. The mode refuses a `types.mode` other than
`strict` with `debezium.types_mode_required` (a lossy NULL→zero or DECIMAL→double decode cannot be
repaired downstream; invariant 6).

### Record shape between source and processor

| Part | Content |
| --- | --- |
| `Operation` | `snapshot` for `r`, `create`, `update`, `delete`. No `OperationTruncate`. |
| `Key` | `StructuredData` of the key columns: the primary key in key-sequence order; otherwise `message.key.columns`; absent for a keyless table (the source drops the synthetic `"<file>_<pos>"` key in this mode — Debezium keys keyless rows by `message.key.columns` or null). Debezium-typed. |
| Old key | Present only when the before image carries key columns that differ from the new key: in `debezium.old_key` metadata (typed JSON). Absent means a plain update. |
| `Payload.Before`/`After` | `StructuredData` in Debezium-typed Go values (never the driver's raw values). |
| Metadata `debezium.topic` | `<prefix><delim><db><delim><table>`, sanitized as Debezium does. |
| Metadata `debezium.op` | `c`, `u`, `d`, `r`. |
| Metadata `debezium.source.*` | the MySQL source struct fields below, as strings, parsed by the processor. |
| Metadata `debezium.descriptor` | The row's Connect-typed descriptor (JSON: column order, Connect type, logical name, parameters, optionality, defaults, key columns). |

The descriptor lives in the record and is never registered in the consumers' registry, exactly as
the Postgres design specifies. The source turns `sdk.schema.extract.*` off in this mode (validating
that it is off) and carries the descriptor in `debezium.descriptor` on every record. Field order is
column order, never Go map order; a record a user processor changed so it no longer matches the
descriptor fails `debezium.record_schema_mismatch` (no coercion).

### Envelope and source schemas

- **Envelope** (Connect name `<topic>.Envelope`, version 2): fields in order `before`, `after`
  (both optional structs named `<topic>.Value`), `source`, `transaction`, `op`, `ts_ms`, `ts_us`,
  `ts_ns` (the last two from `debezium.compat >= 2.6`). `transaction` is always present as an
  optional struct (`id`, `total_order`, `data_collection_order`), its value null (MySQL
  `provide.transaction.metadata` is not in scope for v0.22; see D-list). `ts_ms` is optional.
- **Source** (struct `io.debezium.connector.mysql.Source`): fields in this order **(V, confirm the
  exact order against the harness)** — `version`, `connector`, `name`, `ts_ms`, `snapshot`, `db`,
  `sequence`, `ts_us`, `ts_ns`, `table`, `server_id`, `gtid`, `file`, `pos`, `row`, `thread`,
  `query`. `sequence` is always null for MySQL. `snapshot` is a string enum
  (`io.debezium.data.Enum` with an `allowed` parameter); Conduit produces `true` (snapshot rows) and
  `false` (streaming rows) only.
- **Key** (`<topic>.Key`): key columns only.

### Source block values

| Field | Streaming | Snapshot | Notes |
| --- | --- | --- | --- |
| `version` | configured emulated version | same | D3 |
| `connector` | `mysql` | same | |
| `name` | `topic.prefix` | same | |
| `ts_ms`, `ts_us`, `ts_ns` | event timestamp | snapshot start time **(V)** | |
| `snapshot` | `false` | `true` | |
| `db`, `table` | the table's schema and name | same | |
| `server_id` | the event header's server id | the connector's own | already stamped as `mysql.server.id` in the default output |
| `gtid` | null | null | D6 (no GTID support) |
| `file`, `pos` | the binlog file and offset of the event | the snapshot's `P0` | from `cdc_position.name`/`.pos` |
| `row` | the row index within the event | 0 | from `cdc_position.idx` |
| `thread` | null **(V)** | null | not decoded today; D7 |
| `query` | null | null | DDL QueryEvents are not decoded; see _Schema changes_ |
| `sequence` | null | null | not a MySQL concept |
| envelope `ts_ms` | wall clock at encode | | volatile, D1 |

### Type mapping

The golden files from the harness are the specification. The table lists the intended category so
the reviewer can check the shape; names marked **(V)** need confirmation. Defaults are
`decimal.handling.mode=precise`, `time.precision.mode=adaptive_time_microseconds`,
`binary.handling.mode=bytes`, `bigint.unsigned.handling.mode=long`.

| MySQL | Debezium/Connect type (default modes) | Record value | Notes |
| --- | --- | --- | --- |
| `TINYINT` | INT16 | int | Debezium maps TINYINT to INT16, not INT8; `TINYINT(1)` is not auto-boolean (see Boolean) |
| `SMALLINT` | INT16 | int | |
| `MEDIUMINT` | INT32 | int | |
| `INT`, `INTEGER` | INT32 | int | |
| `BIGINT` | INT64 | int64 | |
| `REAL` | FLOAT32 | float | |
| `FLOAT` | FLOAT64 | float | |
| `DOUBLE` | FLOAT64 | float | |
| `DECIMAL(p,s)`, `NUMERIC(p,s)` | `org.apache.kafka.connect.data.Decimal`, params `scale`, `connect.decimal.precision` | decimal string | `precise`: `setScale(s)` with no rounding; a value wider than the scale is an encode error `debezium.value_out_of_range` (as Debezium throws). `double` → FLOAT64, `string` → STRING |
| `BOOLEAN`, `BOOL` | BOOLEAN | bool | MySQL stores these as `TINYINT(1)`; `SHOW CREATE TABLE` returns `TINYINT(1)` for both, so snapshot maps both to INT16 unless the column is known `BOOLEAN` from DDL. `TinyIntOneToBooleanConverter` is **not** in scope (D8) |
| `TINYINT UNSIGNED` | INT16 | int | |
| `SMALLINT UNSIGNED` | INT32 | int | |
| `MEDIUMINT UNSIGNED` | INT32 | int | |
| `INT UNSIGNED` | INT64 | int64 | |
| `BIGINT UNSIGNED` | INT64 (`long`), `Decimal(20,0)` (`precise`), STRING (`string`) | int64 / decimal string / string | `long` halts on a value above 2^63-1 with `debezium.value_out_of_range` (never a silent wrap; ties to #3040 decision 2) |
| `BIT(1)` | BOOLEAN | bool | |
| `BIT(>1)` | BYTES, `io.debezium.data.Bits`, param `length` | bytes | little-endian, sized to `ceil(M/8)` bytes; `binary.handling.mode=base64`/`hex` → STRING |
| `CHAR`, `VARCHAR`, `TINYTEXT`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT` | STRING | string | character set of the column is used to decode the binlog bytes |
| `BINARY`, `VARBINARY`, `TINYBLOB`, `BLOB`, `MEDIUMBLOB`, `LONGBLOB` | BYTES | bytes | `binary.handling.mode=base64`/`hex` → STRING |
| `DATE` | `io.debezium.time.Date` (INT32 days since epoch) | int | `time.precision.mode=connect` → `org.apache.kafka.connect.data.Date` |
| `TIME(p)` | `io.debezium.time.MicroTime` (INT64 microseconds) | int64 | `connect` → `org.apache.kafka.connect.data.Time` |
| `DATETIME(0..3)` | `io.debezium.time.Timestamp` (INT64 ms) | int64 | local datetime read as UTC epoch, no shift |
| `DATETIME(4..6)` | `io.debezium.time.MicroTimestamp` (INT64 µs) | int64 | `connect` → `org.apache.kafka.connect.data.Timestamp` |
| `TIMESTAMP(p)` | `io.debezium.time.ZonedTimestamp` (STRING ISO8601 UTC, µs) | string | converted from the configured DB timezone to UTC |
| `YEAR` | `io.debezium.time.Year` (INT32) | int | |
| `ENUM` | STRING, `io.debezium.data.Enum`, param `allowed` | string | members from the table schema |
| `SET` | STRING, `io.debezium.data.EnumSet`, param `allowed` | string | |
| `JSON` | STRING, `io.debezium.data.Json` | string | string representation as MySQL sends it |
| `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION` | STRUCT `io.debezium.data.geometry.Geometry` `{srid INT32, wkb BYTES}` | struct | (V) in scope only if reachable through canal; otherwise `debezium.unsupported_type` |

**Nullability and defaults.** MySQL nullability comes from `information_schema` column metadata (the
binlog FULL row image carries no nullability), the same source the default output reads today.
Literal column defaults map to a schema default as Debezium does; expression defaults
(`CURRENT_TIMESTAMP`, auto-increment) are omitted.

**Zero-valued temporal columns.** MySQL allows zero-values for `DATE`/`DATETIME`/`TIMESTAMP`
(`0000-00-00`); Debezium emits them as null when the column allows null and as the epoch otherwise.
The mode mirrors this (D9).

**`TINYINT(1)` / Boolean.** Because `SHOW CREATE TABLE` reports `TINYINT(1)` for both `BOOLEAN`
and `TINYINT(1)`, the snapshot maps both to INT16; the streaming path has the original DDL only if
the connector parses it (it does not), so it also maps both to INT16. This matches Debezium's
default (no `TinyIntOneToBooleanConverter`); it is D8, and a consumer that relied on the converter
is a `migrate`-reported difference.

### Key

- Primary key columns in key-sequence order, Debezium-typed.
- No primary key: `message.key.columns` if configured; otherwise a **null key**. The connector's
  synthetic `"<file>_<pos>"` key is dropped in this mode (Debezium does not produce it). A null key
  means no per-key ordering and sticky partitioning, which is what Debezium does; the source logs a
  warning per keyless table (`mysql.debezium.keyless_table`).
- Composite keys are supported (the default output already builds them; the mode reuses the key
  column list and formats the values with the Debezium types instead of Avro).

### Snapshot to stream handoff

Snapshot rows are `op: r` with `before: null`; streaming rows are `c`, `u`, `d`. The connector's
snapshot→CDC handoff is gap-free by construction (P0 under lock, CDC from P0), so a row read by the
snapshot and then changed is delivered twice (once `r`, once `c`/`u`/`d`) — at-least-once, a
documented difference (D5). Snapshot order is per-table key order, not comparable to Debezium's;
the harness compares snapshot output as a set keyed by primary key (D11).

### Schema changes (DDL)

This is the largest MySQL-specific difference and the reason the mode does not reproduce Debezium's
schema-history machinery.

- **Debezium** parses DDL `QueryEvent`s, stores a schema history in Kafka, and emits schema-change
  events to a `<prefix>` topic. The envelope schema then changes when the history changes.
- **This connector** has no schema history. It re-reads the live table columns once on a
  column-count or type-family mismatch (`refreshStaleTable`) and halts with
  `mysql.cdc.row_columns_mismatch` if they still do not fit. This is the behavior the default output
  already documents as "DDL during a running pipeline is not handled explicitly".

The mode keeps that behavior and does **not** add a schema-history store or emit schema-change
events. Consequences, stated plainly:

- A `DROP COLUMN` + `ADD COLUMN` (or a type change) that ran while the connector was stopped or
  lagging still halts with `mysql.cdc.row_columns_mismatch` (invariant 6: it cannot be told from a
  drop-plus-add, and mis-mapping would be silent). Debezium would continue from its history; Conduit
  is stricter. D10.
- `schema.history.internal.kafka.*` is reported **unsupported, non-blocking** by `migrate`: the
  connector does not use it, and the live-schema re-read already covers value mapping. The
  schema-change topic is not produced (D12).
- A `TRUNCATE` is a DDL `QueryEvent` and is **not decoded**; it neither produces `op: t` events nor
  deletes rows in consumers (the default output has the same behavior). This is a real difference
  from Debezium (which emits `op: t` when `skipped.operations` does not include `t`) and is D13,
  reported by `migrate` when `skipped.operations` would admit `t`.

### Name adjustment and subject naming

Identical to the Postgres design (DeBezium's `schema.name.adjustment.mode` /
`field.name.adjustment.mode`, topic sanitization and truncation, Confluent `AvroData` port,
`scrub.invalid.names`). Subjects default to `TopicNameStrategy` (`<topic>-key`, `<topic>-value`);
names come from the Connect schema names (`<prefix>.<db>.<table>.Key|Value|Envelope`). No change is
specified here beyond the MySQL topic form `<prefix>.<db>.<table>`.

### Default-output isolation

Every change to code shared with the default output is gated on `debezium.enabled` or stands as its
own default-output change. The list:

| Change | Treatment |
| --- | --- |
| Lossless value decode (NULL stays null, exact decimal, uint64) | shared with `types.mode=strict` (#3040); the default legacy output keeps its current shapes. The decode is gated and the Avro `schemaMapper` stays the default path. |
| `db`-qualified topic name | gated; default `opencdc.collection` stays the bare table name |
| Key shape (null key for keyless tables, Debezium-typed values) | gated; default keeps the synthetic `"<file>_<pos>"` key |
| `debezium.*` metadata and in-record descriptor | gated |
| `sdk.schema.extract.*` off | gated (validated off only in this mode) |

A default-output golden test runs on the same workload with the mode off and fails on any byte
change. `debezium.encode`'s four placement rules (last, no `condition`, plan-capable destinations
only, halting DLQ) and the pipeline-start validation hook are the engine's, unchanged; they apply to
the MySQL mode the moment a pipeline wires the MySQL source to `debezium.encode`.

### Migrate integration

`migrate kafka-connect` reads the Debezium and converter settings and sets the three components. The
`debezium-mysql` family rows change from their v0.21 state (blocking `kc.output_shape_differs`,
`topic.prefix` approximated):

| Kafka Connect key | Conduit | Status |
| --- | --- | --- |
| `topic.prefix` | `debezium.topic.prefix` | translated (was `approximated, blocking`) |
| `database.include.list` | `tables` | translated (existing) |
| `decimal.handling.mode`, `bigint.unsigned.handling.mode`, `time.precision.mode`, `binary.handling.mode` | `debezium.*` | translated |
| `tombstones.on.delete` | `debezium.encode` `tombstones.on.delete` | translated |
| `column.include.list` / `column.exclude.list` | `debezium.column.*` | translated in the source so the descriptor matches |
| `skipped.operations` | `debezium.skipped.operations` | translated for `c`/`u`/`d` filtering; a `t` value is reported non-applicable (truncate is DDL this connector does not decode, D13) |
| `message.key.columns` | `debezium.message.key.columns` | translated |
| `schema/field.name.adjustment.mode`, converter keys | same names / `debezium.encode` settings | translated (unchanged from Postgres) |
| `schema.history.internal.kafka.*` | | unsupported, **non-blocking** (D12) |
| `database.server.id` | | unsupported, non-blocking (the connector generates its own; existing) |
| `snapshot.mode` | | `initial` translated; `never`/`schema_only`/`no_data` approximated or unsupported as the snapshot lane decides; `incremental` unsupported (blocking) |
| `heartbeat.*`, `gtid.*` | | unsupported; non-blocking, with D6/D-heartbeat notes |
| `key.converter`, `value.converter` + registry/auth | `debezium.encode` settings + engine config fragment | translated for Avro/JSON converters (unchanged from Postgres) |

`migrate` also sets `sdk.schema.extract.*` off on the source, the halting DLQ window, `acks: all` on
the Kafka destination, and the topic template `{{ index .Metadata "debezium.topic" }}`. The blocking
`kc.output_shape_differs` note is **removed** from the `debezium-mysql` family once the mode ships;
the family's goldens and `expect` entries are updated in the same implementation PR.

## Intentional differences

Each is an entry in the harness's `differences.yaml`. An undocumented difference fails the harness.

| ID | Difference | Rule |
| --- | --- | --- |
| D1 | Envelope `ts_ms`/`ts_us`/`ts_ns` are wall-clock at encode. | within a bound of the event's real time |
| D3 | `source.version` is the configured emulated version, not a Debezium build. | equals configured value |
| D5 | A snapshot restarted by crash resume may emit `r` rows a later `u` changes; Debezium restarts the whole snapshot. | at-least-once set equality by key and final state |
| D6 | `source.gtid` is null (no GTID positions). | absent |
| D7 | `source.thread` is null (not decoded). | absent |
| D8 | `TINYINT(1)`/`BOOLEAN` both map to INT16; no `TinyIntOneToBooleanConverter`. | equal to Debezium default |
| D9 | Zero-valued `DATE`/`DATETIME`/`TIMESTAMP` → null (or epoch when NOT NULL), as Debezium. | equal |
| D10 | Incompatible DDL halts with `mysql.cdc.row_columns_mismatch` rather than continuing from schema history. | halt, documented |
| D11 | Snapshot row order and cross-table order differ; per-key order after the handoff is identical. | set equality for snapshot; sequence equality for streaming |
| D12 | No schema-history store; the `<prefix>` schema-change topic is not produced. | topic absent |
| D13 | `TRUNCATE` (a DDL QueryEvent) is not decoded and produces no `op: t` event. | absent |
| D14 | Kafka producer behavior is the Conduit destination's, not the Connect worker's. | out of scope for byte compare |
| D15 | Plan semantics under failure and replay (the transient windows on a partial plan replay). | chaos asserts the post-replay state only |

## Failure modes

| Failure | What happens | Where it is caught | Invariant |
| --- | --- | --- | --- |
| **BIGINT UNSIGNED above 2^63-1 in `long` mode** | `debezium.value_out_of_range`, halt, never a wrap (ties to #3040 decision 2). | per-type unit + golden tests | 6 |
| **DECIMAL wider than its scale, NaN, Infinity in `precise`** | `debezium.value_out_of_range`, halt (as Debezium throws). | property/golden tests | 6 |
| **Row image skips a column** | already halts with `mysql.cdc.row_image_skipped_columns`; the mode changes nothing. | existing integration tests | 3, 6 |
| **Column count/type mismatch (DDL raced the read)** | `mysql.cdc.row_columns_mismatch` after one re-read; no mis-mapping. | existing integration tests | 6 |
| **DDL `DROP COLUMN`+`ADD COLUMN` while stopped/lagging** | halts (D10), stricter than Debezium; remedy is reset/re-snapshot. | integration test | 6 |
| **Nullability/defaults read from a stale catalog** | descriptor built from `information_schema` at the time the shape is first seen; a DDL that changes nullability without a column-count/type change is undetected until the re-read (same blind spot as today, and as Debezium's history). | documented residual risk | 6 |
| **Primary-key update** | one record, three messages (delete + `__debezium.newkey`, tombstone, create + `__debezium.oldkey`), acked together; old key always available because the image is FULL. | integration + chaos | 1, 3, 4 |
| **Tombstones and plans across restarts** | crash after some plan messages were produced, before position persisted: the record was not acked, so the whole plan replays. Duplicates of the first messages; never a delete without its tombstone after recovery. | chaos SIGKILL between plan messages | 1, 3, 7 |
| **Keyless table** | null key, no per-key order, sticky partitioning; `mysql.debezium.keyless_table` warning; `message.key.columns` restores a key. | harness keyless case | 4 |
| **Descriptor absent or inconsistent with the record** | `debezium.descriptor_missing` / `debezium.record_schema_mismatch`, halt. | unit | 6 |
| **Name adjustment collision or invalid name** | `debezium.name_collision` / `debezium.invalid_name`, halt. | harness identifiers case | 6 |
| **Schema Registry down, or 409 on register** | retry with a bound, then halt; never emit unencoded bytes, never skip. | chaos SR outage | 1, 3, 6 |
| **Mode misuse** (MySQL `debezium.enabled=true` wired to a non-Kafka destination without `debezium.encode`) | refused at pipeline start by the engine hook (`debezium.encode_missing`); typed values never reach a topic as ordinary messages. | validate rules | 3 |
| **`debezium.enabled` without `types.mode=strict`** | `debezium.types_mode_required` at Open (before any record). | unit | 6 |
| **binlog purged / GTID gaps / failover** | existing `ErrCDCStartPositionUnavailable` / "No GTID support" limits; the mode adds no new resume path and does not claim failover. | existing integration tests | 1, 2 |

## Test plan

### Diff harness (the gate)

Extend `tests/debezium` with a MySQL stack: one MySQL 8.0/8.4 (`binlog_format=ROW`,
`binlog_row_image=FULL`, `binlog_row_metadata=FULL`), two isolated Kafka+Schema Registry stacks (A:
pinned Debezium MySQL plugin; B: Conduit with the mode on and `migrate` run against A's config).
The Debezium image matrix reuses the Postgres lane's compat boundaries with the MySQL plugin
(2.6.0.Beta1 side, 2.7.x, 3.0.4, 3.0.5, 3.5, plus latest 3.x).

Workload: snapshot of pre-loaded rows; inserts, updates (key-changing and not), deletes; multi-row
inserts; `BIT`, `DECIMAL` (scale 0 and wide), `BIGINT UNSIGNED` (above and below 2^63-1), temporal
types (0 and 1–6 precision), `JSON`, `ENUM`/`SET`, `BLOB`/`TEXT`, nulls in every column, composite
and single and absent PKs; quoted, Unicode and leading-digit identifiers; a `DROP COLUMN`+`ADD
COLUMN` DDL (asserts the halt, D10); keyless tables with and without `message.key.columns`.

Comparison layers, converter pinning, the Java-oracle `AvroData` differential test, the partitioner
test and the self-test are unchanged from the Postgres design.

### Other tests

- Property tests (`rapid`): per-type round-trip of decoded value to Debezium value; field order
  equals column order; plan expansion yields the message sequence of the table.
- Fuzz targets on the decimal, temporal and `BIT` value formatters and the name-adjustment
  functions.
- Chaos (`tests/chaos`): SIGKILL between messages of a plan (delete/tombstone and the PK-change
  sequence); SIGKILL during registration; the SR outage.
- Default-output isolation: with the mode off, the MySQL source emits byte-identical records on the
  same workload.
- Upgrade: pipeline in the mode at N, upgrade to N+1, resume; positions unchanged (the mode adds no
  position field); a `format.version` the build does not know fails validation
  (`debezium.format_version_unknown`).
- Acceptance: the source in the mode runs the SDK acceptance suite v2; the default-mode suite is
  unchanged and green with the mode off.
- Benchi: a config for default versus debezium mode, committed, no number claimed until then.

## Versioning, upgrade and rollback

- `debezium.format.version` (starts at 1) names Conduit's emulation revision, as in the Postgres
  design. A fix that changes schema or key bytes ships under a new version, old versions selectable
  for two minor releases.
- **Positions do not change.** The mode reuses the existing `snapshot_position` (v2) and
  `cdc_position` (v1) envelopes verbatim; no field is added, `CurrentPositionVersion` stays 2. A
  pipeline can switch `debezium.enabled` on and off without a position migration, because the
  positions are the same binlog/file-and-offset form in both modes. (The type-semantics strict stamp
  from #3040 is the only position-envelope change on this lane, and it is specified there.)
- Turning the mode on for an existing pipeline is a topology change (new topics, new keys, null
  keys for keyless tables); `migrate` creates new pipelines, it does not flip existing ones. Turning
  it off returns the default output shape, not the previous pipeline's state.
- The plan frame is versioned (`kafka.messages.version=1`); a destination that does not know a
  newer version fails `kafka.messages_version_unknown`.
- An ADR for the placement decision (source-typed output + trailing encoder) is written in
  ConduitIO/conduit after sign-off, covering both Postgres and MySQL.

## Observability

Metrics (labels pipeline, connector, table where noted):

| Metric | Purpose |
| --- | --- |
| `conduit_debezium_encoded_records_total{op}` | volume by op (`c`, `u`, `d`, `r`) |
| `conduit_debezium_encode_errors_total{code}` | by stable error code |
| `conduit_debezium_schema_registrations_total{result}` | cutover health |
| `conduit_debezium_keyless_tables_total` | keyless tables emitted with a null key |
| `conduit_kafka_tombstones_total` | tombstones produced |

Errors (stable codes, failing config path, suggested fix): `debezium.config_invalid`,
`debezium.types_mode_required`, `debezium.value_out_of_range`, `debezium.unsupported_type`,
`debezium.record_schema_mismatch`, `debezium.descriptor_missing`, `debezium.name_collision`,
`debezium.invalid_name`, `debezium.format_version_unknown`, plus the engine-side `debezium.*` and
`kafka.*` codes already specified in the Postgres design. Log code, not an error:
`mysql.debezium.keyless_table`.

Runbooks (owed with implementation): schema incompatible at the registry, registry unreachable,
`mysql.cdc.row_columns_mismatch` under the mode (DDL raced the read), tombstone lag on compacted
topics, rollback to Debezium.

## Docs

`conduit.io` docs source and `llms.txt` in the same implementation PRs: the Debezium-compatible
output page for MySQL with the full difference list (D1–D15), the `migrate` guide rows, and the
MySQL connector README's Debezium-mode section and type-mapping table. The `vs Debezium` page cites
the harness result, not this document.

## Dependencies on other lanes

| Lane | Dependency | Direction |
| --- | --- | --- |
| Type semantics (#3040, connector) | Lossless decode (NULL, exact decimal, uint64) and `types.mode=strict`. The Debezium mode requires it. | blocks this lane |
| `debezium.encode` (ConduitIO/conduit #3183, #3032) | The envelope/tombstone/PK-change/naming machinery; parameterize the `source` block for MySQL. | blocks this lane |
| Kafka destination (`conduit-connector-kafka`) | Message plans, headers, null keys, index-correct acks (after #308). | blocks this lane |
| Engine (Conduit core) | The pipeline-start validation hook. | blocks this lane |
| migrate/kc CLI | Rewrite the `debezium-mysql` family mapping rows; drop `kc.output_shape_differs`. | needs this doc's setting names agreed |
| Migration harness (#3010) | Update the `debezium-mysql` family goldens; shares fixtures with `tests/debezium`. | shares fixtures |
| Acceptance suite v2 | Applies as for any source. | none |

## Decisions

Pending DeVaris sign-off (mirrors the Postgres lane's open questions, MySQL-specific where noted):

1. Accept the split: source-typed output (one record per binlog row event), trailing
   `debezium.encode` with message plans, destination-owned plan writes. The seam now has two
   committed producers (Postgres, MySQL).
2. Support the same Debezium version tiers as Postgres (2.7.x + latest 3.x, compat boundaries 2.6 /
   3.0.5 / 3.5).
3. No schema-history store and no schema-change topic; keep the re-read-and-halt DDL behavior
   (D10, D12).
4. No GTID positions, no heartbeat, no `source.gtid`/`source.thread` (D6, D7); these stay "No GTID
   support" / "No heartbeat" limits, not new work in the mode.
5. Keyless tables emit a null key (with `message.key.columns` as the escape); the synthetic
   `"<file>_<pos>"` key is dropped in this mode.
6. The Debezium mode requires `types.mode=strict` (`debezium.types_mode_required` otherwise).

Items marked **(V)** are not open decisions; they are facts the harness establishes before the
corresponding implementation PR merges.

## Related

- `20260724-snapshot-cdc-position-handoff.md`, `20261012-cdc-snapshotted-tables.md`,
  `20261015-cdc-resume-inside-binlog-events.md` (this repo)
- ConduitIO/conduit `docs/design-documents/20261012-debezium-compatible-output.md` (Postgres mode,
  the template for this doc), `20261012-mysql-type-semantics.md` (PR #3040, the type half)
- ConduitIO/conduit #3183 (`debezium.encode`), #3032, #3005, #3010
- Debezium MySQL type mapping: `c_how-the-mysql-connector-maps-data-types.adoc`
- `conduit-connector-kafka#308` (batch `Produce` result tracking)
