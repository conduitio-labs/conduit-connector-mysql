# Conduit Connector for MySQL

<!-- readmegen:description -->
## Source

A source connector pulls data from an external resource and pushes it to
downstream resources via Conduit.

### Snapshot mode

Snapshot mode is the first stage of the source sync process. It reads all rows
from the configured tables as record snapshots.

In snapshot mode, the record payload consists of
[opencdc.StructuredData](https://conduit.io/docs/using/opencdc-record#structured-data),
with each key being a column and each value being that column's value.

### Change Data Capture mode

When the connector switches to CDC mode, it starts streaming changes from the
obtained position at the start of the snapshot. It uses the row-based binlog format
to capture detailed changes at the individual row level.

### Tables without a primary key

The snapshot reads each table in key order and records the last key read in the
position, so it can resume exactly where it stopped and read a consistent view.
That needs a primary key, or a column you name with
`tableConfig.<table>.sortingColumn`. If a table has neither, the connector
fails to start with error `mysql.snapshot.table_without_key`, naming the table
and the config path `tableConfig.<table>.sortingColumn`. To fix it, do one of:

- add a primary key to the table;
- set `tableConfig.<table>.sortingColumn` to a column that is `NOT NULL` and the
  only column of a primary key or unique index. Anything else is rejected at
  start with `mysql.snapshot.sorting_column_invalid`, because duplicate or NULL
  values can make the snapshot skip rows;
- exclude the table. With a pattern list, a later `-` rule removes tables from an
  earlier match, for example `tables: "*,-^audit_log$"` snapshots everything
  except `audit_log`. With an explicit list, remove the table from it;
- set `snapshot.enabled: false` to stream changes only. Rows that already exist
  are then not copied. CDC works for tables without a primary key; their record
  keys are derived from the binlog position.

The key and sorting-column checks run only when a snapshot is about to run. A
pipeline restarting in CDC mode is not affected.

#### Migrating from `snapshot.unsafe`

The `snapshot.unsafe` setting has been removed. It snapshotted keyless tables
with `LIMIT`/`OFFSET`, recorded no position (so every restart began again from
zero), and could skip or repeat rows when the table changed during the
snapshot. In practice it also applied without the setting: a keyless table was
snapshotted this way even when `snapshot.unsafe` was not set.

- `snapshot.unsafe: true` now fails configuration validation with error
  `mysql.config.unsafe_snapshot_removed`, **including for pipelines that are
  already in CDC mode**: validation does not look at the position. Remove the
  setting before upgrading.
- `snapshot.unsafe: false` is accepted and ignored.
- After upgrading, a pipeline that still has a keyless table and will run a
  snapshot fails to start with `mysql.snapshot.table_without_key` until you use
  one of the options above.

### Schema

The source connector uses [avro](https://avro.apache.org/docs/1.11.1/specification/) to decode mysql rows. Here's the MySQL datatype to avro datatype equivalence that the connector uses:

| MySQL Type | Avro Type |
| ---------- | --------- |
| TINYINT    | int       |
| SMALLINT   | int       |
| MEDIUMINT  | int       |
| INT        | int       |
| BIGINT     | long      |
| DECIMAL    | double    |
| NUMERIC    | double    |
| FLOAT      | double    |
| DOUBLE     | double    |
| BIT        | bytes     |
| CHAR       | string    |
| VARCHAR    | string    |
| TINYTEXT   | string    |
| TEXT       | string    |
| MEDIUMTEXT | string    |
| LONGTEXT   | string    |
| BINARY     | bytes     |
| VARBINARY  | bytes     |
| TINYBLOB   | bytes     |
| BLOB       | bytes     |
| MEDIUMBLOB | bytes     |
| LONGBLOB   | bytes     |
| DATE       | string    |
| TIME       | string    |
| DATETIME   | string    |
| TIMESTAMP  | string    |
| YEAR       | long      |
| ENUM       | string    |
| SET        | string    |
| JSON       | string    |

### Record structure

Records produced by the MySQL source connector contain the following [opencdc record structure](https://conduit.io/docs/using/opencdc-record):

*   **Operation**: Indicates the type of change (`create`, `update`, `delete`, `snapshot`).
*   **Payload**:
    *   `Before`: (Only present on CDC `update` operations) `opencdc.StructuredData` representing the row state before the change.
    *   `After`: `opencdc.StructuredData` representing the row state after the change (or the current state for `snapshot` and `create`).
*   **Key**: Identifies the specific row. See Snapshot/CDC sections below for details.
*   **Position**: Represents the point in the data stream. It's a JSON object containing *either* a `snapshot_position` field *or* a `cdc_position` field, structured as described below.
*   **Metadata**: Contains standard OpenCDC fields plus:
    *   `mysql.server.id`: (CDC only) The originating MySQL server ID from the replication event header.
    *   [Key](https://conduit.io/docs/using/opencdc-record/#opencdckeyschema) and [payload](https://conduit.io/docs/using/opencdc-record/#opencdcpayloadschema) schema registry data.

#### Snapshot Records

*   **Operation**: `snapshot`.
*   **Payload `After`**: `opencdc.StructuredData` with all column values.
*   **Key**: `opencdc.StructuredData` using the primary key columns (or the configured `tableConfig.<table>.sortingColumn`) and their values.
*   **Position (`snapshot_position` field)**: A JSON object containing:
    *   `snapshots`: (object) Maps table names to their specific snapshot position object.
        *   *(Single Primary Key Table)*: Contains `last_read` (any) and `snapshot_end` (any).
        *   *(Multiple Primary Key Table)*: Contains an array of objects, each with `key_name` (string), `last_read` (any), and `snapshot_end` (any).

#### CDC Records

*   **Operation**: Either `create`, `update` or `delete`.
*   **Key**:
    *   With Primary Key(s): `opencdc.StructuredData` using primary key columns and values (from the `After` state).
    *   Without Primary Key(s): `opencdc.RawData` string `"<binlog_file_name>_<log_position>"`.
*   **Payload**:
    *   `Before`: (For `update` only) `opencdc.StructuredData` with pre-update column values.
    *   `After`: `opencdc.StructuredData` with post-change column values (for `create`, `update`) or pre-deletion values (for `delete`).
*   **Position (`cdc_position` field)**: Encodes the binlog location in a JSON object with the following fields.
    *   `name`: (string) Binlog file name.
    *   `pos`: (uint32) Position within the binlog file.
    *   `prev`: (object, optional) Position of the preceding event, containing `name` (string) and `pos` (uint32). Omitted if not applicable.
    *   `idx`: (int, optional) Row index within a potentially multi-row MySQL replication event. Omitted if zero or not applicable.

## Destination

The MySQL destination takes a slice of `opencdc.Record` and writes them in batches to MySQL. It will use the `opencdc.collection` field in the metadata to determine the table to write to.

### Upsert Behavior

If the target table contains a column with a unique constraint (this includes PRIMARY KEY and UNIQUE indexes), records will be upserted. Otherwise, they will be appended. Support for updating tables without unique constraints is tracked [here](https://github.com/conduitio-labs/conduit-connector-mysql/issues/66).

If the target table already contains a record with the same key, the Destination will upsert with its current received values. Because Keys must be unique, this can overwrite and thus potentially lose data, so keys should be assigned correctly from the Source.

If a unique key is not present in the target table, the record will be simply appended.

### Columns and schema changes

Each record is written with exactly the fields in its `.Payload.After`, whatever the other records in the batch contain:

- A field the table has no column for fails the write with `mysql.destination.unknown_column`. The destination does not create or alter tables, and it never drops a field. Add the column first, or remove the field with a processor.
- A field the record does not have is not written. An inserted row gets the column's default; an existing row keeps the column's current value. A field set to `null` writes `NULL`.

Table and column names are quoted, so reserved words work. A collection of the form `db.table` writes to `table` in database `db`.

**Security.** Table and column names come from the records. Quoting keeps them from changing the SQL, but a record's collection can name any database the DSN user can write to, through `db.table`. Grant the destination's user access only to the databases it should write. Quoting is only sound in character sets where no character can end in a backtick byte, so a connection using `sjis`, `cp932`, `gbk`, `gb18030` or `big5` is refused with `mysql.destination.unsafe_charset` (checked in the DSN, when the destination opens, and on the connection of every write). Use `utf8mb4`, the driver's default.

### Deletes

Delete records are matched on the target table's primary key, so the table must have one: a delete for a table without a primary key fails with `mysql.destination.delete_without_primary_key` instead of removing rows. The record key must hold a non-null value for every primary-key column (`mysql.destination.key_column_missing` otherwise), either as structured data or as a single JSON object. Each value must be one scalar: a list or an object fails with `mysql.destination.key_value_not_scalar` (it would match several rows or none). Key values are converted to the type of their key column, which is read for every write, so an `ALTER TABLE` on a running pipeline is picked up without a restart:

- **Integer columns** (`TINYINT` to `BIGINT`, signed or `UNSIGNED`) take a whole number within the column's range (`5`, `5.0`, `5e0` or the string `"5"`). A fraction, a non-numeric string or a value outside the range fails with `mysql.destination.key_value_type_mismatch` instead of matching nothing or the wrong row. A boolean matches as MySQL compares it: `true` is 1, which is what a `TINYINT(1)` key needs.
- **String columns** (`CHAR`, `VARCHAR`, the `TEXT` types, `BINARY`, `VARBINARY`, `ENUM`, `SET`) take a string. A number is sent as its decimal text (a raw JSON number as written, so `1e3` matches the row `'1e3'`), because MySQL compares a number with a string column as a double and `1000` would match `'1000'`, `'01000'`, `'1000.0'` and `' 1000'`. A boolean fails. For `ENUM` and `SET` the key is matched by value, never by index.
- **Other columns** (`DECIMAL`, floating point, temporal, `BIT`) get the value as it is; a raw JSON number keeps its literal, which MySQL compares exactly with a `DECIMAL` column. `BIT` keys compare as MySQL compares numbers with `BIT`; prefer integer or string keys.

Integer values in a raw JSON key are matched exactly. Structured keys that reach a standalone connector over the plugin protocol have already passed through a double, so integers above 2^53 sent that way are rounded before the destination sees them ([ConduitIO/conduit#3100](https://github.com/ConduitIO/conduit/issues/3100)). A record whose operation is not create, update, delete or snapshot fails with `mysql.destination.unsupported_operation`.

### Atomicity and acknowledgements

Each write is a single transaction. If any record in the write fails, nothing from that write is stored and none of its records are acknowledged; the records are redelivered after the pipeline recovers. Upserts on a primary or unique key make the redelivery idempotent; a table without one gets duplicate rows.

## Delivery semantics and limits

The source connector is in beta. This section states what it guarantees and what it does not.

### Delivery guarantees

- **At-least-once.** A record is acknowledged upstream only after the destination has
  acknowledged it, and the position carried on a record is what Conduit persists. After a
  crash or restart you can receive records again, never fewer. Make destinations idempotent
  (upsert by key).
- **Snapshot to CDC handoff.** The connector captures the binlog position at the start of
  the snapshot and begins CDC from it, so changes made while the snapshot runs are delivered
  after it. Rows can therefore appear twice: once as a `snapshot` record and again as a
  CDC change.
- **Restart.** A restart during the snapshot resumes the snapshot from the last
  acknowledged key per table. After a CDC restart, CDC resumes after the last acknowledged
  row, also in the middle of a multi-row binlog event, in a later binlog event of one large
  statement, and inside a compressed transaction (`binlog_transaction_compression=ON`). After
  upgrading from an older version, restarts re-read the binlog file of the saved position from
  its start to find the saved event, until the first record is acknowledged under the new
  version. Rows are delivered again (duplicates, never a gap) in three cases, each logged as a
  warning: the saved position was written right after a restart in the middle of an event (that
  event is delivered again), an older version's position points inside a compressed transaction
  (its binlog file is delivered again from its start), or the captured tables changed while the
  saved position was inside a compressed transaction (that transaction is delivered again).
- **Ordering.** CDC records are delivered in binlog order. During the snapshot, rows are
  ordered by key within a table; tables are read concurrently, so there is no ordering across
  tables.

### Limits

- **No GTID support.** Positions are a binlog file name and offset on the server the
  connector read from. They are not portable between servers: a replica, or a replica
  promoted after a failover, has different binlog files and offsets. **Failing over the
  source database breaks resume.** The connector may fail to start, or may not resume at the
  matching point. After a failover, treat the pipeline as needing a new snapshot: clear its
  position and expect to re-read the data. GTID-based positions are not supported in this
  release.
- **Tables added after the pipeline entered CDC are not snapshotted.** A snapshot runs only
  on a fresh start. The position records which tables were snapshotted, and a restart checks
  the configuration against it:
  - If you add a table to an explicit `tables` list (plain table names), or switch
    `snapshot.enabled` from `false` to `true`, the connector refuses to start with
    `mysql.snapshot.tables_added_after_cdc`, naming the tables. Start a new pipeline for them,
    or reset this pipeline's position to snapshot everything again (expect duplicates).
  - If `tables` uses a pattern (`*`, a regex, an exclude, or an anchored name such as
    `^orders$`), the connector logs one warning with the same code and then delivers the new
    tables **from CDC only**: their existing rows are not snapshotted and are never delivered.
  - A plain name is a regex that is not anchored, so `orders` also matches `orders_archive`.
    It is still treated as an explicit list, which is the safe side. Anchor it as `^orders$`
    to make it a pattern.
  - A table removed from `tables` and added back later counts as added: changes made while it
    was excluded were never captured. For an explicit list this means a table that is absent
    at one restart and present at the next errors.
  - A position written by an older version cannot be checked. The connector adopts the
    configured tables as **unverified**, never as snapshotted, and logs
    `mysql.snapshot.tables_unverified` on every start until the position is reset. Tables added
    afterwards are detected.
  - With more than 200 tables the position stores a hash of the table set instead of the
    names. A change to the set then cannot be named or given a direction (a table that was only
    removed also triggers it). The error says that "the configured table set changed", and
    the way out is to restore the previous `tables` list or reset the position; removing tables
    from the list does not clear it.
- **A key column that was retyped restarts its table's snapshot.** The position records the
  type of each key column. If the saved key does not fit the column's type now (an `INT` key
  column that became `VARCHAR`, or `VARCHAR` that became `DECIMAL`, whose values order
  differently), the table's snapshot restarts from its first row with a warning. Rows already
  delivered are delivered again; nothing is skipped.
- **Upgrading in the middle of a snapshot re-reads text, decimal and binary key tables.**
  A snapshot position written by an older version does not record the key type, so a table
  whose key is text, binary, `DECIMAL`, `TIME`, `ENUM`, `SET` or `BIT` cannot be checked for a
  retype and restarts from its first row after the upgrade; its partial progress is re-read.
  Integer, date and time keys resume where they stopped. Pipelines already in CDC are not
  affected.
- **Binlog retention is your responsibility.** If the persisted position has been purged
  from the server, the connector cannot resume. Keep `binlog_expire_logs_seconds` longer than
  the longest outage you want to survive.
- **No heartbeat.** The connector does not send or consume heartbeat events. The position
  is carried on records, so while the configured tables are quiet the persisted position does
  not advance, even if the server keeps writing and rotating binlogs for other tables. A long
  quiet period followed by binlog purging can leave the stored position pointing at a purged
  file.
- **DDL during a running pipeline is not handled explicitly and is not tested.** Running
  `ALTER TABLE` (or other schema changes) on a captured table while the snapshot or CDC is
  running may produce records that do not match the schema registered earlier, or an error.
  Stop the pipeline, change the schema, and verify the result before resuming.
- **Tables must be orderable for the snapshot.** See "Tables without a primary key".
- **Type mapping has known gaps.** `DECIMAL` and `NUMERIC` map to Avro `double` and can lose
  precision beyond what a 64-bit float holds. A `BIGINT UNSIGNED` value above 2^63-1 cannot
  be encoded as Avro `long`; the connector logs a warning and passes the value through
  unconverted. Date and time types are delivered as strings. A full type-mapping design is
  pending; do not rely on exact fidelity for these types yet.

## Requirements and compatibility

The connector is tested against MySQL v8.0. Compatibility with older versions isn't guaranteed.

### MySQL Server Requirements:

- Binary Log (binlog) must be enabled.
- Binlog format must be set to ROW.
- Binlog row image must be set to FULL.
- Tables must have **sortable** primary keys.

### Partial row images

`binlog_row_image` is also a session variable. A writing session that sets it to `MINIMAL` or `NOBLOB` logs
only part of the row, even when the server value is `FULL`:

- `MINIMAL`, UPDATE: the before image holds only the primary key, and the after image holds only the changed
  columns (the primary key is in it only if it changed).
- `MINIMAL`, DELETE: the before image holds only the primary key.
- `MINIMAL`, INSERT: the image holds only the columns the statement names. Every column it omits is skipped,
  including columns with a `DEFAULT` and nullable columns, so an ORM that leaves defaulted columns out halts on
  its first insert. An INSERT that names every column does not halt.
- `NOBLOB`: `BLOB` and `TEXT` columns that an UPDATE or DELETE did not change are skipped, and so is a `BLOB`
  that an INSERT omits. An INSERT that omits a non-`BLOB` column with a default is logged in full.

A skipped column is unknown. It is not NULL and it is not the column default: the binlog carries no value for
it, and the schema default may have changed since the row was written, so the connector does not fill it in.

The CDC source halts on the first such event instead of delivering zero values over real data. The error has
the code `mysql.cdc.row_image_skipped_columns` and names the table, the action, the binlog file and position, and
the columns that were left out. Snapshot rows are not affected. If the event sits inside a compressed
transaction (`binlog_transaction_compression=ON`) the exact position is not available and the error says so.

The source also stops with the code `mysql.cdc.partial_json_unsupported` when a writer logs partial JSON updates
(`binlog_row_value_options=PARTIAL_JSON`), which the connector cannot read. Set the option to an empty value for
the writers.

Two more CDC halts are resolved by resetting the position, so the recovery below (deletes) applies to them too:

- `mysql.cdc.row_columns_mismatch`: a binlog event has fewer values than its table now has columns. The
  connector decodes events with the table's current columns, so an event written before an `ADD COLUMN` and
  read after it (the connector was stopped or lagging across the DDL) cannot be mapped. Restarting does not
  help: the event fails the same way every time. Reset the position to snapshot again.
- `mysql.cdc.resume_event_not_found`: on a restart, the binlog event the saved position names was not where
  the position says. This should not happen; report it with the logged position, and reset the position to
  continue.

#### Recovery

**Resuming means a re-snapshot, and a snapshot does not carry deletes.** Rows deleted at the source while the
pipeline was halted would stay in the destination. Do one of the following, for ALL captured tables, not only
the table named in the error:

- truncate the destination tables, then clear the connector's position to re-snapshot; or
- clear the connector's position to re-snapshot, and after the snapshot has completed reconcile the destination
  against the source, deletes included.

Do not reconcile first and re-snapshot afterwards: rows deleted at the source between the reconcile and the
start of the snapshot would stay in the destination.

1. Stop the cause. Set `binlog_row_image = 'FULL'` for the writers, and for the server use
   `SET PERSIST binlog_row_image = 'FULL'` (`SET GLOBAL` does not survive a restart). Then reconnect every
   writer: existing sessions, pooled connections included, keep the value they started with.
2. The event is already in the binlog, so every restart halts on it. Find its transaction with
   `SHOW BINLOG EVENTS IN '<file>'` (the error gives the start position of the event); with GTIDs enabled,
   the `Gtid` event just before it names the GTID.
3. Truncate the destination for all captured tables, or plan to reconcile it after the snapshot (see above).
4. Clear the connector's position to re-snapshot.
5. If you chose to reconcile, do it once the snapshot has completed, including deletes, for all captured tables.

A lighter way to resume after the event is tracked in
[#212](https://github.com/ConduitIO/conduit-connector-mysql/issues/212).

#### Upgrading

If the binlog after the stored position already holds an event from a `MINIMAL` or `NOBLOB` session, including
an event that an older version had partly emitted, the upgraded connector halts on it after the restart. Older
versions delivered such events with zero values.

### MySQL User Privileges:

For Snapshot and CDC modes, the following privileges are required:

- SELECT
- LOCK TABLES
- RELOAD
- REPLICATION CLIENT
- REPLICATION SLAVE
<!-- /readmegen:description -->

## Source Configuration Parameters

<!-- readmegen:source.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "mysql"
        type: source
        settings:
          # The connection string for the MySQL database.
          # Type: string
          # Required: yes
          dsn: ""
          # Represents the tables to read from. - By default, no tables are
          # included, but can be modified by adding a comma-separated string of
          # regex patterns. - They are applied in the order that they are
          # provided, so the final regex supersedes all previous ones. - To
          # include all tables, use "*". You can then filter that list by adding
          # a comma-separated string of regex patterns. - To set an "include"
          # regex, add "+" or nothing in front of the regex. - To set an
          # "exclude" regex, add "-" in front of the regex. - e.g. "-.*meta$,
          # wp_postmeta" will exclude all tables ending with "meta" but include
          # the table "wp_postmeta".
          # Type: string
          # Required: yes
          tables: ""
          # Disables verbose cdc driver logs.
          # Type: bool
          # Required: no
          cdc.disableLogs: "false"
          # Controls whether the snapshot is done.
          # Type: bool
          # Required: no
          snapshot.enabled: "true"
          # Limits how many rows should be retrieved on each database fetch on
          # snapshot mode.
          # Type: int
          # Required: no
          snapshot.fetchSize: "10000"
          # Removed. The unsafe snapshot no longer exists: true fails validation
          # with a migration message, false is accepted and ignored. Tables
          # without a primary key need tableConfig.<table>.sortingColumn.
          # Type: bool
          # Required: no
          snapshot.unsafe: "false"
          # Allows to force using a custom column to sort the snapshot. The
          # column must be NOT NULL and the only column of a primary key or
          # unique index.
          # Type: string
          # Required: no
          tableConfig.*.sortingColumn: ""
          # Maximum delay before an incomplete batch is read from the source.
          # Type: duration
          # Required: no
          sdk.batch.delay: "1s"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          # Required: no
          sdk.batch.size: "10000"
          # Specifies whether to use a schema context name. If set to false, no
          # schema context name will be used, and schemas will be saved with the
          # subject name specified in the connector (not safe because of name
          # conflicts).
          # Type: bool
          # Required: no
          sdk.schema.context.enabled: "true"
          # Schema context name to be used. Used as a prefix for all schema
          # subject names. If empty, defaults to the connector ID.
          # Type: string
          # Required: no
          sdk.schema.context.name: ""
          # Whether to extract and encode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # The subject of the key schema. If the record metadata contains the
          # field "opencdc.collection" it is prepended to the subject name and
          # separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.key.subject: "key"
          # Whether to extract and encode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
          # The subject of the payload schema. If the record metadata contains
          # the field "opencdc.collection" it is prepended to the subject name
          # and separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.payload.subject: "payload"
          # The type of the payload schema.
          # Type: string
          # Required: no
          sdk.schema.extract.type: "avro"
```
<!-- /readmegen:source.parameters.yaml -->

## Destination Configuration Parameters

<!-- readmegen:destination.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "mysql"
        type: destination
        settings:
          # The connection string for the MySQL database.
          # Type: string
          # Required: yes
          dsn: ""
          # Maximum delay before an incomplete batch is written to the
          # destination.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets written to the destination.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Allow bursts of at most X records (0 or less means that bursts are
          # not limited). Only takes effect if a rate limit per second is set.
          # Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the
          # effective batch size will be equal to `sdk.rate.burst`.
          # Type: int
          # Required: no
          sdk.rate.burst: "0"
          # Maximum number of records written per second (0 means no rate
          # limit).
          # Type: float
          # Required: no
          sdk.rate.perSecond: "0"
          # The format of the output record. See the Conduit documentation for a
          # full list of supported formats
          # (https://conduitdata.io/docs/using/connectors/configuration-parameters/output-format).
          # Type: string
          # Required: no
          sdk.record.format: "opencdc/json"
          # Options to configure the chosen output record format. Options are
          # normally key=value pairs separated with comma (e.g.
          # opt1=val2,opt2=val2), except for the `template` record format, where
          # options are a Go template.
          # Type: string
          # Required: no
          sdk.record.format.options: ""
          # Whether to extract and decode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # Whether to extract and decode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
```
<!-- /readmegen:destination.parameters.yaml -->

## Testing

Run `make test` to run all tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource locally. It includes [adminer](https://www.adminer.org/) for database management.

Use the `TRACE=true` environment variable to enable trace logs when running tests.
