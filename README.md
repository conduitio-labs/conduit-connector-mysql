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

### Unsafe snapshot

By default, the connector will error out if it finds a table that has no primary key and no specified sorting column specified, as we can't guarantee that the snapshot will be consistent. Table changes during the snapshot will be however captured by CDC mode.

As of writing, the unsafe snapshot is implemented using batches with `LIMIT` and `OFFSET`, so expect it to be slow for large tables. You can optimize the snapshot by specifying a sorting column (see source configuration parameters).
The position of the table is currently not recorded, so the unsafe snapshot will restart from zero every time.

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

Records produced by the MySQL source connector contain the following components:

*   **Operation**: Indicates the type of change (`create`, `update`, `delete`, `snapshot`).
*   **Payload**:
    *   `Before`: (Only for `update` operations) `opencdc.StructuredData` representing the row state before the change.
    *   `After`: `opencdc.StructuredData` representing the row state after the change (or the current state for `snapshot` and `create`).
*   **Key**: Identifies the specific row. See Snapshot/CDC sections below for details.
*   **Position**: Represents the point in the data stream. It's a JSON object containing *either* a `snapshot_position` field *or* a `cdc_position` field, structured as described below.
*   **Metadata**: Contains standard OpenCDC fields plus:
    *   `mysql.server.id`: (CDC only) The originating MySQL server ID from the replication event header.
    *   Schema registry information (`opencdc.payload.schema.*`, `opencdc.key.schema.*`).

#### Snapshot Records

*   **Operation**: `snapshot`.
*   **Payload `After`**: `opencdc.StructuredData` with all column values.
*   **Key**:
    *   With Primary Key(s): `opencdc.StructuredData` using primary key columns and values.
    *   Without Primary Key(s) (using `LIMIT`/`OFFSET`): `opencdc.RawData` string `"<table_name>_<row_offset>"`.
*   **Position (`snapshot_position` field)**: A JSON object containing:
    *   `snapshots`: (object) Maps table names to their specific snapshot position object.
        *   *(Single Primary Key Table)*: Contains `last_read` (any) and `snapshot_end` (any).
        *   *(Multiple Primary Key Table)*: Contains an array of objects, each with `key_name` (string), `last_read` (any), and `snapshot_end` (any).
        *   *(No Primary Key Table)*: Table entry might be missing or empty (offset is not persisted).

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

## Requirements and compatibility

The connector is tested against MySQL v8.0. Compatibility with older versions isn't guaranteed.

### MySQL Server Requirements:

- Binary Log (binlog) must be enabled.
- Binlog format must be set to ROW.
- Binlog row image must be set to FULL.
- Tables must have **sortable** primary keys.

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
          # Prevents the connector from doing table snapshots and makes it start
          # directly in cdc mode.
          # Type: bool
          # Required: no
          snapshot.enabled: "false"
          # Limits how many rows should be retrieved on each database fetch on
          # snapshot mode.
          # Type: int
          # Required: no
          snapshot.fetchSize: "10000"
          # Allows a snapshot of a table with neither a primary key nor a
          # defined sorting column. The opencdc.Position won't record the last
          # record read from a table.
          # Type: bool
          # Required: no
          snapshot.unsafe: "false"
          # Allows to force using a custom column to sort the snapshot.
          # Type: string
          # Required: no
          tableConfig.*.sortingColumn: ""
          # Maximum delay before an incomplete batch is read from the source.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
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
          # (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).
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
