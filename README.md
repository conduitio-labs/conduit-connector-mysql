# Conduit Connector for MySQL

[Conduit](https://conduit.io) connector for Mysql.

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all the unit tests. Run `make test-integration` to run
the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the
required resource locally. It includes [adminer](https://www.adminer.org/) for
database management.

## Source

A source connector pulls data from an external resource and pushes it to
downstream resources via Conduit.

### Snapshot mode

Snapshot mode is the first stage of the source sync process. It reads all rows
from the configured tables as record snapshots.

In snapshot mode, the record payload consists of
[opencdc.StructuredData](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk@v0.9.1#StructuredData),
with each key being a column and each value being that column's value.

### Change Data Capture mode

When the connector switches to CDC mode, it starts streaming changes from the
obtained position at the start of the snapshot. It uses the row-based binlog format
to capture detailed changes at the individual row level.

### Configuration

| name     | description                                                                                                                                                                                                                           | required | default value |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ------------- |
| `url`    | The connection URL of the MySQL, in the [following format](https://github.com/go-sql-driver/mysql?tab=readme-ov-file#dsn-data-source-name): `[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]`    | true     |               |
| `tables` | The list of tables to pull data from                                                                                                                                                                                                  | true     |               |

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

## Destination

The mysql destination takes a `record.Record` and parses it into a valid SQL query. Each record is individually parsed and upserted, and write batching is planned to be implemented.

### Upsert Behavior

If the target table contains a column with a unique constraint (this includes PRIMARY KEY and UNIQUE indexes), records will be upserted; otherwise, they will be appended. Support for updating tables without unique constraints is tracked [here](https://github.com/conduitio-labs/conduit-connector-mysql/issues/66).

### Multicollection mode

(Planned to do). You can upvote [the following issue](https://github.com/conduitio-labs/conduit-connector-mysql/issues/13) to add more interest on getting this feature implemented sooner.

### Configuration Options

| name    | description                                                                                                                                                                           | required | default                                      |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | -------------------------------------------- |
| `url`   | Connection string for the mysql database.                                                                                                                                          | true     |                                              |
| `table` | The target table to write the record to | true    |  |
| `key`   | Key represents the column name to use to delete records.                                                                                                 | false    |                                              |

