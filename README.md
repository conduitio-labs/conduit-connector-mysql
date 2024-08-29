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

It (will) operate in two modes: snapshot and CDC. Currently only snapshot mode
is supported.

### Snapshot mode

Snapshot mode is the first stage of the source sync process. It reads all rows
from the configured tables as record snapshots.

In snapshot mode, the record payload consists of
[opencdc.StructuredData](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk@v0.9.1#StructuredData),
with each key being a column and each value being that column's value.

The MySQL user account used needs the following privileges for the snapshot mode:

- SELECT: To read data from the source tables.
- LOCK TABLES: To acquire read locks on tables during snapshotting.
- RELOAD: To execute the FLUSH TABLES command.
- REPLICATION CLIENT: To obtain the binary log position.

### Change Data Capture mode

When the connector switches to CDC mode, it starts streaming changes from the
obtained position at the start of the snapshot. It uses the row-based binlog format
to capture detailed changes at the individual row level.

The MySQL user account used needs the following privileges for the CDC mode:

- REPLICATION CLIENT: To obtain the binary log position and execute SHOW MASTER STATUS.
- REPLICATION SLAVE: For reading the binary log.

It needs the MySQL server to be configured to use row-based format and full
binlog row image. You can verify these settings with these:

```sql
SHOW VARIABLES WHERE Variable_name IN ('binlog_format', 'binlog_row_image');
```

### Configuration

| name     | description                                                                                                                                         | required | default value |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ------------- |
| `url`    | The connection URL of the MySQL, in the following format (\*): [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN] | true     |               |
| `tables` | The list of tables to pull data from                                                                                                                | true     |               |

(\*): You can find more information
[here](https://github.com/go-sql-driver/mysql?tab=readme-ov-file#dsn-data-source-name).
For example: `username:password@tcp(127.0.0.1:3306)/dbname`

## Planned work

- [ ] Support for destination connector
- [ ] Support for [multicollection writes](https://meroxa.com/blog/conduit-0.10-comes-with-multiple-collections-support/)
