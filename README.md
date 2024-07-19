# Conduit Connector for MySQL

[Conduit](https://conduit.io) connector for Mysql.

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all the unit tests. Run `make test-integration` to run the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource locally. It includes [adminer](https://www.adminer.org/) for database management.

## Source

A source connector pulls data from an external resource and pushes it to downstream resources via Conduit.
It (will) operate in two modes: snapshot and CDC. Currently only snapshot mode is supported.

### Snapshot mode

Snapshot mode is the first stage of the source sync process. It reads all rows from the configured tables as record snapshots.

In snapshot mode, the record payload consists of [sdk.StructuredData](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk@v0.9.1#StructuredData), with each key being a column and each value being that column's value.

### CDC mode (planned)

CDC mode is the second stage of the source sync process. It listens to the configured tables for changes and pushes them to downstream resources.

### Configuration

| name     | description                                                                                                                                         | required | default value |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ------------- |
| `url`    | The connection URL of the MySQL, in the following format (\*): [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN] | true     |               |
| `tables` | The list of tables to pull data from                                                                                                                | true     |               |

(\*): You can find more information [here](https://github.com/go-sql-driver/mysql?tab=readme-ov-file#dsn-data-source-name). For example: `username:password@tcp(127.0.0.1:3306)/dbname`

## Planned work

- [ ] Support for source connector cdc mode
- [ ] Support for destination connector
- [ ] Support for [multicollection writes](https://meroxa.com/blog/conduit-0.10-comes-with-multiple-collections-support/)
