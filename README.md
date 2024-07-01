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

The record payload consists of [sdk.StructuredData](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk@v0.9.1#StructuredData), with each key being a column and each value being that column's value.

### Snapshot mode

Snapshot mode is the first stage of the source sync process. It reads all rows from the configured tables as record snapshots.

### CDC mode (planned)

CDC mode is the second stage of the source sync process. It listens to the configured tables for changes and pushes them to downstream resources.

### Configuration

| name       | description                                            | required | default value |
| ---------- | ------------------------------------------------------ | -------- | ------------- |
| `host`     | The hostname or IP address of the MySQL server         | true     |               |
| `port`     | The port number on which the MySQL server is listening | false    | 3306          |
| `user`     | The MySQL user account name for authentication         | true     |               |
| `password` | The password for the MySQL user account                | true     |               |
| `database` | The name of the specific MySQL database to connect to  | true     |               |
| `tables`   | The list of tables to pull data from                   | true     |               |

## Planned work

- [ ] Support for source connector cdc mode
- [ ] Support for destination connector
- [ ] Support for [multicollection writes](https://meroxa.com/blog/conduit-0.10-comes-with-multiple-collections-support/)
