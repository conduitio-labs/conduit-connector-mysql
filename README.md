# Conduit Connector for MySQL

[Conduit](https://conduit.io) connector for Mysql.

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all the unit tests. Run `make test-integration` to run the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource locally.

## Source

A source connector pulls data from an external resource and pushes it to downstream resources via Conduit.

### Configuration

| name       | description                                            | required | default value |
| ---------- | ------------------------------------------------------ | -------- | ------------- |
| `host`     | The hostname or IP address of the MySQL server         | true     |               |
| `port`     | The port number on which the MySQL server is listening | false    | 3306          |
| `user`     | The MySQL user account name for authentication         | true     |               |
| `password` | The password for the MySQL user account                | true     |               |
| `database` | The name of the specific MySQL database to connect to  | true     |               |
| `tables`   | The list of tables to pull data from                   | true     |               |

## Destination

A destination connector pushes data from upstream resources to an external resource via Conduit.

### Configuration

| name                       | description                                | required | default value |
| -------------------------- | ------------------------------------------ | -------- | ------------- |
| `destination_config_param` | Description of `destination_config_param`. | true     | 1000          |

## Known Issues & Limitations

- Known issue A
- Limitation A

## Planned work

- [ ] Item A
- [ ] Item B
