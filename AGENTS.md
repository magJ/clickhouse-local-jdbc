# clickhouse-local-jdbc

A JDBC driver that executes queries via a local `clickhouse-local` binary.

## Building

```
./gradlew build
```

## Testing

**Both test suites MUST be run and pass before finishing any changes.**

```
./gradlew test integrationTest
```

- `test` — unit tests (no external dependencies)
- `integrationTest` — requires a `clickhouse-local` binary on `$PATH`, or pass `-DclickhouseLocalPath=<path>` to specify its location

### Installing clickhouse-local for development

Use the provided script to download a pinned binary to `./clickhouse-bin/`:

```
./install-clickhouse.sh
```

Then run integration tests with the local binary:

```
./gradlew test integrationTest -DclickhouseLocalPath=./clickhouse-bin/clickhouse-local
```
