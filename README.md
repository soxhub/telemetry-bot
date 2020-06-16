# Telemetry Bot
A telemetry exporter that scrapes Prometheus `/metrics` endpoints
and Jaeger api and exports timeseries to TimescaleDB.

![Fact Core](doc/bot.png)

## Development
How do I do `X` in this rust project?

### Setup Environment

1. Install Rust via curl-pipe

   ```sh
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. Create a development database w/ TimescaleDB (instructions for macOS)

    ```sh
    # Install postgres
    brew install postgres

    # Install Timescale DB
    brew tap timescale/tap
    brew install timescaledb
    /usr/local/bin/timescaledb_move.sh
    timescaledb-tune
    brew services restart postgresql

    # Create database
    createuser postgres -s || echo "skipped"
    createdb telemetry --owner=postgres -U postgres
    psql -U postgres -d telemetry -c 'CREATE EXTENSION timescaledb'

    # Run migrations
    cargo run -p telemetry-schema telemetry-migrate
    ```

3. Ensure you are configured to connect to kubernetes

    ```sh
    test -f ~/.kube/config && echo "Ok" || echo "Missing config"
    ```

### Workflow
Common rust commands:

 - `cargo run` – run `telemetry-bot` with debug symbols
 - `cargo check` – check if the project typechecks (faster than `cargo build` when iterating on code)
 - `cargo build` – compile executables with debug symbols (for use with a debugger); outputs to `target/debug/telemetry-bot`
 - `cargo build --all-targets --release` – compile optimized executables for production; outputs to `target/release/telemetry-bot`
 - `cargo doc --open` – build and open API docs for this project and all of its dependencies
 - `cargo test` – run integration and unit tests
 
To reset the database:

```sh
dropdb telemetry -U postgres \
    && createdb telemetry --owner=postgres -U postgres \
    && psql -U postgres -d telemetry -c 'CREATE EXTENSION timescaledb' \
    && cargo run -p telemetry-schema telemetry-migrate
```
