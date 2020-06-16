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
    ```

3. Ensure you are configured to connect to kubernetes

    ```sh
    test -f ~/.kube/config && echo "Ok" || echo "Missing config"
    ```

### Workflow
Common rust commands:

 - `cargo run` – run (like a script) with debug symbols
 - `cargo build` – compile with debug symbols (for use with a debugger); outputs to `target/debug/telemetry-bot`
 - `cargo build --release` – compile optimized build for production; outputs to `target/release/telemetry-bot`
 - `cargo doc --open` – build and open API docs for this project and all of its dependencies
 - `cargo test` – run integration and unit tests
 
To reset the database:

```sh
dropdb telemetry -U postgres \
    && createdb telemetry --owner=postgres -U postgres \
    && psql -U postgres -d telemetry -c 'CREATE EXTENSION timescaledb' \
    && cargo run -p migrate
```

### Libraries for X:
Links to online documentation for important libraries (almost always on `docs.rs/<CRATE>`). 

 - std lib: [std](https://doc.rust-lang.org/std/)
 - database client: [sqlx](https://docs.rs/sqlx)
