# Telemetry Bot
A telemetry exporter that scrapes Prometheus and Jaeger and saves it  
to a compact Postgres-based time series database.

![Fact Core](doc/bot.png)



## Development
How do I do `X` in this rust project?

### Setup Environment
Install Rust via curl-pipe:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Commands

 - `cargo run` – run (like a script) with debug symbols
 - `cargo build` – compile with debug symbols (for use with a debugger); outputs to `target/debug/telemetry-bot`
 - `cargo build --release` – compile optimized build for production; outputs to `target/release/telemetry-bot`
 - `cargo doc --open` – build and open API docs for this project and all of its dependencies
 - `cargo test` – run integration and unit tests

### Libraries for X:
 - __surf:__ http client
 - __sqlx:__ database client
 - __async-std:__ async IO (files, sockets, etc)
