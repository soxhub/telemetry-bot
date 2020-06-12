# ===== BUILD =====
FROM rust:latest as BASE

RUN mkdir -p /src/telemetry-bot
COPY . /src/telemetry-bot
WORKDIR /src/telemetry-bot
RUN cargo build --release
RUN cargo build --release -p migrate

# ===== EXEC =====
FROM debian:stretch as EXEC

COPY --from=BASE /src/telemetry-bot/target/release/telemetry-bot /bin/
COPY --from=BASE /src/telemetry-bot/target/release/migrate /bin/
