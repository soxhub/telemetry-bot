# ===== BUILD =====
FROM rust:latest as build

RUN mkdir -p /src/telemetry-bot
COPY . /src/telemetry-bot
WORKDIR /src/telemetry-bot
RUN cargo build --release
RUN cargo build --release -p migrate

# ===== DEPLOYMENT =====
FROM debian:stretch as deployment

RUN apt-get update
RUN apt-get install -y libssl-dev

COPY --from=build /src/telemetry-bot/target/release/telemetry-bot /bin/
COPY --from=build /src/telemetry-bot/target/release/migrate /bin/
