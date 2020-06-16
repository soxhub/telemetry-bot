# Telemetry Schema
This crate implements a telemetry bot storage backend using a custom TimescaleDB-based database schema.

This backend can be used to scrape prometheus metrics into a postgres database if you want to
avoid running the TimescaleDB [Prometheus Connector](https://github.com/timescale/timescale-prometheus).
