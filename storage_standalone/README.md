# Storage Backend: Standalone
This crate implements a telemetry bot storage backend by writing metrics data to the same
database schema (e.g. `_prom_catalog`, `prom_data`, `prom_series`, etc...) that is used by
the TimescaleDB [Prometheus Connector](https://github.com/timescale/timescale-prometheus),

It is possible to use this storage backend either:

 - Running on its own (aka. "standalone"; w/o prometheus and w/o connector)
 - Running along side an existing Prometheus + Connector deployment
