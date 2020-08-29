# Storage Backend: Remote Write
This crate implements a telemetry bot storage backend using the Prometheus [remote write protocol][1].

This backend is generally intended to be run with the TimescaleDB [prometheus connector][2],
but could be used with any compatible remote write implementation.

[1]: https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations "Prometheus Remote Storage Protocol"
[2]: https://github.com/timescale/timescale-prometheus "TimescaleDB Prometheus Connector"
