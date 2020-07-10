# Telemetry Core
This crate/library implements scraping of Prometheus endpoints in a Kubernetes cluster.

It provides the core functionality of `telemetry-bot` to detect and scrape `/metrics` endpoints,
regardless of which backend storage telemetry bot might be using.
