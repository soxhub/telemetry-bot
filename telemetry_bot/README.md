# Telemetry Bot CLI
This crate implements the `telemetry-bot` command line interface.

## Configuration
Telemetry bot is primarily configured via environment variables.

Environment variables will first be loaded from a `.env` file ("dotenv"), if present;
and then as usual from the process's environment.

### Standalone Example

```sh
STORAGE_TYPE=standalone
REMOTE_WRITE_URL="postgres://postgres@localhost/telemetry"
SCRAPE_LABELS="environment=production"
KUBERNETES_SERVICE_HOST="kubernetes.default.svc"
```

### Remote Example

```sh
STORAGE_TYPE=remote
REMOTE_WRITE_URL="http://timescale-prometheus-connector:9201/write"
SCRAPE_LABELS="environment=production"
KUBERNETES_SERVICE_HOST="kubernetes.default.svc"
```

## Storage Configuration
These options configure how telemetry bot stores metrics + telemetry data.

```
STORAGE_TYPE

    Either "standalone" or "remote"; this option is required.

REMOTE_WRITE_URL

    When STORAGE_TYPE is "remote", this option is required.
    The url to send prometheus remote write requests too; it should include the path.

    e.g. "https://localhost/write"

DATABASE_URL

    When STORAGE_TYPE is "standalone", this option is required.
    The url to connect to TimescaleDB.

    e.g. "postgres://postgres@localhost/defaultdb"

```

## Scrape Configuration
These options configure how telemetry bot collects + scrapes metrics from discovered scrape targets.

```
SCRAPE_TARGET

    Specifies a single specific url which should be scraped.
    If set, telemetry bot will not attempt to discover scrape targets from kubernetes.

SCRAPE_LABELS

    A comma separated list of static labels to add to every scraped metric.

    e.g. "environment=production,version=0.5.0"

SCRAPE_CONCURRENCY

    The maximum number of network sockets to use for requests to "/metrics".
    (default: 4096)

SCRAPE_INTERVAL

    How frequently (in seconds) to collect timeseries data from "/metrics" endpoints.
    (default: 15 seconds)

```

## Discovery Configuration
These options configure how telemetry bot discovers services to be scraped.

If `SCRAPE_TARGET` is set, these options are ignored. 

```
KUBERNETES_SERVICE_HOST

    The hostname of the kubernetes api; usually (but not always) set by kubernetes.
    Telemetry bot will first attempt to load its kubernetes configuration assuming
    it is [running in a pod](https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster/#accessing-the-api-from-a-pod),
    then will fallback to loading configuration from ~/.kube/config.


WATCH_INTERVAL

    How frequently (in seconds) to poll or wait before retrying failed requests to
    the Kubernetes api when querying the list of pods to scrape.
    (default: 30 seconds)

```

## Debugging & Telemetry
These options configure how telemetry is logged, published, and/or exposed.

```
DEBUG

    Enables verbose logging of errors that occur while scraping metrics.
    (default: false, off, 0)

DEBUG_INTERVAL

    How frequently (in seconds) to log internal debug metrics (can also be: false, off, 0, '').
    (default: 300 seconds)

```
