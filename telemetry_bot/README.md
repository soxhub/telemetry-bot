# Telemetry Bot CLI
This crate implements the `telemetry-bot` command line interface.

## Configuration
Telemetry bot is configured via environment variables.

Environment variables will first be loaded from a `.env` file ("dotenv"), if present;
and then as usual from the process's environment.

## Environment Variables

```
STORAGE_TYPE

    Either "standalone" or "remote"

KUBERNETES_SERVICE_HOST

    The hostname of the kubernetes api; usually (but not always) set by kubernetes.
    Telemetry bot will first attempt to load its kubernetes configuration assuming
    it is [running in a pod](https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster/#accessing-the-api-from-a-pod),
    then will fallback to loading configuration from ~/.kube/config.

REMOTE_WRITE_URL

    When STORAGE_TYPE is "remote", this variable is required.
    The url to send prometheus remote write requests too; it should include the path.

    e.g. "https://localhost/write"

DATABASE_URL

    When STORAGE_TYPE is "standalone", this variable is required.
    The url to connect to TimescaleDB.

    e.g. "postgres://postgres@localhost/defaultdb"

DATABASE_POOL_SIZE

    The maximum number of database connections to allow at once to TimescaleDB.

SCRAPE_STATIC_LABELS

    A comma separated list of static labels to add to every scraped metric.

    e.g. "environment=production,version=0.5.0"

SCRAPE_CONCURRENCY

    The maximum number of network sockets to use for requests to "/metrics".
    (default: 128)

SCRAPE_INTERVAL

    How frequently (in seconds) to collect timeseries data from "/metrics" endpoints.
    (default: 15 seconds)

WATCH_INTERVAL

    How frequently (in seconds) to poll Kubernetes for the list of pods to scrape.
    (default: 30 seconds)

DEBUG_INTERVAL

    How frequently (in seconds) to log debug interval (can also be: false, off, 0, '').
    (default: 300 seconds)

ERROR_LOGGER

    Enables (very verbose) logging of errors that occur while scraping metrics.
    (default: false, off, 0)

```
