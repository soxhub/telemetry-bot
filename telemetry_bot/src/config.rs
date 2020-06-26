use anyhow::{Context, Result};
use heck::SnakeCase;
use std::str::FromStr;
use std::time::Duration;
use structopt::StructOpt;

#[derive(Debug)]
pub struct Config {
    /// Enables verbose logging of errors that occur while scraping metrics
    pub debug: bool,

    /// How frequently (in seconds) to log internal debug metrics
    pub debug_interval: Option<Duration>,

    /// How frequently (in seconds) to poll or wait before retrying failed requests to
    /// the Kubernetes api when querying the list of pods to scrape.
    pub watch_interval: Duration,

    /// How frequently (in seconds) to collect timeseries data from "/metrics" endpoints.
    pub scrape_interval: Duration,

    /// The maximum number of network sockets to use for requests to "/metrics".
    pub scrape_concurrency: u16,

    /// A comma separated list of static labels to add to every scraped metric.
    /// e.g. "environment=production,version=0.5.0"
    pub scrape_labels: Vec<(String, String)>,

    /// Specifies a single specific url which should be scraped.
    /// If set, telemetry bot will not attempt to discover scrape targets from kubernetes.
    pub scrape_target: Option<String>,

    /// Either "standalone" or "remote", this option is required.
    pub storage_type: String,

    /// When STORAGE_TYPE is "standalone", this option is required.
    /// The url to connect to TimescaleDB.
    /// e.g. "postgres://postgres@localhost/defaultdb"
    pub database_url: Option<String>,

    /// The maximum number of database connections to allow at once to TimescaleDB.
    pub database_pool_size: u16,

    /// When STORAGE_TYPE is "remote", this option is required.
    /// The url to send prometheus remote write requests too; it should include the path.
    /// e.g. "https://localhost/write"
    pub remote_write_url: Option<String>,
}

impl Config {
    /// Loads configuration from arguments, env and dotenv
    pub fn load() -> Result<Config> {
        // Attempts to find a `.env` file to initialize/extend the environment
        dotenv::dotenv().context("error loading configuration")?;

        // Load the config from arguments, then environment variables
        let env = Environment::from_args();

        // Perform custom handling for some environment variables
        Ok(Config {
            debug: env.debug
                || match dotenv::var("DEBUG").ok() {
                    Some(val) if val == "true" || val == "on" || val == "1" => true,
                    Some(val) if val == "false" || val == "off" || val == "0" || val == "" => false,
                    Some(val) => val.parse::<bool>().context("invalid DEBUG")?,
                    None => false,
                },
            debug_interval: match env.debug_interval {
                ToggleValue::Enabled(secs) => Some(Duration::from_secs(secs)),
                ToggleValue::EnabledDefault => Some(Duration::from_secs(300)),
                ToggleValue::Disabled => None,
            },
            watch_interval: Duration::from_secs(env.watch_interval),
            scrape_interval: Duration::from_secs(env.scrape_interval),
            scrape_concurrency: if env.scrape_concurrency == 0 {
                4098
            } else {
                env.scrape_concurrency
            },
            scrape_labels: match env.scrape_labels {
                Some(val) if !val.is_empty() => val
                    .split(',')
                    .map(|name_value| {
                        let name_value = name_value.splitn(2, '=').collect::<Vec<_>>();
                        match name_value.as_slice() {
                            [name, value]
                                if !value.is_empty()
                                    && !name.is_empty()
                                    && *name == name.to_snake_case() =>
                            {
                                Ok((name.to_string(), value.to_string()))
                            }
                            _ => Err(anyhow::format_err!("invalid SCRAPE_LABELS")),
                        }
                    })
                    .collect::<Result<_, _>>()?,
                _ => Vec::new(),
            },
            scrape_target: env.scrape_target,
            storage_type: env.storage_type,
            database_url: env.database_url,
            database_pool_size: env.database_pool_size,
            remote_write_url: env.remote_write_url,
        })
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "telemetry-bot")]
struct Environment {
    /// Enables verbose logging of errors that occur while scraping metrics
    #[structopt(short, long)]
    debug: bool,

    /// How frequently (in seconds) to log internal debug metrics
    #[structopt(long, env = "DEBUG_INTERVAL", default_value = "300")]
    debug_interval: ToggleValue<u64>,

    /// How frequently (in seconds) to wait before retrying k8s errors
    #[structopt(long, env = "WATCH_INTERVAL", default_value = "30")]
    watch_interval: u64,

    /// How frequently (in seconds) to scrape from "/metrics" endpoints
    #[structopt(long, env = "SCRAPE_INTERVAL", default_value = "15")]
    scrape_interval: u64,

    /// The max number of network sockets to use for "/metrics" requests
    #[structopt(long, env = "SCRAPE_CONCURRENCY", default_value = "4096")]
    scrape_concurrency: u16,

    /// A comma separated list of static labels to add to metrics
    #[structopt(long, env = "SCRAPE_LABELS")]
    scrape_labels: Option<String>,

    /// Specifies a single specific url which should be scraped
    #[structopt(short = "t", long, env = "SCRAPE_TARGET")]
    scrape_target: Option<String>,

    /// One of "standalone", "compat" or "remote".
    #[structopt(short = "s", long = "storage", env = "STORAGE_TYPE")]
    storage_type: String,

    /// The url to connect to timescaledb (if storage is "standalone" or "compat")
    #[structopt(env = "DATABASE_URL")]
    //, required_if("storage", "standalone"), required_if("storage", "compat")
    database_url: Option<String>,

    /// The maximum number of database connections to allow at once
    #[structopt(env = "DATABASE_POOL_SIZE", default_value = "8")]
    database_pool_size: u16,

    /// The url to send prometheus remote write requests too
    #[structopt(long, env = "REMOTE_WRITE_URL")] //, required_if("storage", "remote")
    remote_write_url: Option<String>,

    /// The hostname of the kubernetes http api; usually set by kubernetes
    #[structopt(env = "KUBERNETES_SERVICE_HOST")]
    _k8s_host: Option<String>,

    /// The port of the kubernetes http api; usually set by kubernetes
    #[structopt(env = "KUBERNETES_SERVICE_PORT")]
    _k8s_port: Option<u16>,
}

#[derive(Debug, Eq, PartialEq)]
enum ToggleValue<T: FromStr> {
    Enabled(T),
    EnabledDefault,
    Disabled,
}

impl<T: FromStr> Default for ToggleValue<T> {
    fn default() -> Self {
        ToggleValue::Disabled
    }
}

impl<T: FromStr> FromStr for ToggleValue<T> {
    type Err = T::Err;
    fn from_str(val: &str) -> std::result::Result<Self, Self::Err> {
        match val {
            "false" | "off" | "0" | "" => Ok(ToggleValue::Disabled),
            "true" | "on" | "1" => Ok(ToggleValue::EnabledDefault),
            _ => val.parse().map(ToggleValue::Enabled),
        }
    }
}