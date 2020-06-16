//! Telemetry Bot

mod debug;
mod parser;
mod schema;
mod scrape;

use anyhow::{Context, Result}; // alias std::result::Result with dynamic error type
use chrono::prelude::*;
use futures::channel::oneshot;
use futures::stream::StreamExt;
use heck::SnakeCase;
use parallel_stream::prelude::*;
use parking_lot::RwLock; // guaranteed to be eventually fair
use sqlx::prelude::*;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use crate::debug::DebugMetrics;
use crate::schema::*;
use crate::scrape::{ScrapeList, ScrapeTarget};

/// Whether to log (verbose) error output.
/// Use the `ERROR_LOGGER` env var to override (on, off)
static ERROR_LOGGER: AtomicBool = AtomicBool::new(false);

/// Counters for debug metrics
static DEBUG: DebugMetrics = DebugMetrics::new();

/// How frequently to collect metric timeseries data.
/// Use the `DEBUG_INTERVAL` env var to override (on, off, NUM_SECONDS)
const DEFAULT_DEBUG_INTERVAL: Duration = Duration::from_secs(300);

/// How frequently to update our pod list.
/// Use the `WATCH_INTERVAL` env var to override.
const DEFAULT_WATCH_INTERVAL: Duration = Duration::from_secs(30);

/// How frequently to collect metric timeseries data.
/// Use the `SCRAPE_INTERVAL` env var to override.
const DEFAULT_SCRAPE_INTERVAL: Duration = Duration::from_secs(15);

/// How long to wait before skipping a scrape endpoint.
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(1);

/// The program's main entry point.
fn main() -> Result<()> {
    let (send_shutdown, recv_shutdown) = oneshot::channel::<()>();

    // When we receive a SIGINT (or SIGTERM) signal, begin exiting.
    let signal_once = Cell::new(Some(send_shutdown));
    ctrlc::set_handler(move || {
        if let Some(sender) = signal_once.take() {
            sender.send(()).expect("failed to shutdown");
        }
    })?;

    // Start the main event loop
    async_std::task::block_on(run(recv_shutdown))
}

/// The main thread's event loop
async fn run(shutdown: oneshot::Receiver<()>) -> Result<()> {
    // Load configuration from environment variables
    let error_logger_enabled = match dotenv::var("ERROR_LOGGER").ok() {
        Some(val) if val == "true" || val == "on" || val == "1" => true,
        Some(val) if val == "false" || val == "off" || val == "0" || val == "" => false,
        Some(val) => val.parse::<bool>().context("invalid ERROR_LOGGER")?,
        None => false,
    };
    ERROR_LOGGER.store(error_logger_enabled, Ordering::Relaxed);
    let debug_interval_secs = match dotenv::var("DEBUG_INTERVAL").ok() {
        None => Some(DEFAULT_DEBUG_INTERVAL),
        Some(val) if val == "true" || val == "on" || val == "1" => Some(DEFAULT_DEBUG_INTERVAL),
        Some(val) if val == "false" || val == "off" || val == "0" || val == "" => None,
        Some(val) => {
            let secs = val.parse().context("invalid DEBUG_INTERVAL")?;
            Some(Duration::from_secs(secs))
        }
    };
    let watch_interval_secs = match dotenv::var("WATCH_INTERVAL").ok() {
        Some(val) => {
            let secs = val.parse().context("invalid WATCH_INTERVAL")?;
            Duration::from_secs(secs)
        }
        None => DEFAULT_WATCH_INTERVAL,
    };
    let scrape_interval_secs = match dotenv::var("SCRAPE_INTERVAL").ok() {
        Some(val) => {
            let secs = val.parse().context("invalid SCRAPE_INTERVAL")?;
            Duration::from_secs(secs)
        }
        None => DEFAULT_SCRAPE_INTERVAL,
    };
    let scrape_static_labels = match dotenv::var("SCRAPE_STATIC_LABELS").ok() {
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
                    _ => Err(anyhow::format_err!("invalid SCRAPE_STATIC_LABELS")),
                }
            })
            .collect::<Result<_, _>>()?,
        _ => Vec::new(),
    };
    let scrape_static_labels: &'static _ = Box::leak(scrape_static_labels.into_boxed_slice());
    let scrape_concurrency: usize = match dotenv::var("SCRAPE_CONCURRENCY").ok() {
        Some(val) => val.parse().context("invalid SCRAPE_CONCURRENCY")?,
        None => 128,
    };
    let db_pool_size = match dotenv::var("DATABASE_POOL_SIZE").ok() {
        Some(val) => val.parse().context("invalid DATABASE_POOL_SIZE")?,
        None => 8,
    };

    // Open a sqlx connection pool
    println!("Connected to database...");
    let db_url = dotenv::var("DATABASE_URL").context("missing DATABASE_URL")?;
    let db = sqlx::postgres::PgPool::builder()
        .max_size(db_pool_size)
        .build(&db_url)
        .await
        .context("connecting to timescaledb")?;
    println!("Connected to {}", db_url);

    // Load known metrics from the database
    println!("Loading metrics metadata...");
    let metrics: Vec<(String, String, String, String, Vec<String>)> =
        sqlx::query_as("SELECT name, table_name, schema_name, series_type, label_columns FROM telemetry_catalog.metrics_tables")
            .fetch_all(&db)
            .await?;
    let mut tables: HashMap<String, &'static SeriesTable> = HashMap::with_capacity(metrics.len());
    for (metric, table, schema, type_, labels) in metrics {
        let key = metric.clone();
        let type_ = type_.parse().expect("invalid metric type");
        let table = SeriesTable::new(metric, table, schema, type_, labels, true);
        tables.insert(key, Box::leak(Box::new(table)));
    }
    println!("Loaded {} metrics.", tables.len());

    // Collect the list of pods to be scraped for metrics
    println!("Loading scrape targets...");
    let kube_client = kube::Client::try_default()
        .await
        .context("reading kubernetes api config")?;
    let pods = ScrapeList::shared(kube_client);
    pods.refresh().await.context("listing prometheus pods")?;
    println!("Loaded {} scrape targets...", pods.len());

    // Every debug interval, log debug informationc
    let debug_interval = match debug_interval_secs {
        Some(duration) => Some(async_std::task::spawn(async move {
            let mut interval = async_std::stream::interval(duration);
            while let Some(_) = interval.next().await {
                DEBUG.publish();
            }
        })),
        None => None,
    };

    // Every WATCH_INTERVAL, update our list of pods
    let watch_interval = {
        let pods = pods.clone();
        async_std::task::spawn(async move {
            let mut interval = async_std::stream::interval(watch_interval_secs);
            while let Some(_) = interval.next().await {
                match pods.refresh().await {
                    Ok(()) => DEBUG.update_pods(pods.len()),
                    Err(err) => {
                        DEBUG.polling_failed();
                        debug_error(err);
                    }
                }
            }
        })
    };

    // Every SCRAPE_INTERVAL (at most), scrape each endpoint and write it to the database
    let db: &'static _ = Box::leak(Box::new(db));
    let tables: &'static _ = Box::leak(Box::new(RwLock::new(tables)));
    let scrape_interval = async_std::task::spawn(async move {
        loop {
            let start = Instant::now();

            // Get the current list of pods to iterate over
            let targets = pods.get();

            // Scrape the targets with a given `max_concurrency`
            targets
                .into_par_stream()
                .limit(scrape_concurrency)
                .map(move |target| {
                    scrape_once(db, tables, scrape_static_labels, Arc::clone(&target))
                })
                .collect::<Vec<()>>()
                .await;

            // Sleep until the next scrape interval
            if let Some(delay) = scrape_interval_secs.checked_sub(start.elapsed()) {
                async_std::task::sleep(delay).await;
            }
        }
    });

    // Shutdown when the process is killed
    shutdown.await?;
    watch_interval.cancel().await;
    scrape_interval.cancel().await;
    if let Some(debug_interval) = debug_interval {
        debug_interval.cancel().await;
    }

    Ok(())
}

async fn scrape_once(
    db: &'static sqlx::postgres::PgPool,
    tables: &'static RwLock<HashMap<String, &'static SeriesTable>>,
    static_labels: &'static [(String, String)],
    target: Arc<ScrapeTarget>,
) {
    // Get the current timestamp (w/o nanoseconds); we don't need that level of precision
    let scrape_time = {
        let raw = Utc::now().naive_utc();
        raw.with_nanosecond(0).unwrap_or(raw)
    };
    match target.scrape().await {
        Ok(input) => {
            DEBUG.scrape_succeeded();

            // Define a vec with all of the static labels + per-endpoint labels
            let mut extra_labels = Vec::with_capacity(static_labels.len() + target.labels.len());
            for label in static_labels {
                extra_labels.push(label.clone());
            }
            for (label, value) in &target.labels {
                if let Some(value) = value {
                    extra_labels.push((label.to_string(), value.clone()));
                }
            }

            // Insert each value into postgres, create new tables as necessary
            let (metrics, values) = parser::parse(&input);
            for value in values {
                let time = value
                    .timestamp
                    .map(|t| NaiveDateTime::from_timestamp(t, 0))
                    .unwrap_or(scrape_time);
                let key = value.name;

                // Load the table for this metric
                let mut table: Option<&'static SeriesTable> = (*tables.read()).get(key).copied();
                if table.is_none() {
                    if let Some(type_) = metrics.get(value.name) {
                        tables.write().entry(key.to_string()).or_insert_with(|| {
                            Box::leak(Box::new(define_series(*type_, &value, &extra_labels)))
                        });
                        table = (*tables.read()).get(key).copied();
                    }
                }

                // Insert the data point
                if let Some(table) = table {
                    match table.insert(&db, time, value, &extra_labels).await {
                        Ok(_) => DEBUG.write_succeeded(),
                        Err(err) => {
                            DEBUG.write_failed();
                            debug_error(err);
                        }
                    }
                }
            }
        }
        Err(err) => {
            DEBUG.scrape_failed();
            debug_error(err);
        }
    }
}

fn define_series(
    type_: SeriesType,
    sample: &parser::Measurement,
    static_labels: &[(String, String)],
) -> SeriesTable {
    let name = sample.name.to_owned();

    // Postgres table names have a max length of 63 characters.
    //
    // We truncate to 48 characters so that we can have useful index names for the table.
    let schema = "telemetry_metrics".into();
    let table = {
        let mut snake_name = name.to_snake_case();
        snake_name.truncate(48);
        snake_name.trim_end_matches('_').to_owned()
    };

    // Collect the list of expected labels from a sample measurement
    let mut labels = Vec::new();
    for (label, _) in static_labels {
        labels.push(label.clone());
    }
    for (label, _) in &sample.labels {
        if *label != "time"
            && *label != "value"
            && !label.starts_with("__")
            && label
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '_' || ch == '-')
        {
            labels.push(label.to_string());
        }
    }
    SeriesTable::new(name, table, schema, type_, labels, false)
}

fn debug_error(err: anyhow::Error) {
    eprintln!("Error: {}", err);
    for err in err.chain().skip(1) {
        eprintln!("Caused by: {}", err);
    }
}
