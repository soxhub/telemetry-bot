//! Telemetry Bot

mod storage;

use anyhow::{Context, Result}; // alias std::result::Result with dynamic error type
use chrono::prelude::*;
use futures::channel::oneshot;
use futures::stream::StreamExt;
use heck::SnakeCase;
use parallel_stream::prelude::*;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use telemetry_prometheus::debug::DEBUG;
use telemetry_prometheus::error::debug_error;
use telemetry_prometheus::parser;
use telemetry_prometheus::scrape::{ScrapeList, ScrapeTarget};

use crate::storage::Storage;

/// Whether to log (verbose) error output.
/// Use the `ERROR_LOGGER` env var to override (on, off)
static ERROR_LOGGER: AtomicBool = AtomicBool::new(false);

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
        // The first time we receive the signal, shutdown gracefully
        if let Some(sender) = signal_once.take() {
            sender.send(()).expect("failed to shutdown");
        }
        // The second time we receive the signal, shutdown immediately
        else {
            std::process::exit(1);
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

    // Connect to storage provider
    let store: &'static _ = Box::leak(storage::from_env().await?);

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
    let scrape_interval = async_std::task::spawn(async move {
        loop {
            let start = Instant::now();

            // Get the current list of pods to iterate over
            let targets = pods.get();

            // Scrape the targets with a given `max_concurrency`
            targets
                .into_par_stream()
                .limit(scrape_concurrency)
                .map(move |target| scrape_target(store, scrape_static_labels, Arc::clone(&target)))
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

async fn scrape_target(
    store: &'static dyn Storage,
    global_labels: &'static [(String, String)],
    target: Arc<ScrapeTarget>,
) {
    // Get the current timestamp (w/o nanoseconds); we don't need that level of precision
    let timestamp = {
        let raw = Utc::now().naive_utc();
        raw.with_nanosecond(0).unwrap_or(raw).timestamp()
    };
    match target.scrape(SCRAPE_TIMEOUT).await {
        Ok(input) => {
            DEBUG.scrape_succeeded();

            // Define a vec with all of the static labels + per-endpoint labels
            let mut static_labels = Vec::with_capacity(global_labels.len() + target.labels.len());
            for label in global_labels {
                static_labels.push(label.clone());
            }
            for (label, value) in &target.labels {
                if let Some(value) = value {
                    static_labels.push((label.to_string(), value.clone()));
                }
            }

            // Write the samples to the backing storage
            let (metrics, samples) = parser::parse(&input);
            if samples.is_empty() {
                return;
            }

            let (sent, errors) = store
                .write(timestamp, metrics, samples, &static_labels)
                .await;
            if sent > 0 {
                DEBUG.writes_succeeded(sent);
            }
            if !errors.is_empty() {
                DEBUG.writes_failed(errors.len());
                for err in errors {
                    debug_error(err);
                }
            }
        }
        Err(err) => {
            DEBUG.scrape_failed();
            debug_error(err);
        }
    }
}
