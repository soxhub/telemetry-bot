//! Telemetry Bot

mod config;
mod storage;

use anyhow::{Context, Result}; // alias std::result::Result with dynamic error type
use chrono::prelude::*;
use futures::stream::StreamExt;
use parallel_stream::prelude::*;
use std::cell::Cell;
use std::sync::Arc;
use std::time::Instant;

use telemetry_core::debug::DEBUG;
use telemetry_core::error::{debug_error, debug_error_enabled};
use telemetry_core::parser;
use telemetry_core::scrape::{ScrapeError, ScrapeList, ScrapeTarget};

use crate::config::*;
use crate::storage::Storage;

/// The program's main entry point.
fn main() -> Result<()> {
    let config = Config::load()?;
    debug_error_enabled(config.debug);

    // When we receive a SIGINT (or SIGTERM) signal, begin exiting.
    let (send_shutdown, recv_shutdown) = piper::chan::<()>(0);
    let signal_once = Cell::new(Some(send_shutdown));
    ctrlc::set_handler(move || {
        // The first time we receive the signal, shutdown gracefully
        if let Some(sender) = signal_once.take() {
            // Closing the channel will cause the sender to shutdown
            std::mem::drop(sender);
        }
        // The second time we receive the signal, shutdown immediately
        else {
            std::process::exit(1);
        }
    })?;

    // Create a work-stealing thread pool.
    let num_threads = num_cpus::get().max(1);
    let mut threads = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        let shutdown = recv_shutdown.clone();
        threads.push(std::thread::spawn(move || {
            async_std::task::block_on(shutdown.recv())
        }));
    }

    // Start the main event loop
    async_std::task::block_on(run(config, recv_shutdown))
}

/// The main thread's event loop
async fn run(config: Config, shutdown: piper::Receiver<()>) -> Result<()> {
    // Allocate static memory for configuration
    let config: &'static _ = Box::leak(Box::new(config));

    // Connect to storage provider
    let store: &'static _ = Box::leak(storage::from_config(config).await?);

    // Collect the list of pods to be scraped for metrics
    let pods = if let Some(scrape_url) = &config.scrape_target {
        let kube_client =
            kube::Client::new(kube::Config::new("https://localhost".parse().unwrap()));
        let pods = ScrapeList::shared(kube_client, Vec::new());
        pods.update(vec![ScrapeTarget::new(
            "default".into(),
            scrape_url.into(),
            Vec::new(),
        )]);
        println!("Target: {}", scrape_url);
        pods
    } else {
        println!("Loading scrape targets...");
        let mut annotations = Vec::new();
        if config.watch_prometheus_io {
            annotations.push("prometheus.io/scrape")
        }
        if config.watch_telemetry_bot {
            annotations.push("telemetry.bot/scrape")
        }
        let kube_client = kube::Client::try_default()
            .await
            .context("reading kubernetes api config")?;
        let pods = ScrapeList::shared(kube_client, annotations);
        pods.refresh()
            .await
            .context("error listing prometheus pods")?;
        println!("Loaded {} scrape targets...", pods.len());
        pods
    };

    // Every debug interval, log debug informationc
    let debug_interval = match config.debug_interval {
        Some(duration) => Some(async_std::task::spawn(async move {
            let mut interval = async_std::stream::interval(duration);
            while let Some(_) = interval.next().await {
                DEBUG.publish();
            }
        })),
        None => None,
    };

    // Every WATCH_INTERVAL, update our list of pods
    let watch_interval = if config.scrape_target.is_none() {
        let pods = pods.clone();
        Some(async_std::task::spawn(async move {
            /* Watch pods using Kubenertes informer */
            loop {
                match pods.watch().await {
                    Ok(_) => (),
                    Err(err) => {
                        DEBUG.polling_failed();
                        debug_error(err.into());

                        // On failure, wait WATCH_INTERVAL seconds before retrying
                        async_std::task::sleep(config.watch_interval).await;
                    }
                }
            }
        }))
    } else {
        None
    };

    // Every SCRAPE_INTERVAL (at most), scrape each endpoint and write it to the database
    let scrape_interval = async_std::task::spawn(async move {
        // Attempt to align scrapes to a 5 second interval
        let spawn_ts = Utc::now();
        if spawn_ts.timestamp() % 5 > 0 {
            let start_ts = spawn_ts + chrono::Duration::seconds(5);
            let start_ts = start_ts
                .with_second((start_ts.second() / 5) * 5)
                .and_then(|ts| ts.with_nanosecond(0))
                .unwrap_or(start_ts);
            if let Ok(sleep) = (start_ts - spawn_ts).to_std() {
                async_std::task::sleep(sleep).await;
            }
        }

        // Begin scraping
        loop {
            let start = Instant::now();

            // Get the current list of pods to iterate over
            let targets = pods.get();

            // Scrape the targets with a given `max_concurrency`
            targets
                .into_par_stream()
                .limit(config.scrape_concurrency as usize)
                .for_each(move |target| {
                    scrape_target(
                        store,
                        config.scrape_timeout,
                        &config.scrape_labels,
                        Arc::clone(&target),
                    )
                })
                .await;

            // Sleep until the next scrape interval
            if let Some(delay) = config.scrape_interval.checked_sub(start.elapsed()) {
                async_std::task::sleep(delay).await;
            }
        }
    });

    // Shutdown when the process is killed
    shutdown.recv().await;
    scrape_interval.cancel().await;
    if let Some(watch_interval) = watch_interval {
        watch_interval.cancel().await;
    }
    if let Some(debug_interval) = debug_interval {
        debug_interval.cancel().await;
    }

    Ok(())
}

async fn scrape_target(
    store: &'static dyn Storage,
    timeout: std::time::Duration,
    global_labels: &'static [(String, String)],
    target: Arc<ScrapeTarget>,
) {
    // Get the current timestamp (w/o nanoseconds); we don't need that level of precision
    let timestamp = Utc::now().naive_utc().timestamp();

    // Don't collect data samples that that occur before `timestamp_min`.
    //
    // They may be duplicates (if they specify their own timestamp),
    // and attempting to store too-old of series may cause errors on
    // some storage backends.
    let max_age = std::cmp::max(
        target.bookmark(timestamp),
        timestamp - (5 * 60), // at most ~5 minute old timestamps
    );
    match target.scrape(timeout).await {
        Ok(input) => {
            DEBUG.scrape_succeeded();
            DEBUG.allocate_response(input.len());

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
            let mut parsed = parser::parse(&input);
            let total = parsed.samples.len();
            parsed.samples = parsed
                .samples
                .into_iter()
                .filter(|d| d.timestamp.map(|t| t >= max_age).unwrap_or(true))
                .collect::<Vec<_>>();
            if parsed.samples.is_empty() {
                DEBUG.drop_response(input.len());
                return;
            }

            let expected = parsed.samples.len();
            let skipped = total - expected;
            let (sent, errors) = store.write(timestamp, parsed, &static_labels).await;
            if sent > 0 {
                DEBUG.writes_succeeded(sent);
            }
            if skipped > 0 {
                DEBUG.writes_skipped(skipped);
            }
            if !errors.is_empty() {
                DEBUG.writes_failed(expected - sent);
                let mut errors = errors;
                let err = if errors.len() > 1 {
                    errors
                        .swap_remove(0)
                        .context("a write failed with an error")
                        .context(format!("write failed with {} errors", errors.len() + 1))
                } else {
                    errors.swap_remove(0).context("write failed")
                };
                debug_error(err);
            }
            DEBUG.drop_response(input.len());
        }
        Err(ScrapeError::Timeout) => {
            DEBUG.scrape_timeout();
        }
        Err(err) if err.to_string().contains("Connection refused") => {
            DEBUG.scrape_refused();
        }
        Err(err) => {
            DEBUG.scrape_failed();
            debug_error(err.into());
        }
    }
}
