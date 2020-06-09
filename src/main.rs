//! Telemetry Bot

mod scrape;
mod series;

use anyhow::{Context, Result}; // alias std::result::Result with dynamic error type
use futures::channel::oneshot;
use futures::stream::StreamExt;
use parallel_stream::prelude::*;
use std::cell::Cell;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::sync::RwLock;

/// How frequently to update our pod list
const WATCH_INTERVAL: Duration = Duration::from_secs(60);

/// How frequently to collect metric timeseries data
const SCRAPE_INTERVAL: Duration = Duration::from_secs(15);

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
    let max_concurrency: usize = match dotenv::var("SCRAPE_CONCURRENCY").ok() {
        Some(val) => val.parse().context("invalid SCRAPE_CONCURRENCY")?,
        None => 128,
    };
    let db_pool_size = match dotenv::var("DATABASE_POOL_SIZE").ok() {
        Some(val) => val.parse().context("invalid DATABASE_POOL_SIZE")?,
        None => 8,
    };

    // Open a sqlx connection pool
    let db_url = dotenv::var("DATABASE_URL").context("missing DATABASE_URL")?;
    let _db = sqlx::postgres::PgPool::builder()
        .max_size(db_pool_size)
        .build(&db_url)
        .await?;
    println!("Connected to {}", db_url);

    // Collect the list of pods to be scraped for metrics
    let pods = scrape::ScrapeList::shared();
    pods.update().await?;

    // Every WATCH_INTERVAL, update our list of pods
    let watch_interval = {
        let pods = pods.clone();
        async_std::task::spawn(async move {
            let mut interval = async_std::stream::interval(WATCH_INTERVAL);
            while let Some(_) = interval.next().await {
                if let Err(_) = pods.update().await {
                    // TODO: Log or something
                }
            }
        })
    };

    // Every SCRAPE_INTERVAL (at most), scrape each endpoint and write it to the database
    let scrape_interval = async_std::task::spawn(async move {
        let metrics = RwLock::new(HashMap::new());

        loop {
            let start = Instant::now();

            // Get the current list of pods to iterate over
            let targets = pods.get();

            // Scrape the targets with a given `max_concurrency`
            targets
                .into_par_stream()
                .limit(max_concurrency)
                .map(|target| async {
                    if let Ok(input) = target.scrape().await {
                        let (metrics, values) = scrape::parse(&input);
                        for value in values {}
                        series::write(db, values);
                        // let conn = db.
                    }
                })
                .collect()
                .await;

            // Sleep until the next scrape interval
            if let Some(delay) = SCRAPE_INTERVAL.checked_sub(start.elapsed()) {
                async_std::task::sleep(delay).await;
            }
        }
    });

    // Shutdown when the process is killed
    shutdown.await?;
    watch_interval.cancel().await;
    scrape_interval.cancel().await;

    Ok(())
}
