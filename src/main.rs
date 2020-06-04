//! Telemetry Bot

mod encoding;
mod migrations;
mod prometheus_api;
mod prometheus_timeseries;

use anyhow::{Context, Result}; // alias std::result::Result with dynamic error type
use futures::channel::oneshot;
use futures::future::FutureExt;
use std::cell::Cell;
use std::time::{Duration, Instant};

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
    let mut shutdown = shutdown.fuse();
    let prom_url = dotenv::var("PROMETHEUS_URL").context("missing PROMETHEUS_URL")?;
    let db_url = dotenv::var("DATABASE_URL").context("missing DATABASE_URL")?;

    // Run any outstanding database migrations
    {
        use refinery::config::{Config, ConfigDbType};

        // Parse the DATABASE_URL param and pass it to refinery's connection builder api
        let url = url::Url::parse(&db_url).context("invalid DATABASE_URL")?;
        let mut db = Config::new(ConfigDbType::Postgres);
        if !url.username().is_empty() {
            db = db.set_db_user(url.username());
        }
        if let Some(password) = url.password() {
            db = db.set_db_user(password);
        }
        if let Some(host) = url.host_str() {
            db = db.set_db_host(host);
        }
        if let Some(port) = url.port() {
            db = db.set_db_port(&port.to_string());
        }
        let db_name = url.path().trim_start_matches('/');
        if !db_name.is_empty() {
            db = db.set_db_name(db_name);
        }

        // Run migrations
        println!("Checking database migrations...");
        let report = migrations::runner().run(&mut db)?;
        let migrations = report.applied_migrations();
        if migrations.is_empty() {
            println!("No unapplied database migrations.");
        } else {
            println!("Applied migrations:");
        }
        for migration in migrations {
            println!(" - Migration: {}", migration);
        }
    }

    // Open a sqlx connection pool
    let db = sqlx::postgres::PgPool::builder()
        .max_size(1)
        .build(&db_url)
        .await?;
    println!("Connected to {}", db_url);

    // Open a exporter
    let mut exporter = prometheus_timeseries::PrometheusExporter::new(db, prom_url);
    exporter.restore_schema().await?;

    // Scrape prometheus metric values at most every 5 minutes
    const INTERVAL: Duration = Duration::from_secs(5 * 60);
    'shutdown: loop {
        let started_at = Instant::now();

        // Run the exporter once
        futures::select! {
            ok = exporter.run().fuse() => ok?,
            _ = shutdown => break 'shutdown,
        };

        // Don't start another query until at least INTERVAL seconds have passed since we started
        if let Some(sleep_dur) = INTERVAL.checked_sub(started_at.elapsed()) {
            futures::select! {
                _ = async_std::task::sleep(sleep_dur).fuse() => (),
                _ = shutdown => break 'shutdown,
            };
        }
    }

    Ok(())
}
