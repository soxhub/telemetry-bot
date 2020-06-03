//! Telemetry Bot

mod prometheus_api;

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
    let db = sqlx::postgres::PgPool::builder()
        .max_size(1)
        .build(&db_url)
        .await?;

    // Scrape prometheus metric values at most every 5 minutes
    const INTERVAL: Duration = Duration::from_secs(5 * 60);
    loop {
        let started_at = Instant::now();


        // Don't start another query until at least INTERVAL has passed since we started
        if let Some(sleep_dur) = INTERVAL.checked_sub(started_at.elapsed()) {
            futures::select! {
                _ = async_std::task::sleep(sleep_dur).fuse() => (),
                _ = shutdown => break,
            };
        }
    }

    Ok(())
}
