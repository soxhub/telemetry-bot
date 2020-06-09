//! Telemetry Bot

mod scrape;

use anyhow::{Context, Result}; // alias std::result::Result with dynamic error type
use futures::channel::oneshot;
use std::cell::Cell;

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
    // Open a sqlx connection pool
    let db_url = dotenv::var("DATABASE_URL").context("missing DATABASE_URL")?;
    let _db = sqlx::postgres::PgPool::builder()
        .max_size(1)
        .build(&db_url)
        .await?;
    println!("Connected to {}", db_url);

    // let mut shutdown = shutdown.fuse();
    shutdown.await?;

    Ok(())
}
