//! Telemetry Bot

use anyhow::Result; // alias std::result::Result with dynamic error type
use std::sync::atomic::{AtomicBool, Ordering};

/// A flag to shutdown the bot
pub static SHOULD_EXIT: AtomicBool = AtomicBool::new(false);

/// Asks the program to exit.
pub fn shutdown() {
    SHOULD_EXIT.store(true, Ordering::SeqCst)
}

/// The program's main entry point.
fn main() -> Result<()> {
    // When we receive a SIGINT (or SIGTERM) signal, begin exiting.
    ctrlc::set_handler(shutdown).expect("Error setting signal handlers.");

    // Start the main event loop
    async_std::task::block_on(run())
}

/// The main thread's event loop
async fn run() -> Result<()> {
    println!("Beep boop");
    Ok(())
}
