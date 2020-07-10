use std::sync::atomic::{AtomicBool, Ordering};

/// Whether to log (verbose) error output.
/// Use the `ERROR_LOGGER` env var to override (on, off)
static ERROR_LOGGER: AtomicBool = AtomicBool::new(false);

pub fn debug_error_enabled(enabled: bool) {
    ERROR_LOGGER.store(enabled, Ordering::Relaxed);
}

pub fn debug_error(err: anyhow::Error) {
    if ERROR_LOGGER.load(Ordering::Relaxed) {
        eprintln!("Warn: {}", err);
        for err in err.chain().skip(1) {
            eprintln!("Caused by: {}", err);
        }
    }
}
