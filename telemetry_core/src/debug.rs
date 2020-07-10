pub use std::sync::atomic::{AtomicUsize, Ordering};

pub static DEBUG: DebugMetrics = DebugMetrics::new();

pub struct DebugMetrics {
    pod_count: AtomicUsize,
    polling_errors: AtomicUsize,
    polling_resets: AtomicUsize,
    scrape_count: AtomicUsize,
    scrape_errors: AtomicUsize,
    scrape_timeouts: AtomicUsize,
    scrape_disconnects: AtomicUsize,
    write_count: AtomicUsize,
    write_errors: AtomicUsize,
    write_skips: AtomicUsize,
}

impl DebugMetrics {
    pub const fn new() -> Self {
        DebugMetrics {
            pod_count: AtomicUsize::new(0),
            polling_errors: AtomicUsize::new(0),
            polling_resets: AtomicUsize::new(0),
            scrape_count: AtomicUsize::new(0),
            scrape_errors: AtomicUsize::new(0),
            scrape_timeouts: AtomicUsize::new(0),
            scrape_disconnects: AtomicUsize::new(0),
            write_count: AtomicUsize::new(0),
            write_errors: AtomicUsize::new(0),
            write_skips: AtomicUsize::new(0),
        }
    }

    pub fn update_pods(&self, n: usize) {
        self.pod_count.store(n, Ordering::Relaxed);
    }

    pub fn polling_failed(&self) {
        self.polling_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn polling_reset(&self) {
        self.polling_resets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn scrape_succeeded(&self) {
        self.scrape_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn scrape_failed(&self) {
        self.scrape_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn scrape_timeout(&self) {
        self.scrape_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn scrape_refused(&self) {
        self.scrape_disconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub fn writes_succeeded(&self, n: usize) {
        self.write_count.fetch_add(n, Ordering::Relaxed);
    }

    pub fn write_succeeded(&self) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn writes_failed(&self, n: usize) {
        self.write_errors.fetch_add(n, Ordering::Relaxed);
    }

    pub fn write_failed(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn writes_skipped(&self, n: usize) {
        self.write_skips.fetch_add(n, Ordering::Relaxed);
    }

    pub fn write_skipped(&self) {
        self.write_skips.fetch_add(1, Ordering::Relaxed);
    }

    // Log the current metrics and reset the counters
    pub fn publish(&self) {
        let pod_count = self.pod_count.load(Ordering::Relaxed);
        let polling_errors = self.polling_errors.swap(0, Ordering::Relaxed);
        let polling_resets = self.polling_resets.swap(0, Ordering::Relaxed);
        let scrape_count = self.scrape_count.swap(0, Ordering::Relaxed);
        let scrape_errors = self.scrape_errors.swap(0, Ordering::Relaxed);
        let scrape_timeouts = self.scrape_timeouts.swap(0, Ordering::Relaxed);
        let scrape_disconnects = self.scrape_disconnects.swap(0, Ordering::Relaxed);
        let write_count = self.write_count.swap(0, Ordering::Relaxed);
        let write_errors = self.write_errors.swap(0, Ordering::Relaxed);
        let write_skips = self.write_skips.swap(0, Ordering::Relaxed);
        println!(
            "Debug: pods {} (errors {}, resets {}) | scraped {} (errors {}, timeouts {}, disconnects {}) | writes {} (errors {}, skipped {})",
            pod_count,
            polling_errors,
            polling_resets,
            scrape_count,
            scrape_errors,
            scrape_timeouts,
            scrape_disconnects,
            write_count,
            write_errors,
            write_skips
        );
    }
}
