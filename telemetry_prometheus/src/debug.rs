pub use std::sync::atomic::{AtomicUsize, Ordering};

pub static DEBUG: DebugMetrics = DebugMetrics::new();

pub struct DebugMetrics {
    pod_count: AtomicUsize,
    polling_errors: AtomicUsize,
    scrape_count: AtomicUsize,
    scrape_errors: AtomicUsize,
    write_count: AtomicUsize,
    write_errors: AtomicUsize,
}

impl DebugMetrics {
    pub const fn new() -> Self {
        DebugMetrics {
            pod_count: AtomicUsize::new(0),
            polling_errors: AtomicUsize::new(0),
            scrape_count: AtomicUsize::new(0),
            scrape_errors: AtomicUsize::new(0),
            write_count: AtomicUsize::new(0),
            write_errors: AtomicUsize::new(0),
        }
    }

    pub fn update_pods(&self, n: usize) {
        self.pod_count.store(n, Ordering::Relaxed);
    }

    pub fn polling_failed(&self) {
        self.polling_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn scrape_succeeded(&self) {
        self.scrape_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn scrape_failed(&self) {
        self.scrape_count.fetch_add(1, Ordering::Relaxed);
        self.scrape_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn write_succeeded(&self) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn write_failed(&self) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    // Log the current metrics and reset the counters
    pub fn publish(&self) {
        let pod_count = self.pod_count.swap(0, Ordering::Relaxed);
        let polling_errors = self.polling_errors.swap(0, Ordering::Relaxed);
        let scrape_count = self.scrape_count.swap(0, Ordering::Relaxed);
        let scrape_errors = self.scrape_errors.swap(0, Ordering::Relaxed);
        let write_count = self.write_count.swap(0, Ordering::Relaxed);
        let write_errors = self.write_errors.swap(0, Ordering::Relaxed);
        println!(
            "Debug: pods {} (errors {}) | scraped {} (errors {}) | writes {} (errors {})",
            pod_count, polling_errors, scrape_count, scrape_errors, write_count, write_errors
        );
    }
}
