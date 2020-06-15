pub use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub struct DebugMetrics {
    total_endpoints: AtomicUsize,
    total_scrapes: AtomicUsize,
    total_inserts: AtomicUsize,
    failed_polling: AtomicUsize,
    failed_scrapes: AtomicUsize,
    failed_inserts: AtomicUsize,
}

impl DebugMetrics {
    pub fn update_endpoints(&self, n: usize) {
        self.total_endpoints.store(n, Ordering::Relaxed);
    }

    pub fn polling_failed(&self) {
        self.failed_polling.fetch_add(1, Ordering::Relaxed);
    }

    pub fn scrape_succeeded(&self) {
        self.total_scrapes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn scrape_failed(&self) {
        self.total_scrapes.fetch_add(1, Ordering::Relaxed);
        self.failed_scrapes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn insert_succeeded(&self) {
        self.total_inserts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn insert_failed(&self) {
        self.total_inserts.fetch_add(1, Ordering::Relaxed);
        self.failed_inserts.fetch_add(1, Ordering::Relaxed);
    }

    // Log the current metrics and reset the counters
    pub fn publish(&self) {
        let total_targets = self.total_endpoints.swap(0, Ordering::Relaxed);
        let total_scrapes = self.total_scrapes.swap(0, Ordering::Relaxed);
        let total_inserts = self.total_inserts.swap(0, Ordering::Relaxed);
        let failed_scrapes = self.failed_scrapes.swap(0, Ordering::Relaxed);
        let failed_inserts = self.failed_inserts.swap(0, Ordering::Relaxed);
        let failed_polling = self.failed_polling.swap(0, Ordering::Relaxed);
        println!(
            "Debug: pods {} (errors {}) | scraped {} (errors {}) | inserted {} (errors {})",
            total_targets,
            failed_polling,
            total_scrapes,
            failed_scrapes,
            total_inserts,
            failed_inserts
        );
    }
}
