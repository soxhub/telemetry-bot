pub use std::sync::atomic::{AtomicUsize, Ordering};

pub static DEBUG: DebugMetrics = DebugMetrics::new();

pub struct DebugMetrics {
    pod_count: AtomicUsize,
    series_count: AtomicUsize,
    series_bytes: AtomicUsize,
    polling_errors: AtomicUsize,
    polling_resets: AtomicUsize,
    scrape_count: AtomicUsize,
    scrape_errors: AtomicUsize,
    scrape_timeouts: AtomicUsize,
    scrape_disconnects: AtomicUsize,
    write_count: AtomicUsize,
    write_errors: AtomicUsize,
    write_skips: AtomicUsize,
    write_task_count: AtomicUsize,
    write_task_peak: AtomicUsize,
    query_count: AtomicUsize,
    targets_active: AtomicUsize,
    targets_peak: AtomicUsize,
    response_bytes_used: AtomicUsize,
    response_bytes_peak: AtomicUsize,
}

impl DebugMetrics {
    pub const fn new() -> Self {
        DebugMetrics {
            pod_count: AtomicUsize::new(0),
            series_count: AtomicUsize::new(0),
            series_bytes: AtomicUsize::new(0),
            polling_errors: AtomicUsize::new(0),
            polling_resets: AtomicUsize::new(0),
            scrape_count: AtomicUsize::new(0),
            scrape_errors: AtomicUsize::new(0),
            scrape_timeouts: AtomicUsize::new(0),
            scrape_disconnects: AtomicUsize::new(0),
            write_count: AtomicUsize::new(0),
            write_errors: AtomicUsize::new(0),
            write_skips: AtomicUsize::new(0),
            write_task_count: AtomicUsize::new(0),
            write_task_peak: AtomicUsize::new(0),
            query_count: AtomicUsize::new(0),
            targets_active: AtomicUsize::new(0),
            targets_peak: AtomicUsize::new(0),
            response_bytes_used: AtomicUsize::new(0),
            response_bytes_peak: AtomicUsize::new(0),
        }
    }

    pub fn update_pods(&self, n: usize) {
        self.pod_count.store(n, Ordering::Relaxed);
    }

    pub fn series_added(&self, key_bytes: usize) {
        self.series_count.fetch_add(1, Ordering::Relaxed);
        self.series_bytes.fetch_add(key_bytes, Ordering::Relaxed);
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

    pub fn finished_query(&self) {
        self.query_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn allocate_target(&self) {
        let active = self.targets_active.fetch_add(1, Ordering::SeqCst);
        // TODO: Use `fetch_max` when it is stable
        if active > self.targets_peak.load(Ordering::SeqCst) {
            self.targets_peak.store(active, Ordering::SeqCst);
        }
    }

    pub fn drop_target(&self) {
        self.targets_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn allocate_response(&self, bytes: usize) {
        let used = self.response_bytes_used.fetch_add(bytes, Ordering::SeqCst);
        // TODO: Use `fetch_max` when it is stable
        if used > self.response_bytes_peak.load(Ordering::SeqCst) {
            self.response_bytes_peak.store(used, Ordering::SeqCst);
        }
    }

    pub fn drop_response(&self, bytes: usize) {
        self.response_bytes_used.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn write_task_started(&self) {
        let concurrent = self.write_task_count.fetch_add(1, Ordering::SeqCst);
        // TODO: Use `fetch_max` when it is stable
        if concurrent > self.write_task_peak.load(Ordering::SeqCst) {
            self.write_task_peak.store(concurrent, Ordering::SeqCst);
        }
    }

    pub fn write_task_finished(&self) {
        self.write_task_count.fetch_sub(1, Ordering::Relaxed);
    }

    // Log the current metrics and reset the counters
    pub fn publish(&self) {
        let pod_count = self.pod_count.load(Ordering::Relaxed);
        let targets_active = self.targets_active.load(Ordering::Relaxed);
        let targets_peak = self.targets_peak.swap(targets_active, Ordering::Relaxed);
        let series_count = self.series_count.load(Ordering::Relaxed);
        let series_bytes = self.series_bytes.load(Ordering::Relaxed);
        let resp_bytes_used = self.response_bytes_used.load(Ordering::Relaxed);
        let resp_bytes_peak = self
            .response_bytes_peak
            .swap(resp_bytes_used, Ordering::Relaxed);

        fn format_bytes(bytes: usize) -> String {
            const KI_B: f32 = 1024.;
            const MI_B: f32 = 1024. * 1024.;

            let bytes = bytes as f32;
            if bytes > MI_B {
                format!("{:.1} {}", bytes / MI_B, "MiB")
            } else {
                format!("{:.1} {}", bytes / KI_B, "KiB")
            }
        }
        let series_bytes = format_bytes(series_bytes);
        let resp_bytes_used = format_bytes(resp_bytes_used);
        let resp_bytes_peak = format_bytes(resp_bytes_peak);

        let polling_errors = self.polling_errors.swap(0, Ordering::Relaxed);
        let polling_resets = self.polling_resets.swap(0, Ordering::Relaxed);
        let scrape_count = self.scrape_count.swap(0, Ordering::Relaxed);
        let scrape_errors = self.scrape_errors.swap(0, Ordering::Relaxed);
        let scrape_timeouts = self.scrape_timeouts.swap(0, Ordering::Relaxed);
        let scrape_disconnects = self.scrape_disconnects.swap(0, Ordering::Relaxed);
        let write_count = self.write_count.swap(0, Ordering::Relaxed);
        let write_errors = self.write_errors.swap(0, Ordering::Relaxed);
        let write_skips = self.write_skips.swap(0, Ordering::Relaxed);
        let write_task_curr = self.write_task_count.load(Ordering::Relaxed);
        let write_task_peak = self
            .write_task_peak
            .swap(write_task_curr, Ordering::Relaxed);
        let sql_queries = self.query_count.swap(0, Ordering::Relaxed);
        println!(
            "Debug: pods {} (used {}, peak {}) | polling (errors {}, resets {}) | scraped {} (errors {}, timeouts {}, disconnects {}) | writes {} (errors {}, skipped {}) | series {} (rss {}) | response bytes (used {}, peak {}) | tasks (curr {}, peak {}) | sql (queries {})",
            pod_count,
            targets_active,
            targets_peak,
            polling_errors,
            polling_resets,
            scrape_count,
            scrape_errors,
            scrape_timeouts,
            scrape_disconnects,
            write_count,
            write_errors,
            write_skips,
            series_count,
            series_bytes,
            resp_bytes_used,
            resp_bytes_peak,
            write_task_curr,
            write_task_peak,
            sql_queries,
        );
    }
}
