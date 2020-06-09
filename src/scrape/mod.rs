mod detect;
mod metrics;

use anyhow::Result;
use arc_swap::ArcSwap;
use std::sync::Arc;

pub use metrics::{parse, Measurement, Number, SeriesType};

pub struct ScrapeTarget {
    pub name: String,
    pub namespace: String,

    // e.g. http://10.0.15.15:9025/metrics
    pub url: String,
}

impl ScrapeTarget {
    pub async fn scrape(&self) -> Result<String> {
        surf::get(&self.url)
            .recv_string()
            .await
            .map_err(|err| anyhow::anyhow!("failed to scrape metrics: {}", err))
    }
}

pub struct ScrapeList {
    list: ArcSwap<Vec<ScrapeTarget>>,
}

impl ScrapeList {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self {
            list: ArcSwap::new(Arc::new(Vec::new())),
        })
    }

    pub fn get(&self) -> Arc<Vec<ScrapeTarget>> {
        self.list.load_full()
    }

    pub fn put(&self, list: Vec<ScrapeTarget>) {
        self.list.store(Arc::new(list));
    }

    pub async fn update(&self) -> Result<()> {
        let targets = detect::find_scrape_targets().await?;
        self.put(targets);
        Ok(())
    }
}
