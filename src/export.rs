use anyhow::Result;
use lru::LruCache;
use sqlx::prelude::*;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::prometheus_api;

/// How frequently to reload the metrics from prometheus
const SCHEMA_REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// How many unique label value mappings to hold in memory
///
/// More values means faster insertion sizes, but will use more memory.
const LABEL_VALUE_CACHE_SIZE: usize = 1000;

pub struct Exporter {
    db: sqlx::postgres::PgPool,
    prom_url: String,
    schema_updated_at: Option<Instant>,
    metric_map: HashMap<String, MetricTable>,
    label_map: HashMap<String, i32>,
    label_value_map: LruCache<String, i32>,
}

impl Exporter {
    pub fn new(db: sqlx::postgres::PgPool, prom_url: String) -> Self {
        Exporter {
            db,
            prom_url,
            schema_updated_at: None,
            metric_map: HashMap::new(),
            label_map: HashMap::new(),
            label_value_map: LruCache::new(LABEL_VALUE_CACHE_SIZE),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let should_refresh = self
            .schema_updated_at
            .map(|ts| ts.elapsed() > SCHEMA_REFRESH_INTERVAL)
            .unwrap_or(false);
        if should_refresh {
            self.refresh_schema().await?;
        }
        Ok(())
    }

    /// Restore schema mapping from database
    pub async fn restore_schema(&mut self) -> Result<()> {
        Ok(())
    }

    /// Update schema mappings from prometheus state
    pub async fn refresh_schema(&mut self) -> Result<()> {
        // Load all labels
        let mut insert_labels = Vec::new();
        let labels = prometheus_api::labels(&self.prom_url).await?;
        let labels = labels.as_array().expect("labels is not an array");
        for label in labels {
            let label = label.as_str().expect("label is not a string");
            if !self.label_map.contains_key(label) {
                insert_labels.push(label);
            }
        }

        // Load all metrics
        let mut insert_metric_names = Vec::new();
        let mut insert_metric_types = Vec::new();
        let metrics = prometheus_api::metadata(&self.prom_url).await?;
        let metrics = metrics.as_object().expect("metadata is not an object");
        for (name, info) in metrics {
            if !self.metric_map.contains_key(name) {
                let type_ = info["type"].as_str().expect("metric type is not a string");
                insert_metric_names.push(name.as_str());
                insert_metric_types.push(type_);
            }
        }
        let metrics: Vec<(i32, String, String)> = sqlx::query_as(
            r#"
                INSERT INTO prometheus_metrics (name, type)
                SELECT id, name
                FROM UNNEST($1, $2)
                ON CONFLICT name DO NOTHING
                RETURNING id, name, type
            "#,
        )
        .bind(insert_metric_names)
        .bind(insert_metric_types)
        .fetch_all(&self.db)
        .await?;
        for (id, name, type_) in metrics {
            let table = MetricTable::new(id, name.clone(), type_);
            self.metric_map.entry(name).or_insert(table);
        }
        Ok(())
    }
}

struct MetricTable {
    id: i32,
    name: String,
    type_: String,
    values_table: String,
    labels_table: String,

    // Is 'None' if unknown, otherwise array of (column_name, label_id)
    label_columns: Option<Vec<(String, i32)>>,
}

impl MetricTable {
    fn new(id: i32, name: String, type_: String) -> Self {
        let values_table = format!("prometheus_series.values__{}__{}", type_, name);
        let labels_table = format!("prometheus_series.labels__{}__{}", type_, name);
        MetricTable {
            id,
            name,
            type_,
            values_table,
            labels_table,
            label_columns: None,
        }
    }
}
