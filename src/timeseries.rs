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
/// More values means faster ingestion times, but will use more memory.
const LABEL_VALUE_CACHE_SIZE: usize = 2048;

/// How many label sets to hold in memory, per metric
///
/// More values means faster ingestion times, but will use more resident memory.
const PER_METRIC_LABELS_CACHE_SIZE: usize = 512;

pub struct PrometheusExporter {
    db: sqlx::postgres::PgPool,
    prom_url: String,
    schema_updated_at: Option<Instant>,
    metrics: Vec<MetricSchema>,
    /// Map of metric ids to index in `metrics`
    metric_by_id: HashMap<i32, usize>,
    /// Map of metric names to index in `metrics`
    metric_by_name: HashMap<String, usize>,
    /// Map of label names to ids
    label_by_id: HashMap<i32, String>,
    /// Map of label ids to names
    label_by_name: HashMap<String, i32>,
    label_value_cache: LruCache<String, i32>,
}

impl PrometheusExporter {
    pub fn new(db: sqlx::postgres::PgPool, prom_url: String) -> Self {
        PrometheusExporter {
            db,
            prom_url,
            schema_updated_at: None,
            metrics: Vec::new(),
            metric_by_id: HashMap::new(),
            metric_by_name: HashMap::new(),
            label_by_id: HashMap::new(),
            label_by_name: HashMap::new(),
            label_value_cache: LruCache::new(LABEL_VALUE_CACHE_SIZE),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let should_refresh = self
            .schema_updated_at
            .map(|ts| ts.elapsed() > SCHEMA_REFRESH_INTERVAL)
            .unwrap_or(true);
        if should_refresh {
            self.refresh_schema().await?;
        }
        Ok(())
    }

    /// Restore schema mapping from database
    pub async fn restore_schema(&mut self) -> Result<()> {
        println!("Loading schema from telemetry database...");

        // Load known labels from the database
        let labels: Vec<(i32, String)> =
            sqlx::query_as("SELECT id, label FROM prometheus_label_keys")
                .fetch_all(&self.db)
                .await?;
        println!("Loaded {} labels.", labels.len());
        for (id, label) in labels {
            self.label_by_id.entry(id).or_insert(label.clone());
            self.label_by_name.entry(label).or_insert(id);
        }

        // Load known metrics from the database
        let metrics: Vec<(i32, String, String)> =
            sqlx::query_as("SELECT id, name, type FROM prometheus_metrics")
                .fetch_all(&self.db)
                .await?;
        println!("Loaded {} metrics.", metrics.len());
        for (id, name, type_) in metrics {
            let index = self.metrics.len();
            self.metrics
                .push(MetricSchema::new(id, name.clone(), type_));
            self.metric_by_id.entry(id).or_insert(index);
            self.metric_by_name.entry(name).or_insert(index);
        }

        // Load known metric labels from the database
        let metric_labels: Vec<(i32, i32)> =
            sqlx::query_as("SELECT metric_id, label_id FROM prometheus_metric_labels")
                .fetch_all(&self.db)
                .await?;
        for (metric_id, label_id) in metric_labels {
            let metric = &mut self.metrics[self.metric_by_id[&metric_id]];
            let label = &self.label_by_id[&label_id];
            if let Some(columns) = &mut metric.labels_columns {
                columns.push((label_id, label.clone()));
            } else {
                metric.labels_columns = Some(vec![(label_id, label.clone())]);
            }
        }

        println!("Loaded schema from telemetry database.");
        Ok(())
    }

    /// Update schema mappings from prometheus state
    pub async fn refresh_schema(&mut self) -> Result<()> {
        println!("Refreshing schema from prometheus...");

        // Load all labels
        let mut insert_labels = Vec::new();
        let labels = prometheus_api::labels(&self.prom_url).await?;
        let labels = labels.as_array().expect("labels is not an array");
        for label in labels {
            let label = label.as_str().expect("label is not a string");
            if !self.label_by_name.contains_key(label) {
                insert_labels.push(label);
            }
        }
        let labels: Vec<(i32, String)> = sqlx::query_as(
            r#"
                INSERT INTO prometheus_label_keys (label)
                SELECT * FROM UNNEST($1)
                ON CONFLICT DO NOTHING
                RETURNING id, label
            "#,
        )
        .bind(insert_labels)
        .fetch_all(&self.db)
        .await?;

        // Update in-memory labels
        if labels.is_empty() {
            println!("No new labels.");
        } else {
            println!("Found {} new labels:", labels.len());
        }
        for (id, label) in labels {
            println!(" - Label: {}", label);
            self.label_by_id.entry(id).or_insert(label.clone());
            self.label_by_name.entry(label).or_insert(id);
        }

        // Load all metrics
        let mut insert_metric_names = Vec::new();
        let mut insert_metric_types = Vec::new();
        let metrics = prometheus_api::metadata(&self.prom_url).await?;
        let metrics = metrics.as_object().expect("metadata is not an object");
        for (name, info) in metrics {
            if !self.metric_by_name.contains_key(name) {
                let type_ = info[0]["type"]
                    .as_str()
                    .expect("metric type is not a string");
                insert_metric_names.push(name.as_str());
                insert_metric_types.push(type_);
            }
        }
        let metrics: Vec<(i32, String, String)> = sqlx::query_as(
            r#"
                INSERT INTO prometheus_metrics (name, type)
                SELECT * FROM UNNEST($1, $2)
                ON CONFLICT DO NOTHING
                RETURNING id, name, type
            "#,
        )
        .bind(insert_metric_names)
        .bind(insert_metric_types)
        .fetch_all(&self.db)
        .await?;

        // Update in-memory metrics
        if metrics.is_empty() {
            println!("No new metrics.");
        } else {
            println!("Found {} new metrics:", metrics.len());
        }
        for (id, name, type_) in metrics {
            println!(" - Metric: {}", name);
            let index = self.metrics.len();
            let schema = MetricSchema::new(id, name.clone(), type_);
            schema.create_if_not_exists(&self.db).await?;
            self.metrics.push(schema);
            self.metric_by_id.entry(id).or_insert(index);
            self.metric_by_name.entry(name).or_insert(index);
        }

        println!("Refreshed schema from prometheus.");
        Ok(())
    }
}

struct MetricSchema {
    id: i32,
    name: String,
    type_: String,
    values_table: String,
    labels_table: String,

    // is 'None' if unknown, otherwise array of (label_id, column_name)
    labels_columns: Option<Vec<(i32, String)>>,

    /// Cache of unique label sets to labels_table ids
    labels_cache: LruCache<String, i32>,
}

impl MetricSchema {
    fn new(id: i32, name: String, type_: String) -> Self {
        let name_prefix = name
            .chars()
            .filter_map(|ch| match ch {
                '-' | '_' | '.' | ':' => Some('_'),
                _ if ch.is_ascii_alphanumeric() => Some(ch),
                _ => None,
            })
            .take(16)
            .collect::<String>();
        let name_prefix = name_prefix.trim_end_matches('_');
        let values_table = format!("values_{:04}_{}_{}", id, type_, name_prefix);
        let labels_table = format!("labels_{:04}_{}_{}", id, type_, name_prefix);
        MetricSchema {
            id,
            name,
            type_,
            values_table,
            labels_table,
            labels_columns: None,
            labels_cache: LruCache::new(PER_METRIC_LABELS_CACHE_SIZE),
        }
    }

    async fn create_if_not_exists(&self, db: &sqlx::postgres::PgPool) -> Result<()> {
        // Create a table for series labels
        sqlx::query(&format!(
            r#"
                CREATE TABLE IF NOT EXISTS prometheus_series.{} (
                    id serial PRIMARY KEY,
                    additional_labels JSONB
                );
            "#,
            self.labels_table
        ))
        .execute(db)
        .await?;

        // Create a table for series timestamps+values
        sqlx::query(&format!(
            r#"
                CREATE TABLE IF NOT EXISTS prometheus_series.{} (
                    id bigserial PRIMARY KEY,
                    labels_id int NOT NULL,
                    min_timestamp timestamp NOT NULL,
                    max_timestamp timestamp NOT NULL,
                    timestamps bytea NOT NULL,
                    values bytea NOT NULL
                ) WITH (toast_tuple_target = 128);
            "#,
            self.values_table
        ))
        .execute(db)
        .await?;

        // Alter the table so timestamps are stored externally
        sqlx::query(&format!(
            r#"
                ALTER TABLE prometheus_series.{}
                    ALTER COLUMN timestamps SET STORAGE EXTERNAL,
                    ALTER COLUMN values SET STORAGE EXTERNAL
                ;
            "#,
            self.values_table
        ))
        .execute(db)
        .await?;

        // Create indices for the table
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS {}_timestamps_idx ON prometheus_series.{} (max_timestamp DESC, min_timestamp DESC);",
            self.values_table,
            self.values_table,
        ))
        .execute(db)
        .await?;
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS {}_labels_idx ON prometheus_series.{} (labels_id, max_timestamp DESC, min_timestamp DESC);",
            self.values_table,
            self.values_table,
        ))
        .execute(db)
        .await?;

        Ok(())
    }
}
