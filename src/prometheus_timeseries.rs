//! This module implements a compact-ish database representation for prometheus timeseries.

/*
 * TODO: We should replace this by setting up TimescaleDB and having prometheus write to it directly
 */

use anyhow::Result;
use chrono::prelude::*;
use sqlx::prelude::*;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::prometheus_api::{self, RequestError};

/// How frequently to reload the metrics from prometheus
const SCHEMA_REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);

pub struct PrometheusExporter {
    db: sqlx::postgres::PgPool,
    prom_url: String,
    schema_updated_at: Option<Instant>,
    metrics: Vec<MetricSchema>,
    /// Map of metric names to index in `metrics`
    metric_by_name: HashMap<String, usize>,
}

impl PrometheusExporter {
    pub fn new(db: sqlx::postgres::PgPool, prom_url: String) -> Self {
        PrometheusExporter {
            db,
            prom_url,
            schema_updated_at: None,
            metrics: Vec::new(),
            metric_by_name: HashMap::new(),
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

        // For each metric, load its recent data
        for metric in &mut self.metrics {
            // Scrape up to 30 minutes of data.
            //
            // We only scrape >5 minutes of data if we are "playing catch-up";
            // that is, if we already have history for the metric.
            //
            // We scrape the metrics trailing real time by ~5 minutes to mitigate
            // possible clock-skew on servers reporting data.
            let now = chrono::Utc::now().naive_utc();
            let end = Some(now)
                .and_then(|t| t.with_nanosecond(0))
                .and_then(|t| t.with_second(0))
                .and_then(|t| t.with_minute(now.minute() % 5))
                .expect("time could not be truncated")
                - chrono::Duration::minutes(5);
            let start = match metric.last_scrape {
                Some(last) => std::cmp::max(last + chrono::Duration::seconds(1), end - chrono::Duration::minutes(30)),
                None => end - chrono::Duration::minutes(5),
            };

            // Don't bother scraping if we'd only received <1 minute of new data
            if end <= start + chrono::Duration::minutes(1) {
                continue;
            }

            // Query prometheus for data in the requested time range
            println!("Scraping {}", metric.name);
            let try_query =
                prometheus_api::query_range(&self.prom_url, &metric.name, start, end, "1m").await;
            match try_query {
                // If we got our results, import them directly
                Ok(data) => {
                    // After the query completes successfully, update our last scrape
                    metric.last_scrape = Some(end);
                    metric.import_range_query(&self.db, data).await?;
                }

                // If the result set would be too big, then try to chunk it up
                Err(RequestError::Failed {
                    error_type, error, ..
                }) if error_type == "execution" && error.contains("too many samples") => {
                    let interval = std::cmp::min((end - start) / 10, chrono::Duration::minutes(1));
                    let mut next = start;
                    while next < end {
                        let chunk_start = next;
                        let chunk_end = next + interval - chrono::Duration::seconds(1);;
                        let data = prometheus_api::query_range(
                            &self.prom_url,
                            &metric.name,
                            chunk_start,
                            chunk_end,
                            "1m",
                        )
                        .await?;

                        // Try to make progress, by saving our `last_scrape` when successful
                        metric.last_scrape = Some(chunk_end);
                        metric.import_range_query(&self.db, data).await?;

                        // Advance the interval
                        next = next + interval;
                    }
                }

                // Otherwise, crash.
                Err(err) => anyhow::bail!(err),
            }

            // Compress series values and labels
        }

        Ok(())
    }

    /// Restore schema mapping from database
    pub async fn restore_schema(&mut self) -> Result<()> {
        println!("Loading schema from telemetry database...");

        // Load known metrics from the database
        let metrics: Vec<(i32, String, String, Option<NaiveDateTime>)> =
            sqlx::query_as("SELECT id, name, type, last_scrape FROM prometheus_metrics")
                .fetch_all(&self.db)
                .await?;
        println!("Loaded {} metrics.", metrics.len());
        for (id, name, type_, last_scrape) in metrics {
            let index = self.metrics.len();
            let mut schema = MetricSchema::new(id, name.clone(), type_);
            schema.last_scrape = last_scrape;
            self.metrics.push(schema);
            self.metric_by_name.entry(name).or_insert(index);
        }

        println!("Loaded schema from telemetry database.");
        Ok(())
    }

    /// Update schema mappings from prometheus state
    pub async fn refresh_schema(&mut self) -> Result<()> {
        println!("Refreshing schema from prometheus...");

        // Load all metrics
        let mut insert_metric_names = Vec::new();
        let mut insert_metric_types = Vec::new();
        let metrics = prometheus_api::metadata(&self.prom_url).await?;
        let metrics = metrics.as_object().expect("metadata is not an object");
        for (name, info) in metrics {
            if name.starts_with("soxhub_") {
                let type_ = info[0]["type"]
                    .as_str()
                    .expect("metric type is not a string");
                if type_ == "histogram" {
                    let bucket_name = format!("{}_bucket", name);
                    if !self.metric_by_name.contains_key(&bucket_name) {
                        insert_metric_names.push(bucket_name);
                        insert_metric_types.push("bucket");
                        insert_metric_names.push(format!("{}_count", name));
                        insert_metric_types.push("count");
                        insert_metric_names.push(format!("{}_sum", name));
                        insert_metric_types.push("sum");
                    }
                }
            }
        }

        // Add new metrics to the database
        if insert_metric_names.is_empty() {
            println!("No new metrics.");
        } else {
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

            // Cache metrics in memory
            println!("Found {} new metrics:", metrics.len());
            for (id, name, type_) in metrics {
                println!(" - Metric: {}", name);
                let index = self.metrics.len();
                let schema = MetricSchema::new(id, name.clone(), type_);
                schema.create_if_not_exists(&self.db).await?;
                self.metrics.push(schema);
                self.metric_by_name.entry(name).or_insert(index);
            }
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
    last_scrape: Option<NaiveDateTime>,
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
            last_scrape: None,
        }
    }

    async fn create_if_not_exists(&self, db: &sqlx::postgres::PgPool) -> Result<()> {
        // Create a table for series labels
        sqlx::query(&format!(
            r#"
                CREATE TABLE IF NOT EXISTS prometheus_series.{} (
                    id serial PRIMARY KEY,
                    labels JSONB NOT NULL,
                    UNIQUE (labels)
                );
            "#,
            self.labels_table
        ))
        .execute(db)
        .await?;

        // Create indices for the values table
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS {}_gin_idx ON prometheus_series.{} USING GIN (labels);",
            self.labels_table, self.labels_table,
        ))
        .execute(db)
        .await?;

        // Create a table for series timestamps+values
        sqlx::query(&format!(
            r#"
                CREATE TABLE IF NOT EXISTS prometheus_series.{} (
                    labels_id int NOT NULL,
                    min_timestamp timestamp NOT NULL,
                    max_timestamp timestamp NOT NULL,
                    count int NOT NULL,
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

        // Create indices for the values table
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

    async fn import_range_query(
        &mut self,
        db: &sqlx::postgres::PgPool,
        mut data: serde_json::Value,
    ) -> Result<()> {
        sqlx::query("UPDATE prometheus_metrics SET last_scrape = $2 WHERE id = $1")
            .bind(self.id)
            .bind(self.last_scrape)
            .execute(db)
            .await?;

        let result = data["result"]
            .as_array_mut()
            .expect("query result is not an array");
        for series in result {
            // Delete the "__name__" label
            let labels = series["metric"]
                .as_object_mut()
                .expect("query metric is not an object");
            labels.remove("__name__");
            let labels = series["metric"].take();

            // Upsert labels, returning the new or existing id
            let (labels_id,): (i32,) = sqlx::query_as(&format!(
                r#"
                    WITH inserted AS (
                        INSERT INTO prometheus_series.{} (labels)
                        VALUES ($1)
                        ON CONFLICT DO NOTHING
                        RETURNING id
                    )
                    SELECT id FROM inserted
                    UNION
                    SELECT id FROM prometheus_series.{} WHERE labels = $1 LIMIT 1
                "#,
                self.labels_table, self.labels_table,
            ))
            .bind(labels)
            .fetch_one(db)
            .await?;

            // Break the results into 1000 datapoint chunks
            let measures = series["values"]
                .as_array()
                .expect("histogram values is not an array");
            for chunk in measures.chunks(1000) {
                if chunk.is_empty() {
                    break;
                }

                let min_timestamp = chunk[0][0]
                    .as_f64()
                    .expect("histogram value is missing timestamp");
                let min_timestamp = NaiveDateTime::from_timestamp(min_timestamp as i64, 0);
                let max_timestamp = chunk[chunk.len() - 1][0]
                    .as_f64()
                    .expect("histogram value is missing timestamp");
                let max_timestamp = NaiveDateTime::from_timestamp(max_timestamp as i64, 0);

                // Compress the values using delta encoding
                let (timestamps, values) = if self.type_ == "sum" {
                    let mut timestamp_enc =
                        crate::encoding::I64DeltaEncoder::sized_for(chunk.len());
                    let mut value_enc = crate::encoding::F32Encoder::sized_for(chunk.len());
                    for measure in chunk {
                        let timestamp = measure[0]
                            .as_f64()
                            .expect("histogram value is missing timestamp")
                            as i64;
                        timestamp_enc.encode(timestamp);
                        let value = measure[1]
                            .as_str()
                            .expect("histogram value is not a string")
                            .parse::<f64>()
                            .expect("histogram value is not a valid number");
                        value_enc.encode(value as f32);
                    }
                    (timestamp_enc.finish(), value_enc.finish())
                } else {
                    let mut timestamp_enc =
                        crate::encoding::I64DeltaEncoder::sized_for(chunk.len());
                    let mut value_enc = crate::encoding::I64DeltaEncoder::sized_for(chunk.len());
                    for measure in chunk {
                        let timestamp = measure[0]
                            .as_f64()
                            .expect("histogram value is missing timestamp")
                            as i64;
                        timestamp_enc.encode(timestamp);
                        let value = measure[1]
                            .as_str()
                            .expect("histogram value is not a string")
                            .parse::<i64>()
                            .expect("histogram value is not a valid integer");
                        value_enc.encode(value);
                    }
                    (timestamp_enc.finish(), value_enc.finish())
                };

                // Insert the chunk
                sqlx::query(&format!(
                    r#"
                        INSERT INTO prometheus_series.{}
                            (labels_id, min_timestamp, max_timestamp, count, timestamps, values)
                        VALUES
                            ($1, $2, $3, $4, $5, $6)
                    "#,
                    self.values_table,
                ))
                .bind(labels_id)
                .bind(min_timestamp)
                .bind(max_timestamp)
                .bind(values.len() as i32)
                .bind(timestamps)
                .bind(values)
                .execute(db)
                .await?;
            }
        }
        Ok(())
    }
}
