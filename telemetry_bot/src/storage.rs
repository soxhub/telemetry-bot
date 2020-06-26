use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use telemetry_prometheus::parser::{MetricTypes, Sample};

#[rustfmt::skip]
#[cfg(feature = "storage-timescale-standalone")]
use {
    chrono::prelude::*,
    heck::SnakeCase,
    parking_lot::RwLock, sqlx::prelude::*,
    std::collections::HashMap,
    telemetry_prometheus::SeriesType,
    telemetry_schema::SeriesSchema,
};

use crate::config::Config;

/// Configures the storage backend from environment variables.
pub async fn from_config(config: &Config) -> Result<Box<dyn Storage>> {
    match config.storage_type.as_str() {
        "standalone" => {
            if cfg!(feature = "storage-timescale-standalone") {
                let storage = StandaloneStorage::from_config(config).await?;
                Ok(Box::new(storage))
            } else {
                Err(anyhow::format_err!("can't use storage type 'standalone': must compiled wit 'feature = storage-timescale-standalone'"))
            }
        }
        "remote" => {
            if cfg!(feature = "storage-remote-write") {
                let storage = RemoteWriteStorage::from_config(config)?;
                Ok(Box::new(storage))
            } else {
                Err(anyhow::format_err!("can't use storage type 'remote': must compiled wit 'feature = storage-remote-write"))
            }
        }
        type_ => Err(anyhow::format_err!("unsupported STORAGE_TYPE: {}", type_)),
    }
}

/// The Storage trait must be implemented by each telemetry storage backend
/// that is intended to received scraped metrics.
#[async_trait]
pub trait Storage: Send + Sync {
    async fn write(
        &self,
        default_timestamp: i64,
        metric_types: MetricTypes<'_>,
        sample_batch: Vec<Sample<'_>>,
        static_labels: &[(String, String)],
    ) -> (usize, Vec<Error>);
}

#[cfg(feature = "storage-remote-write")]
pub struct RemoteWriteStorage {
    write_url: String,
}

#[cfg(feature = "storage-remote-write")]
impl RemoteWriteStorage {
    pub fn from_config(config: &Config) -> Result<RemoteWriteStorage> {
        let write_url = config
            .remote_write_url
            .clone()
            .ok_or_else(|| anyhow::format_err!("missing REMOTE_WRITE_URL"))?;
        Ok(RemoteWriteStorage { write_url })
    }
}

#[async_trait]
#[cfg(feature = "storage-remote-write")]
impl Storage for RemoteWriteStorage {
    async fn write(
        &self,
        default_timestamp: i64,
        _metrics_types: MetricTypes<'_>,
        sample_batch: Vec<Sample<'_>>,
        static_labels: &[(String, String)],
    ) -> (usize, Vec<Error>) {
        let result = telemetry_remote_write::write_samples(
            &self.write_url,
            default_timestamp,
            sample_batch,
            static_labels,
        )
        .await;

        // Wrap the single result in an error
        match result {
            Ok(sent) => (sent, vec![]),
            Err(err) => (0, vec![err]),
        }
    }
}

#[cfg(feature = "storage-timescale-standalone")]
pub struct StandaloneStorage {
    db: &'static sqlx::postgres::PgPool,
    tables: &'static parking_lot::RwLock<HashMap<String, &'static SeriesSchema>>,
}

#[cfg(feature = "storage-timescale-standalone")]
impl StandaloneStorage {
    pub async fn from_config(config: &Config) -> Result<StandaloneStorage> {
        // Open a sqlx connection pool
        let db_url = config
            .database_url
            .clone()
            .ok_or_else(|| anyhow::format_err!("missing DATABASE_URL"))?;
        let db_pool_size = config.database_pool_size;
        println!("Connecting to database...");
        let db = sqlx::postgres::PgPool::builder()
            .max_size(db_pool_size as u32)
            .build(&db_url)
            .await
            .context("connecting to timescaledb")?;
        println!("Connected to {}", db_url);

        // Load known metrics from the database
        println!("Loading metrics metadata...");
        let stmt =
            "SELECT name, table_name, series_type, label_columns FROM telemetry_catalog.tables";
        let metrics: Vec<(String, String, String, Vec<String>)> = sqlx::query_as(stmt)
            .fetch_all(&db)
            .await
            .context("loading pre-existing metrics")?;
        let mut tables: HashMap<String, &'static SeriesSchema> =
            HashMap::with_capacity(metrics.len());
        for (metric, table, type_, labels) in metrics {
            let key = metric.clone();
            let type_ = type_.parse().expect("invalid metric type");
            let table = SeriesSchema::new(metric, table, type_, labels, true);
            tables.insert(key, Box::leak(Box::new(table)));
        }
        println!("Loaded {} metrics.", tables.len());

        // Build storage
        let db: &'static _ = Box::leak(Box::new(db));
        let tables: &'static _ = Box::leak(Box::new(RwLock::new(tables)));
        Ok(StandaloneStorage { db, tables })
    }

    async fn write_sample(
        &self,
        default_timestamp: DateTime<Utc>,
        sample: Sample<'_>,
        static_labels: &[(String, String)],
        metric_types: &MetricTypes<'_>,
    ) -> Result<bool> {
        let time = sample
            .timestamp
            .map(|t| DateTime::from_utc(NaiveDateTime::from_timestamp(t, 0), Utc))
            .unwrap_or(default_timestamp);

        // Load the table for this metric
        let key = sample.name;
        let tables = self.tables;
        let mut table: Option<&'static SeriesSchema> = (*tables.read()).get(key).copied();
        if table.is_none() {
            if let Some(type_) = metric_types.get(sample.name) {
                tables.write().entry(key.to_string()).or_insert_with(|| {
                    Box::leak(Box::new(Self::build_series(*type_, &sample, static_labels)))
                });
                table = (*tables.read()).get(key).copied();
            }
        }

        // Insert the data point
        if let Some(table) = table {
            if let Err(err) = table.insert(self.db, time, sample, static_labels).await {
                if table.poisoned() {
                    return Ok(false);
                } else {
                    return Err(err);
                }
            }
        }

        Ok(true)
    }

    fn build_series(
        type_: SeriesType,
        sample: &Sample,
        static_labels: &[(String, String)],
    ) -> SeriesSchema {
        let name = sample.name.to_owned();
        let table = name.to_snake_case();

        // Collect the list of expected labels from a sample measurement
        let mut labels = Vec::new();
        for (label, _) in static_labels {
            labels.push(label.clone());
        }
        for (label, _) in &sample.labels {
            if *label != "time"
                && *label != "value"
                && *label != "series_id"
                && *label != "json"
                && !label.starts_with("__")
                && label
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '_' || ch == '-')
            {
                labels.push(label.to_string());
            }
        }

        // Build the series
        let series = SeriesSchema::new(name, table, type_, labels, false);

        // Postgres table names have a max length of 63 characters.
        // We need to reserve 11 extra characters for constraint and index names
        if series.table.len() > (63 - 11) {
            // FIXME: Instead we should truncate the table_name and append a unique ID
            eprintln!(
                "Warn: Cannot store metric '{}', metric name too long",
                series.table
            );
            series.poison();
        }

        series
    }
}

#[async_trait]
#[cfg(feature = "storage-timescale-standalone")]
impl Storage for StandaloneStorage {
    async fn write(
        &self,
        default_timestamp: i64,
        metric_types: MetricTypes<'_>,
        sample_batch: Vec<Sample<'_>>,
        static_labels: &[(String, String)],
    ) -> (usize, Vec<Error>) {
        let mut sent = 0;
        let mut errors = Vec::new();
        let default_timestamp =
            DateTime::from_utc(NaiveDateTime::from_timestamp(default_timestamp, 0), Utc);
        for sample in sample_batch {
            let time = sample
                .timestamp
                .map(|t| DateTime::from_utc(NaiveDateTime::from_timestamp(t, 0), Utc))
                .unwrap_or(default_timestamp);
            match self
                .write_sample(time, sample, static_labels, &metric_types)
                .await
            {
                Ok(false) => (), // skipped
                Ok(true) => sent += 1,
                Err(err) => errors.push(err),
            }
        }
        (sent, errors)
    }
}
