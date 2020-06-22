use anyhow::{Context, Result};
use async_std::sync::Mutex;
use sqlx::prelude::*;
use std::sync::atomic;

use telemetry_prometheus::parser::Sample;
use telemetry_prometheus::{SampleValue, SeriesType};

pub struct SeriesSchema {
    pub name: String,
    pub table: String,
    pub series: SeriesType,
    pub labels: Vec<String>,
    exists: atomic::AtomicBool,
    poison: atomic::AtomicUsize,
    create: Mutex<()>,
}

impl SeriesSchema {
    pub fn new(
        name: String,
        table: String,
        series: SeriesType,
        labels: Vec<String>,
        exists: bool,
    ) -> Self {
        SeriesSchema {
            name,
            table,
            series,
            labels,
            exists: atomic::AtomicBool::new(exists),
            poison: atomic::AtomicUsize::new(0),
            create: Mutex::new(()),
        }
    }

    #[inline]
    pub fn exists(&self) -> bool {
        self.exists.load(atomic::Ordering::Relaxed)
    }

    #[inline]
    pub fn poisoned(&self) -> bool {
        self.poison.load(atomic::Ordering::Relaxed) > 1
    }

    #[inline]
    pub fn poison(&self) {
        self.poison.store(2, atomic::Ordering::Relaxed);
    }

    pub async fn insert(
        &self,
        db: &sqlx::postgres::PgPool,
        time: chrono::NaiveDateTime,
        sample: Sample<'_>,
        extra_labels: &[(String, String)],
    ) -> Result<()> {
        // Check the table has been created with `exists()` which is faster
        // than calling `create_if_not_exists` if the table already exists.
        if !self.exists() {
            if self.poisoned() {
                return Err(anyhow::format_err!("cannot create tables for metric"));
            }
            match self.create_if_not_exists(db).await {
                Ok(()) => (),
                Err(err) => {
                    self.poison.fetch_add(1, atomic::Ordering::SeqCst);
                    return Err(err.context("SeriesSchema::create_if_not_exists"));
                }
            }
        }

        // Collect columns from labels
        let mut argn = 1u32;
        let mut label_columns = String::new();
        let mut label_placeholders = String::new();
        let mut label_values = Vec::new();
        let mut json_labels = serde_json::Map::new();
        for (label, value) in sample.labels {
            if self.labels.iter().any(|l| l == label) {
                label_columns.push_str(",\"");
                label_columns.push_str(label);
                label_columns.push('"');
                label_placeholders.push_str(",$");
                label_placeholders.push_str(&argn.to_string());
                label_values.push(value);
                argn += 1;
            } else {
                json_labels.insert(label.into(), value.into());
            }
        }
        for (label, value) in extra_labels {
            if self.labels.iter().any(|l| l == label) {
                label_columns.push_str(",\"");
                label_columns.push_str(label);
                label_columns.push('"');
                label_placeholders.push_str(",$");
                label_placeholders.push_str(&argn.to_string());
                label_values.push(value.into());
                argn += 1;
            } else {
                json_labels.insert(label.into(), value.clone().into());
            }
        }
        if !json_labels.is_empty() {
            label_columns.push_str(",\"json\"");
            label_placeholders.push_str(",$");
            label_placeholders.push_str(&argn.to_string());
        }

        // Insert labels into series
        let stmt = format!(
            r#"
                INSERT INTO telemetry_series.{table_name} ({columns})
                VALUES ({placeholders})
                ON CONFLICT ON CONSTRAINT {table_name}_unique_idx DO UPDATE SET id = {table_name}.id
                RETURNING id
            "#,
            table_name = self.table,
            columns = label_columns.trim_start_matches(','),
            placeholders = label_placeholders.trim_start_matches(','),
        );
        let mut query = sqlx::query_as::<_, (i64,)>(&stmt);
        for value in &label_values {
            query = query.bind(value.as_ref());
        }
        if !json_labels.is_empty() {
            query = query.bind(serde_json::Value::Object(json_labels));
        }
        let (series_id,) = query
            .fetch_one(db)
            .await
            .context("failed to insert series labels")?;

        // Failed to insert
        let stmt = format!(
            "INSERT INTO telemetry_data.{table_name} (time,series_id,value) VALUES ($1,$2,$3)",
            table_name = self.table,
        );
        let mut query = sqlx::query(&stmt).bind(time).bind(series_id);
        match sample.value {
            SampleValue::F64(n) => query = query.bind(n),
            SampleValue::I64(n) => query = query.bind(n),
        }
        query
            .execute(db)
            .await
            .context("failed to insert data sample")?;

        Ok(())
    }

    pub async fn create_if_not_exists(&self, db: &sqlx::postgres::PgPool) -> Result<()> {
        // Acquire the lock, and only continue if exists is false
        let _lock = self.create.lock().await;
        if self.exists.load(atomic::Ordering::SeqCst) {
            return Ok(());
        }

        // Create a table for the data
        sqlx::query(&format!(
            r#"
                CREATE TABLE telemetry_data.{table_name} (
                    time timestamp NOT NULL,
                    value {value_type} NOT NULL,
                    series_id bigint NOT NULL
                );
            "#,
            table_name = self.table,
            value_type = match self.series {
                SeriesType::Counter | SeriesType::Gauge => "float",
                SeriesType::CounterInteger | SeriesType::GaugeInteger => "bigint",
            },
        ))
        .execute(db)
        .await
        .context("failed to create data table")?;

        // Create a hypertable for the data table
        sqlx::query(&format!(
            "SELECT create_hypertable('telemetry_data.{table_name}', 'time');",
            table_name = self.table,
        ))
        .execute(db)
        .await
        .context("failed to create hypertable")?;

        // Configure compression for the hyper table
        sqlx::query(&format!(
            "ALTER TABLE telemetry_data.{table_name} SET (timescaledb.compress, timescaledb.compress_segmentby = 'series_id');",
            table_name = self.table,
        ))
        .execute(db)
        .await
        .context("failed to set hypertable compression segmentby")?;
        sqlx::query(&format!(
            "SELECT add_compress_chunks_policy('telemetry_data.{table_name}', INTERVAL '7 days');",
            table_name = self.table,
        ))
        .execute(db)
        .await
        .context("failed to set hypertable compression policy")?;

        // Create a table for the labels
        let label_column_definitions =
            std::iter::once("json jsonb DEFAULT 'null'::jsonb NOT NULL".to_string())
                .chain(
                    self.labels
                        .iter()
                        .map(|col| format!("\"{}\" text DEFAULT '' NOT NULL", col)),
                )
                .collect::<Vec<_>>()
                .join(",");
        sqlx::query(&format!(
            r#"
                CREATE TABLE telemetry_series.{table_name} (
                    id bigserial PRIMARY KEY,
                    {column_definitions}
                );
            "#,
            table_name = self.table,
            column_definitions = label_column_definitions,
        ))
        .execute(db)
        .await
        .context("failed to create series table")?;

        // Create unique index for the labels table
        let label_column_constraints = self
            .labels
            .iter()
            .map(|col| format!("\"{}\"", col))
            .chain(std::iter::once("\"json\"".to_string()))
            .collect::<Vec<_>>()
            .join(",");
        sqlx::query(&format!(
            "CREATE UNIQUE INDEX {table_name}_unique_idx ON telemetry_series.{table_name} ({column_constraints});",
            table_name = self.table,
            column_constraints = label_column_constraints,
        ))
        .execute(db)
        .await
        .context("failed to create unique index on series table")?;

        // Create unique constraint for the labels table
        sqlx::query(&format!(
            r#"
                ALTER TABLE telemetry_series.{table_name}
                ADD CONSTRAINT {table_name}_unique_idx
                UNIQUE USING INDEX {table_name}_unique_idx;
            "#,
            table_name = self.table,
        ))
        .execute(db)
        .await
        .context("failed to create unique constraint on series table")?;

        // Create jsonb index for the labels table
        sqlx::query(&format!(
            "CREATE INDEX {table_name}_gin_idx ON telemetry_series.{table_name} USING GIN (json) WHERE json IS NOT NULL;",
            table_name = self.table,
        ))
        .execute(db)
        .await
        .context("failed to create gin index on series table")?;

        // Create a view for the metric
        let label_column_selects = std::iter::once("series.\"json\"".to_string())
            .chain(self.labels.iter().map(|col| format!("series.\"{}\"", col)))
            .collect::<Vec<_>>()
            .join(",");
        sqlx::query(&format!(
            r#"
                CREATE VIEW telemetry_metric.{table_name} AS
                SELECT
                    data."time",
                    data."value",
                    data."series_id",
                    {column_selects}
                FROM telemetry_data.{table_name} data
                LEFT JOIN telemetry_series.{table_name} series ON series.id = data.series_id;
            "#,
            table_name = self.table,
            column_selects = label_column_selects,
        ))
        .execute(db)
        .await
        .context("failed to create gin index on labels table")?;

        // Record the table in the database
        sqlx::query(
            r#"
                INSERT INTO telemetry_catalog.tables
                    (name, table_name, series_type, label_columns)
                VALUES
                    ($1, $2, $3, $4)
            "#,
        )
        .bind(&self.name)
        .bind(&self.table)
        .bind(self.series.as_str())
        .bind(&self.labels)
        .execute(db)
        .await?;

        // Update created at state
        self.exists.store(true, atomic::Ordering::SeqCst);

        Ok(())
    }
}
