use anyhow::{Context, Result};
use async_std::sync::Mutex;
use sqlx::prelude::*;
use std::str::FromStr;
use std::sync::atomic;

use crate::parser::{Measurement, Number};

#[derive(Copy, Clone)]
pub enum SeriesType {
    Counter,
    CounterInteger,
    Gauge,
    GaugeInteger,
}

impl SeriesType {
    fn as_str(&self) -> &str {
        match self {
            SeriesType::Counter => "Counter",
            SeriesType::CounterInteger => "CounterInteger",
            SeriesType::Gauge => "Gauge",
            SeriesType::GaugeInteger => "GaugeInteger",
        }
    }

    fn as_sql_type(&self) -> &'static str {
        match self {
            SeriesType::Counter | SeriesType::Gauge => "float",
            SeriesType::CounterInteger | SeriesType::GaugeInteger => "bigint",
        }
    }
}

impl FromStr for SeriesType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Counter" => Ok(SeriesType::Counter),
            "CounterInteger" => Ok(SeriesType::CounterInteger),
            "Gauge" => Ok(SeriesType::Gauge),
            "GaugeInteger" => Ok(SeriesType::GaugeInteger),
            _ => Err(()),
        }
    }
}

pub struct SeriesSchema {
    pub name: String,
    pub table: String,
    pub series: SeriesType,
    pub labels: Vec<String>,
    exists: atomic::AtomicBool,
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
            create: Mutex::new(()),
        }
    }

    #[inline]
    pub fn exists(&self) -> bool {
        self.exists.load(atomic::Ordering::Relaxed)
    }

    pub async fn insert(
        &self,
        db: &sqlx::postgres::PgPool,
        time: chrono::NaiveDateTime,
        data: Measurement<'_>,
        extra_labels: &[(String, String)],
    ) -> Result<()> {
        // Check the table has been created with `exists()` which is faster
        // than calling `create_if_not_exists` if the table already exists.
        if !self.exists() {
            self.create_if_not_exists(db)
                .await
                .context("SeriesSchema::create_if_not_exists")?;
        }

        // Collect columns from labels
        let mut argn = 1u32;
        let mut label_columns = String::new();
        let mut label_placeholders = String::new();
        let mut label_values = Vec::new();
        let mut json_labels = serde_json::Map::new();
        for (label, value) in data.labels {
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
                ON CONFLICT {table_name}_unique_idx DO UPDATE SET id = excluded.id
                RETURNING id
            "#,
            table_name = self.table,
            columns = label_columns,
            placeholders = label_placeholders,
        );
        let mut query = sqlx::query_as::<_, (i64,)>(&stmt);
        query = query.bind(if json_labels.len() > 0 {
            Some(serde_json::Value::Object(json_labels))
        } else {
            None
        });
        for value in &label_values {
            query = query.bind(value.as_ref());
        }
        let (series_id,) = query
            .fetch_one(db)
            .await
            .context("failed to insert measurment labels")?;

        // Failed to insert
        let stmt = format!(
            "INSERT INTO telemetry_data.{table_name} (time,series_id,value) VALUES ($1,$2,$3)",
            table_name = self.table,
        );
        let mut query = sqlx::query(&stmt).bind(time).bind(series_id);
        match data.value {
            Number::F64(n) => query = query.bind(n),
            Number::I64(n) => query = query.bind(n),
        }
        query
            .execute(db)
            .await
            .context("failed to insert measurment data")?;

        Ok(())
    }

    pub async fn create_if_not_exists(&self, db: &sqlx::postgres::PgPool) -> Result<()> {
        // Acquire the lock, and only continue if exists is false
        let _lock = self.create.lock().await;
        if self.exists.load(atomic::Ordering::SeqCst) {
            return Ok(());
        }

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

        // Create a table for the data
        sqlx::query(&format!(
            r#"
                CREATE TABLE telemetry_data.{table_name} (
                    time timestamp NOT NULL,
                    value {value_type} NOT NULL,
                    series_id text NOT NULL,
                );
            "#,
            table_name = self.table,
            value_type = self.series.as_sql_type(),
        ))
        .execute(db)
        .await
        .context("failed to create telemetry_data table")?;

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
            "SELECT add_compress_chunks_policy('telemetry_metrics.{table_name}', INTERVAL '7 days');",
            table_name = self.table,
        ))
        .execute(db)
        .await
        .context("failed to set hypertable compression policy")?;

        // Create a table for the labels
        let label_column_definitions = std::iter::once("json jsonb".to_string())
            .chain(self.labels.iter().map(|col| format!("{} text", col)))
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
        .context("failed to create telemetry_series table")?;

        // Create unique index for the labels table
        let label_column_names = std::iter::once("json")
            .chain(self.labels.iter().map(|col| col.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        sqlx::query(&format!(
            r#"
                CREATE UNIQUE INDEX {table_name}_unique_idx
                ON telemetry_series.{table_name} ({column_names})
                WHERE labels IS NOT NULL;
            "#,
            table_name = self.table,
            column_names = label_column_names,
        ))
        .execute(db)
        .await
        .context("failed to create unique index on labels table")?;

        // Create jsonb index for the labels table
        sqlx::query(&format!(
            r#"
                CREATE INDEX {table_name}_gin_idx
                ON telemetry_series.{table_name} USING GIN (json)
                WHERE labels IS NOT NULL;
            "#,
            table_name = self.table,
        ))
        .execute(db)
        .await
        .context("failed to create gin index on labels table")?;

        // Update created at state
        self.exists.store(true, atomic::Ordering::SeqCst);

        Ok(())
    }
}
