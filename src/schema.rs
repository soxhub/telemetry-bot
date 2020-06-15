use anyhow::Result;
use async_std::sync::Mutex;
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
    fn sql_type(&self) -> &'static str {
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

pub struct SeriesTable {
    pub name: String,
    pub table: String,
    pub schema: String,
    pub series: SeriesType,
    pub labels: Vec<String>,
    exists: atomic::AtomicBool,
    create: Mutex<()>,
}

impl SeriesTable {
    pub fn new(
        name: String,
        table: String,
        schema: String,
        series: SeriesType,
        labels: Vec<String>,
        exists: bool,
    ) -> Self {
        SeriesTable {
            name,
            table,
            schema,
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
            self.create_if_not_exists(db).await?;
        }

        // Collect columns from labels
        let mut argn = 4u32;
        let mut columns = String::from("time,value,labels");
        let mut placeholders = String::from("$1,$2,$3");
        let mut label_values = Vec::new();
        let mut json_labels = serde_json::Map::new();
        for (label, value) in data.labels {
            if self.labels.iter().any(|l| l == label) {
                columns.push_str(",\"");
                columns.push_str(label);
                columns.push('"');
                placeholders.push('$');
                placeholders.push_str(&argn.to_string());
                label_values.push(value);
                argn += 1;
            } else {
                json_labels.insert(label.into(), value.into());
            }
        }
        for (label, value) in extra_labels {
            if self.labels.iter().any(|l| l == label) {
                columns.push_str(",\"");
                columns.push_str(label);
                columns.push('"');
                placeholders.push_str("$");
                placeholders.push_str(&argn.to_string());
                label_values.push(value.into());
                argn += 1;
            } else {
                json_labels.insert(label.into(), value.clone().into());
            }
        }

        // Build query
        let stmt = format!(
            "INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})",
            schema_name = self.schema,
            table_name = self.table,
            columns = columns,
            placeholders = placeholders,
        );
        let mut query = sqlx::query(&stmt);

        // Set timestamp
        query = query.bind(time);

        // Set value
        match data.value {
            Number::F64(n) => query = query.bind(n),
            Number::I64(n) => query = query.bind(n),
        }

        // Set labels
        query = query.bind(if json_labels.len() > 0 {
            Some(serde_json::Value::Object(json_labels))
        } else {
            None
        });
        for value in &label_values {
            query = query.bind(value.as_ref());
        }

        // Insert data
        query.execute(db).await?;

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
                INSERT INTO telemetry_catalog.metrics_tables (name, table_name, series_type, label_columns)
                VALUES ($1, $2, $3, $4, $5)
            "#
        )
        .bind(&self.name)
        .bind(&self.table)
        .bind(&self.schema)
        .bind(self.series.sql_type())
        .bind(&self.labels)
        .execute(db)
        .await?;

        // Create a table for the series
        sqlx::query(&format!(
            r#"
                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    time timestamp,
                    value {value_type},
                    labels jsonb
                );
            "#,
            schema_name = self.schema,
            table_name = self.table,
            value_type = self.series.sql_type(),
        ))
        .execute(db)
        .await?;

        // Add columns to the table
        for label in &self.labels {
            sqlx::query(&format!(
                r#"
                    ALTER TABLE {schema_name}.{table_name}
                        ADD COLUMN IF NOT EXISTS {column_name} text
                    ;
                "#,
                schema_name = self.schema,
                table_name = self.table,
                column_name = label,
            ))
            .execute(db)
            .await?;
        }

        // Create a hypertable for the series
        sqlx::query(&format!(
            "SELECT create_hypertable('{schema_name}.{table_name}', 'time');",
            schema_name = self.schema,
            table_name = self.table,
        ))
        .execute(db)
        .await?;

        // Create indices for the table
        sqlx::query(&format!(
            r#"
                CREATE INDEX IF NOT EXISTS {table_name}_gin_idx
                ON {schema_name}.{table_name} USING GIN (labels)
                WHERE labels IS NOT NULL;
            "#,
            schema_name = self.schema,
            table_name = self.table,
        ))
        .execute(db)
        .await?;

        self.exists.store(true, atomic::Ordering::SeqCst);

        Ok(())
    }
}
