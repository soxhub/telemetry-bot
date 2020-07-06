use anyhow::{Context, Error, Result};
use chrono::prelude::*;
use dashmap::DashMap;
use lasso::{Spur, ThreadedRodeo};
use sqlx::prelude::*;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use telemetry_prometheus::parser::Sample;

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct LabelNameKey(Spur);

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct LabelValueKey(Spur);

#[repr(transparent)]
#[derive(Eq, PartialEq, Hash)]
struct SeriesKey(Box<[(LabelNameKey, LabelValueKey)]>);

pub struct Connector {
    db: sqlx::postgres::PgPool,

    /// A map of label sets to series ids
    series: DashMap<SeriesKey, i32>,

    /// A map of metric name to metric table name
    metrics: DashMap<String, Arc<String>>,

    /// An interned map of label names to integer ids
    label_names: ThreadedRodeo<Spur>,

    /// An interned map of label values to integer ids
    label_values: ThreadedRodeo<Spur>,
}

impl Connector {
    pub fn new(db: sqlx::postgres::PgPool) -> Self {
        Self {
            db,
            series: DashMap::new(),
            metrics: DashMap::new(),
            label_names: ThreadedRodeo::new(),
            label_values: ThreadedRodeo::new(),
        }
    }

    // Call to initialize a connector from the existing database state
    pub async fn resume(&self) -> Result<()> {
        // TODO: Load existing metrics table names

        // On startup, run to recover any potentially incomplete metric
        schema::finalize_metric_creation(&self.db).await;

        Ok(())
    }

    /// Write a batch of samples to the remote write storage backend
    pub async fn write_samples(
        &self,
        default_timestamp: i64,
        scraped_samples: Vec<Sample<'_>>,
        static_labels: &[(String, String)],
    ) -> (usize, Vec<Error>) {
        let mut errors = Vec::new();

        // TODO: Maybe create a channel+task per metric to perform batch inserts,
        //       instead of inserting only a couple values at a time as is more likely here.
        let mut batches: HashMap<Arc<String>, (Vec<DateTime<Utc>>, Vec<f64>, Vec<i32>)> =
            HashMap::with_capacity(scraped_samples.len());
        for sample in scraped_samples {
            // Get metric table name
            let metric_table_name = if let Some(table_name) = self.metrics.get(sample.name) {
                Arc::clone(&table_name)
            } else {
                match self.upsert_metric(&sample.name).await {
                    Ok(table_name) => table_name,
                    Err(err) => {
                        errors.push(err);
                        continue;
                    }
                }
            };

            // Collect dynamic and static labels
            let num_labels = 1 + static_labels.len() + sample.labels.len();
            let mut label_pairs = Vec::with_capacity(num_labels);
            label_pairs.push(("__name__", sample.name));
            for (name, value) in static_labels {
                label_pairs.push((&name, &value));
            }
            for (name, value) in &sample.labels {
                label_pairs.push((&name, &value));
            }

            // Labels must be sorted by name alphabetically
            label_pairs.sort_by(|a, b| a.0.cmp(&b.0));

            // Create a comparable + hashable key for the series
            let label_keys = label_pairs
                .iter()
                .map(|(name, value)| {
                    (
                        LabelNameKey(self.label_names.get_or_intern(name)),
                        LabelValueKey(self.label_values.get_or_intern(value)),
                    )
                })
                .collect::<Vec<_>>();
            let series_key = SeriesKey(label_keys.into_boxed_slice());

            // Find the series id and save the data row to be inserted
            let timestamp = sample.timestamp.unwrap_or(default_timestamp);
            if let Some(series_id) = self.series.get(&series_key) {
                let (times, values, series_ids) = batches.entry(metric_table_name).or_default();
                times.push(DateTime::from_utc(
                    NaiveDateTime::from_timestamp(timestamp, 0),
                    Utc,
                ));
                values.push(sample.value.to_f64());
                series_ids.push(*series_id);
            } else {
                match self
                    .upsert_series(&sample.name, series_key, &label_pairs)
                    .await
                {
                    Ok(series_id) => {
                        let (times, values, series_ids) =
                            batches.entry(metric_table_name).or_default();
                        times.push(DateTime::from_utc(
                            NaiveDateTime::from_timestamp(timestamp, 0),
                            Utc,
                        ));
                        values.push(sample.value.to_f64());
                        series_ids.push(series_id);
                    }
                    Err(err) => {
                        errors.push(err);
                        continue;
                    }
                }
            }
        }

        // Insert the data values
        let mut inserted = 0;
        for (table_name, (times, values, series_ids)) in batches {
            let insert_result = sqlx::query(&format!(
                r#"
                    INSERT INTO {schema_name}.{table_name} ("time", "value", "series_id")
                    SELECT * FROM UNNEST($1::timestamptz[], $2::float8, $3::int4[])
                "#,
                table_name = table_name,
                schema_name = schema::DATA_SCHEMA
            ))
            .bind(&times)
            .bind(&values)
            .bind(&series_ids)
            .execute(&self.db)
            .await;
            match insert_result {
                Ok(rows) => inserted += rows as usize,
                Err(err) => errors.push(Error::new(err).context("error inserting data")),
            }
        }

        (inserted, errors)
    }

    async fn upsert_metric(&self, metric_name: &str) -> Result<Arc<String>> {
        let (table_name, possibly_new): (String, bool) =
            sqlx::query_as(schema::UPSERT_METRICS_TABLE_NAME)
                .bind(metric_name)
                .fetch_one(&self.db)
                .await?;
        let table_name = Arc::new(table_name);

        // Cache the table name for the metric
        self.metrics.insert(metric_name.into(), table_name.clone());

        // If we created a metric, finalize the metric creation in separate task
        if possibly_new {
            let db = self.db.clone();
            async_std::task::spawn(async move { schema::finalize_metric_creation(&db).await });
        }

        Ok(table_name)
    }

    async fn upsert_series(
        &self,
        metric: &str,
        series_key: SeriesKey,
        label_pairs: &[(&str, &str)],
    ) -> Result<i32> {
        let labels = label_pairs.iter().map(|(a, _)| *a).collect::<Vec<_>>();
        let values = label_pairs.iter().map(|(_, b)| *b).collect::<Vec<_>>();
        let (_table_name, series_id): (String, i64) =
            sqlx::query_as(schema::UPSERT_SERIES_ID_FOR_LABELS)
                .bind(metric)
                .bind(&labels)
                .bind(&values)
                .fetch_one(&self.db)
                .await?;

        // // NOTE: Unlike `prometheus_connector` we are not performing multiple upserts
        // //       in a batch, so it isn't necessary to segregate them into separate transactions.
        // let mut tx = self.db.begin().await?;
        // let (_table_name, series_id): (String, i32) =
        //     sqlx::query_as(schema::UPSERT_SERIES_ID_FOR_LABELS)
        //         .bind(metric)
        //         .bind(&labels)
        //         .bind(&values)
        //         .fetch_one(&mut tx)
        //         .await?;
        // tx.commit().await?;

        // While the `prom_data_series.*` tables use an int8,
        // the `prom_data` table uses an int4.
        let series_id = series_id
            .try_into()
            .context("invalid series id (too large)")?;

        // Cache the id for the series
        self.series.insert(series_key, series_id);

        Ok(series_id)
    }
}

mod schema {
    use anyhow::Error;
    use telemetry_prometheus::error::debug_error;

    pub const DATA_SCHEMA: &str = "prom_data";
    // pub const CATALOG_SCHEMA: &str = "_prom_catalog";

    pub const UPSERT_METRICS_TABLE_NAME: &str =
        "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)";
    pub const UPSERT_SERIES_ID_FOR_LABELS: &str =
        "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)";
    pub const CALL_FINALIZE_METRIC_CREATION: &str = "CALL _prom_catalog.finalize_metric_creation()";

    pub async fn finalize_metric_creation(db: &sqlx::postgres::PgPool) {
        if let Err(err) = sqlx::query(CALL_FINALIZE_METRIC_CREATION).execute(db).await {
            debug_error(Error::new(err).context("error finalizing metric"));
        }
    }
}
