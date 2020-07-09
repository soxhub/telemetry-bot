use anyhow::{Context, Error, Result};
use chrono::prelude::*;
use dashmap::DashMap;
use lasso::{Spur, ThreadedRodeo};
use sqlx::prelude::*;
use sqlx::Executor;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use telemetry_prometheus::parser::SampleSet;

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct LabelNameKey(Spur);

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct LabelValueKey(Spur);

#[repr(transparent)]
#[derive(Eq, PartialEq, Hash)]
struct SeriesKey(Box<[(LabelNameKey, LabelValueKey)]>);

#[derive(Default)]
struct HistogramRow {
    count: Option<f64>,
    sum: Option<f64>,
    buckets: HashMap<String, f64>,
}

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

    /// Whether to write histograms to the custom `prom_data_histogram` schema
    use_histogram_schema: bool,
}

impl Connector {
    pub fn new(db: sqlx::postgres::PgPool) -> Self {
        Self {
            db,
            series: DashMap::new(),
            metrics: DashMap::new(),
            label_names: ThreadedRodeo::new(),
            label_values: ThreadedRodeo::new(),
            use_histogram_schema: false,
        }
    }

    pub fn with_ext_histogram_schema(mut self, enabled: bool) -> Self {
        self.use_histogram_schema = enabled;
        self
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
        scraped: SampleSet<'_>,
        static_labels: &[(String, String)],
    ) -> (usize, Vec<Error>) {
        let mut errors = Vec::new();

        let mut histogram_rows: HashMap<(Arc<String>, i32), HistogramRow> = HashMap::new();
        let mut histogram_metrics = HashMap::new();
        if self.use_histogram_schema {
            histogram_metrics.reserve(scraped.histograms.len() * 3);
            for name in scraped.histograms {
                histogram_metrics.insert(format!("{}_bucket", name), name);
                histogram_metrics.insert(format!("{}_count", name), name);
                histogram_metrics.insert(format!("{}_sum", name), name);
            }
        }

        // TODO: Maybe create a channel+task per metric to perform batch inserts,
        //       instead of inserting only a couple values at a time as is more likely here.
        let mut metric_rows: HashMap<Arc<String>, (Vec<DateTime<Utc>>, Vec<f64>, Vec<i32>)> =
            HashMap::with_capacity(scraped.samples.len());
        for sample in scraped.samples {
            // If use_histogram_schema is enabled, then store histogram metrics in a different schema
            if let Some(histogram_name) = histogram_metrics.get(sample.name) {
                /* ==================================
                 * Collect rows for `histogram` table
                 * ================================== */

                // Upsert the histogram table
                let metric_table_name = if let Some(table_name) = self.metrics.get(sample.name) {
                    Arc::clone(&table_name)
                } else {
                    match self.upsert_histogram(histogram_name).await {
                        Ok(table_name) => table_name,
                        Err(err) => {
                            errors.push(err);
                            continue;
                        }
                    }
                };

                // Collect dynamic and static labels
                let num_labels = 1 + static_labels.len() + sample.labels.len();
                let mut bucket = None;
                let mut label_pairs = Vec::with_capacity(num_labels);
                label_pairs.push(("__name__", *histogram_name));
                for (name, value) in static_labels {
                    label_pairs.push((&name, &value));
                }
                for (name, value) in &sample.labels {
                    if *name == "le" {
                        bucket = Some(value);
                    } else {
                        label_pairs.push((&name, &value));
                    }
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

                // Find the series id and save the histogram row to be inserted
                if let Some(series_id) = self.series.get(&series_key) {
                    let row = histogram_rows
                        .entry((metric_table_name, *series_id))
                        .or_default();
                    if sample.name.ends_with("et") {
                        // ends_with buck(et)
                        if let Some(le) = bucket {
                            row.buckets.insert(le.to_string(), sample.value.to_f64());
                        }
                    } else if sample.name.ends_with('m') {
                        // ends_with su(m)
                        row.sum = Some(sample.value.to_f64().into());
                    } else {
                        // ends_with count
                        row.count = Some(sample.value.to_f64().into());
                    }
                } else {
                    match self
                        .upsert_series(&sample.name, series_key, &label_pairs)
                        .await
                    {
                        Ok(series_id) => {
                            let row = histogram_rows
                                .entry((metric_table_name, series_id))
                                .or_default();
                            if sample.name.ends_with("et") {
                                // ends_with buck(et)
                                if let Some(bucket) = bucket {
                                    row.buckets
                                        .insert(bucket.to_string(), sample.value.to_f64().into());
                                }
                            } else if sample.name.ends_with('m') {
                                // ends_with su(m)
                                row.sum = Some(sample.value.to_f64().into());
                            } else {
                                // ends_with count
                                row.count = Some(sample.value.to_f64().into());
                            }
                        }
                        Err(err) => {
                            errors.push(err);
                            continue;
                        }
                    }
                }
            } else {
                /* ==================================
                 * Collect rows for `prom_data` table
                 * ================================== */

                // Get the normal metric table name
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
                let timestamp =
                    DateTime::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
                if let Some(series_id) = self.series.get(&series_key) {
                    let (times, values, series_ids) =
                        metric_rows.entry(metric_table_name).or_default();
                    times.push(timestamp);
                    values.push(sample.value.to_f64());
                    series_ids.push(*series_id);
                } else {
                    match self
                        .upsert_series(&sample.name, series_key, &label_pairs)
                        .await
                    {
                        Ok(series_id) => {
                            let (times, values, series_ids) =
                                metric_rows.entry(metric_table_name).or_default();
                            times.push(timestamp);
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
        }

        // Insert the data rows
        let mut inserted = 0;
        for (table_name, (times, values, series_ids)) in metric_rows {
            let insert_result = sqlx::query(&format!(
                r#"
                    INSERT INTO {schema_name}.{table_name} ("time", "value", "series_id")
                    SELECT * FROM UNNEST('{timestamp_arr_str}'::timestamptz[], $1::float8[], $2::int4[])
                "#,
                table_name = table_name,
                schema_name = schema::DATA_SCHEMA,
                // TEMPORARY: attempt to insert timestampts using text format
                timestamp_arr_str = format!("{{ {} }}",
                    times.iter().map(DateTime::to_rfc3339).collect::<Vec<_>>().join(",")
                ),
            ))
            .bind(&values)
            .bind(&series_ids)
            .execute(&self.db)
            .await;
            match insert_result {
                Ok(rows) => inserted += rows as usize,
                Err(err) => errors.push(Error::new(err).context("error inserting data rows")),
            }
        }

        // Insert the histogram rows
        if self.use_histogram_schema {
            type Batch = (
                Vec<DateTime<Utc>>,
                Vec<i32>,
                Vec<f64>,
                Vec<f64>,
                Vec<HashMap<String, f64>>,
            );
            let timestamp =
                DateTime::from_utc(NaiveDateTime::from_timestamp(default_timestamp, 0), Utc);
            let mut histogram_batches: HashMap<Arc<String>, Batch> = HashMap::new();
            for ((table_name, series_id), row) in histogram_rows {
                if let (Some(count), Some(sum)) = (row.count, row.sum) {
                    let (times, series_ids, counts, sums, buckets) =
                        histogram_batches.entry(table_name).or_default();
                    times.push(timestamp);
                    series_ids.push(series_id);
                    counts.push(count);
                    sums.push(sum);
                    buckets.push(row.buckets);
                }
            }
            for (table_name, (times, series_ids, counts, sums, buckets)) in histogram_batches {
                let insert_result = sqlx::query(&format!(
                    r#"
                        INSERT INTO {schema_name}.{table_name} ("time", "series_id", "count", "sum", "le")
                        SELECT * FROM UNNEST(
                            ARRAY[{timestamp_elements}]::timestamptz[],
                            $1::int4[],
                            $2::float8[],
                            $3::float8[],
                            ARRAY[{jsonb_elements}]::jsonb[]
                        )
                    "#,
                    table_name = table_name,
                    schema_name = schema::HISTOGRAM_DATA_SCHEMA,
                    // TEMPORARY: insert timestampts using text format
                    //            b.c. sqlx has a bug with serializing in binary format
                    timestamp_elements = times.iter().map(|t| format!("'{}'", t.to_rfc3339())).collect::<Vec<_>>().join(","),
                    jsonb_elements = buckets.iter().map(|b| format!("'{}'", serde_json::to_string(b).unwrap_or_default())).collect::<Vec<_>>().join(","),
                ))
                .bind(&series_ids)
                .bind(&counts)
                .bind(&sums)
                .execute(&self.db)
                .await;
                match insert_result {
                    Ok(rows) => inserted += rows as usize,
                    Err(err) => {
                        errors.push(Error::new(err).context("error inserting histogram rows"))
                    }
                }
            }
        }

        (inserted, errors)
    }

    async fn upsert_histogram(&self, histogram_name: &str) -> Result<Arc<String>> {
        let table_name = self.upsert_metric(histogram_name).await?;
        let mut tx = self
            .db
            .begin()
            .await
            .context("failed to begin tx in upsert_histogram")?;
        {
            let (metric_id,): (i32,) = sqlx::query_as(&format!(
                "SELECT id FROM {}.metric WHERE metric_name = $1",
                schema::CATALOG_SCHEMA
            ))
            .bind(&histogram_name)
            .fetch_one(&mut tx)
            .await
            .context("error fetching metric id")?;
            let stmt = format!(
                r#"
                    CREATE TABLE IF NOT EXISTS {histogram_data_schema}.{table_name} (
                        "time" timestamptz NOT NULL,
                        "series_id" int4 NOT NULL,
                        "count" float8 NOT NULL,
                        "sum" float8 NOT NULL,
                        "le" jsonb NOT NULL
                    );
                    CREATE INDEX data_histogram_series_id_time_{metric_id}
                        ON {histogram_data_schema}.{table_name} (series_id, time) INCLUDE (count, sum)
                    ;
                    SELECT create_hypertable('{histogram_data_schema}.{table_name}', 'time',
                                             chunk_time_interval=>{catalog_schema}.get_default_chunk_interval(),
                                             create_default_indexes=>false);
                    ALTER TABLE {histogram_data_schema}.{table_name} SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'series_id',
                        timescaledb.compress_orderby = 'time'
                    );
                    SELECT add_compress_chunks_policy('{histogram_data_schema}.{table_name}', INTERVAL '1 hour');
                "#,
                histogram_data_schema = schema::HISTOGRAM_DATA_SCHEMA,
                catalog_schema = schema::CATALOG_SCHEMA,
                table_name = table_name,
                metric_id = metric_id,
            );
            tx.execute(stmt.as_str())
                .await
                .context("error creating histogram hypertable")?;
        }
        tx.commit()
            .await
            .context("failed to commit histogram hypertable")?;
        Ok(table_name)
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
                .await
                .context("error upserting series")?;

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

    pub const CATALOG_SCHEMA: &str = "_prom_catalog";
    pub const DATA_SCHEMA: &str = "prom_data";
    pub const HISTOGRAM_DATA_SCHEMA: &str = "prom_histogram_data";

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
