mod schema;

use anyhow::{Context, Error, Result};
use chrono::prelude::*;
use dashmap::DashMap;
use lasso::{Spur, ThreadedRodeo};
use sqlx::prelude::*;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Write;
use std::sync::Arc;

use telemetry_core::debug::DEBUG;
use telemetry_core::parser::SampleSet;

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct LabelNameKey(Spur);

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct LabelValueKey(Spur);

#[repr(transparent)]
#[derive(Clone, Eq, PartialEq, Hash)]
struct SeriesKey(Box<[u8]>);

impl SeriesKey {
    pub fn size_of(&self) -> usize {
        self.0.len()
    }
}

type SampleRow = (DateTime<Utc>, f64, i32);

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
        // Run database migrations
        schema::migrate(&self.db).await?;

        // On startup, run to recover any potentially incomplete metric
        schema::finalize_metric_creation(&self.db).await;

        Ok(())
    }

    fn build_series_key(&self, label_pairs: &Vec<(&str, &str)>) -> SeriesKey {
        let mut key_builder = encoding::U32ZigZagEncoder::sized_for(label_pairs.len() * 2);
        debug_assert_eq!(
            std::mem::size_of::<LabelNameKey>(),
            std::mem::size_of::<u32>()
        );
        debug_assert_eq!(
            std::mem::size_of::<LabelValueKey>(),
            std::mem::size_of::<u32>()
        );
        for (name, _) in label_pairs {
            let name_key = LabelNameKey(self.label_names.get_or_intern(name));

            // Safety: Spur is a #[repr(transparent)] wrapper around NonZeroU32
            let name_id = unsafe { std::mem::transmute::<LabelNameKey, u32>(name_key) };

            // Encode the ids into our series key
            key_builder.encode(name_id);
        }
        for (_, value) in label_pairs {
            let value_key = LabelValueKey(self.label_values.get_or_intern(value));

            // Safety: Spur is a #[repr(transparent)] wrapper around NonZeroU32
            let value_id = unsafe { std::mem::transmute::<LabelValueKey, u32>(value_key) };

            // Encode the ids into our series key
            key_builder.encode(value_id);
        }
        SeriesKey(key_builder.finish().into_boxed_slice())
    }

    /// Write a batch of samples to the remote write storage backend
    pub async fn write_samples(
        &self,
        default_timestamp: i64,
        scraped: SampleSet<'_>,
        static_labels: &[(String, String)],
    ) -> (usize, Vec<Error>) {
        DEBUG.write_task_started();

        let mut errors = Vec::new();

        // Collect samples as rows per table
        let mut rows: HashMap<Arc<String>, Vec<SampleRow>> =
            HashMap::with_capacity(scraped.samples.len());
        let mut missing_series = Vec::new();
        let mut incomplete_samples = Vec::new();
        for sample in &scraped.samples {
            // Get the normal metric table name
            let metric_table = if let Some(table_name) = self.metrics.get(sample.name) {
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
            let series_key = self.build_series_key(&label_pairs);

            // Find already known series ids and save the data row to be inserted
            let timestamp = sample.timestamp.unwrap_or(default_timestamp);
            let timestamp = DateTime::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
            let value = sample.value.to_f64();
            if let Some(series_id) = self.series.get(&series_key) {
                let row = (timestamp, value, *series_id);
                rows.entry(metric_table).or_default().push(row);
            } else {
                missing_series.push((sample.name, series_key, label_pairs));
                incomplete_samples.push((metric_table, timestamp, value));
            }
        }

        // Upsert any missing series
        if !missing_series.is_empty() {
            match self.upsert_series_batch(missing_series).await {
                Ok(series_ids) => {
                    for (series_id, (table, timestamp, value)) in
                        series_ids.into_iter().zip(incomplete_samples)
                    {
                        let row = (timestamp, value, series_id);
                        rows.entry(table).or_default().push(row);
                    }
                }
                Err(err) => {
                    errors.push(err);
                }
            }
        }

        // Insert the data rows
        let mut inserted = 0;
        for (table_name, rows) in rows {
            let insert_count = rows.len();
            match insert_samples(&self.db, &table_name, rows).await {
                Ok(_) => inserted += insert_count,
                Err(err) => errors.push(err),
            }
        }

        DEBUG.write_task_finished();
        (inserted, errors)
    }

    /// Ensure tables for the metric with the given name exist, and cache the table name
    async fn upsert_metric(&self, metric_name: &str) -> Result<Arc<String>> {
        let (table_name, finalize_metric): (String, bool) =
            sqlx::query_as(schema::UPSERT_METRIC_TABLE_NAME)
                .bind(metric_name)
                .fetch_one(&self.db)
                .await
                .context("error upserting metric")?;
        DEBUG.finished_query();

        // Cache the table name for the metric
        let table_name = Arc::new(table_name);
        self.metrics.insert(metric_name.into(), table_name.clone());

        // If we created a metric, finalize the metric creation in separate task
        if finalize_metric {
            let db = self.db.clone();
            async_std::task::spawn(async move { schema::finalize_metric_creation(&db).await });
        }

        Ok(table_name)
    }

    /// Ensure series ids exist for multiple sets of label pairs, and cache the ids
    async fn upsert_series_batch(
        &self,
        mut series_data: Vec<(&str, SeriesKey, Vec<(&str, &str)>)>,
    ) -> Result<Vec<i32>> {
        let sql_func = format!(
            "{}.get_or_create_series_id_for_kv_array",
            schema::SCHEMA_CATALOG
        );
        #[rustfmt::skip]
        let per_row_length: usize =
            sql_func.len()
            + " UNION ALL SELECT series_id, ___ FROM".len()
            + "($___,$___,$___)".len()
            + 2 // enclosing paranthesis
            ;

        // Upsert chunks of max 1000 series at a time.
        //
        // We must choose some value that is less than 1/3rd of int16 max (~32k),
        // because a postgres query can't bind more than that many variables.
        let mut results = Vec::with_capacity(series_data.len());
        let take_chunk = |vec: &mut Vec<_>| -> Option<Vec<_>> {
            if vec.is_empty() {
                None
            } else if vec.len() > 1000 {
                Some(vec.split_off(vec.len() - 1000))
            } else {
                Some(std::mem::take(vec))
            }
        };
        while let Some(chunk) = take_chunk(&mut series_data) {
            // Format the query string
            let mut nth: u32 = 1;
            let mut stmt = String::with_capacity(chunk.len() * per_row_length);
            for order in 0..chunk.len() {
                if nth > 1 {
                    stmt.write_str(" UNION ALL ").ok();
                }

                #[rustfmt::skip]
                write!(&mut stmt, "(SELECT series_id, {} FROM {}(${},${},${}))", order, sql_func, nth, nth + 1, nth + 2).ok();
                nth += 3;
            }

            // Bind all arguments
            let mut stmt = sqlx::query_as(&stmt);
            for (metric, _, label_pairs) in &chunk {
                let labels = label_pairs.iter().map(|(a, _)| *a).collect::<Vec<_>>();
                let values = label_pairs.iter().map(|(_, b)| *b).collect::<Vec<_>>();
                stmt = stmt.bind(metric).bind(labels).bind(values);
            }

            // Acquire a connection (retry once after timeout)
            let mut conn = match self.db.acquire().await {
                Err(sqlx::Error::PoolTimedOut(..)) => self.db.acquire().await,
                Err(err) => Err(err),
                Ok(conn) => Ok(conn),
            }
            .context("error upserting series")?;

            // Perform the upsert
            let rows: Vec<(i64,)> = stmt
                .fetch_all(&mut conn)
                .await
                .context("error upserting series")?;
            DEBUG.finished_query();

            // Ensure the connection is released
            std::mem::drop(conn);

            // If the number of results is incorrect, return an error
            if rows.len() != chunk.len() {
                return Err(anyhow::format_err!(
                    "error upserting series; results != expected ({} != {})",
                    rows.len(),
                    chunk.len()
                ));
            }

            // Save the ids to the cache
            for ((series_id,), (_, series_key, _)) in rows.into_iter().zip(chunk) {
                // While the `prom_data_series.*` tables use an int8,
                // the `prom_data` table uses an int4.
                let series_id = series_id
                    .try_into()
                    .context("invalid series id (too large)")?;

                // Cache the id for the series
                let size_bytes = series_key.size_of();
                if !self.series.insert(series_key, series_id) {
                    DEBUG.series_added(size_bytes);
                }

                // Add it to the results
                results.push(series_id);
            }
        }

        Ok(results)
    }
}

/// Inserts a set of samples into the specified data table
async fn insert_samples(
    db: &sqlx::postgres::PgPool,
    table_name: &str,
    rows: Vec<SampleRow>,
) -> Result<()> {
    // Collect rows by column
    let count = rows.len();
    let mut times = Vec::with_capacity(count);
    let mut values = Vec::with_capacity(count);
    let mut series_ids = Vec::with_capacity(count);
    for (time, value, series_id) in rows {
        times.push(time);
        values.push(value);
        series_ids.push(series_id);
    }

    // Wait until a database connection is available
    let mut conn = match db.acquire().await {
        Err(sqlx::Error::PoolTimedOut(..)) => db.acquire().await,
        Err(err) => Err(err),
        Ok(conn) => Ok(conn),
    }
    .context("error inserting samples")?;
    sqlx::query(&format!(
        r#"
            INSERT INTO {schema_name}.{table_name} ("time", "value", "series_id")
            SELECT * FROM UNNEST('{timestamp_arr_str}'::timestamptz[], $1::float8[], $2::int4[])
        "#,
        table_name = table_name,
        schema_name = schema::SCHEMA_DATA,
        // TEMPORARY: attempt to insert timestampts using text format
        timestamp_arr_str = format!(
            "{{ {} }}",
            times
                .iter()
                .map(DateTime::to_rfc3339)
                .collect::<Vec<_>>()
                .join(",")
        ),
    ))
    .bind(&values)
    .bind(&series_ids)
    .execute(&mut conn)
    .await
    .context("error inserting samples")?;
    DEBUG.finished_query();

    Ok(())
}