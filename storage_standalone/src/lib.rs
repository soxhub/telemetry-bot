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

#[rustfmt::skip]
#[allow(dead_code)]
mod schema {
    use anyhow::{Error, Result};
    use sqlx::prelude::*;
    use sqlx::Executor;

    use telemetry_core::error::debug_error;

    // Define constants as macros so that these schema names can be used with `concat!`
    macro_rules! prom_schema { () => ("prom_api") }
    macro_rules! series_view_schema { () => ("prom_series") }
    macro_rules! metric_view_schema { () => ("prom_metric") }
    macro_rules! data_schema { () => ("prom_data") }
    macro_rules! data_series_schema { () => ("prom_data_series") }
    macro_rules! info_schema { () => ("prom_info") }
    macro_rules! catalog_schema { () => ("_prom_catalog") }
    macro_rules! ext_schema { () => ("_prom_ext") }

    // Declare const values as part of the module public api
    pub const SCHEMA_PROM: &str = prom_schema!();
    pub const SCHEMA_SERIES: &str = series_view_schema!();
    pub const SCHEMA_METRIC: &str = metric_view_schema!();
    pub const SCHEMA_DATA: &str = data_schema!();
    pub const SCHEMA_DATA_SERIES: &str = data_series_schema!();
    pub const SCHEMA_INFO: &str = info_schema!();
    pub const SCHEMA_CATALOG: &str = catalog_schema!();
    pub const SCHEMA_EXT: &str = ext_schema!();
    const BASE_MIGRATIONS_TABLE: &str = "prom_schema_migrations";
    const HISTOGRAM_MIGRATIONS_TABLE: &str = "bot_schema_migrations";

    // Pre-define queries that can be build statically at compile time
    pub const UPSERT_METRIC_TABLE_NAME: &str =
        concat!("SELECT table_name, possibly_new FROM ", catalog_schema!(), ".get_or_create_metric_table_name($1)");
    pub const UPSERT_SERIES_ID_FOR_LABELS: &str =
        concat!("SELECT * FROM ", catalog_schema!(), ".get_or_create_series_id_for_kv_array($1, $2, $3)");
    const CALL_FINALIZE_METRIC_CREATION: &str =
        concat!("CALL ", catalog_schema!(), ".finalize_metric_creation()");
    const INSTALL_TIMESCALE_EXTENSION: &str =
        "CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;";
    const INSTALL_PROMETHEUS_EXTENSION: &str =
        concat!("CREATE EXTENSION IF NOT EXISTS timescale_prometheus_extra WITH SCHEMA ", ext_schema!(), ";");
    const UPDATE_METADATA_WITH_EXTENSION: &str =
        "SELECT update_tsprom_metadata($1, $2, $3);";
    const UPDATE_METADATA_NO_EXTENSION: &str =
        "INSERT INTO _timescaledb_catalog.metadata(key, value, include_in_telemetry) VALUES ($1, $2, $3) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, include_in_telemetry = EXCLUDED.include_in_telemetry;";

    /// A task intended to be run in the background to finalize metric creation
    pub async fn finalize_metric_creation(db: &sqlx::postgres::PgPool) {
        if let Err(err) = sqlx::query(CALL_FINALIZE_METRIC_CREATION).execute(db).await {
            debug_error(Error::new(err).context("error finalizing metric"));
        }
    }

    pub async fn migrate(db: &sqlx::postgres::PgPool) -> Result<()> {
        let mut conn = db.acquire().await?;

        // Install timescaledb extension
        conn.execute(INSTALL_TIMESCALE_EXTENSION).await?;

        // Run base schema migrations
        if must_run_migrations(&mut conn, BASE_MIGRATIONS_TABLE, 1).await? {
            let base_schema = include_str!("../sql/base_schema.sql")
                .replace("SCHEMA_CATALOG", SCHEMA_CATALOG)
                .replace("SCHEMA_EXT", SCHEMA_EXT)
                .replace("SCHEMA_PROM", SCHEMA_PROM)
                .replace("SCHEMA_SERIES", SCHEMA_SERIES)
                .replace("SCHEMA_METRIC", SCHEMA_METRIC)
                .replace("SCHEMA_DATA", SCHEMA_DATA)
                .replace("SCHEMA_DATA_SERIES", SCHEMA_DATA_SERIES)
                .replace("SCHEMA_INFO", SCHEMA_INFO);
            run_migration(&mut conn, BASE_MIGRATIONS_TABLE, 1, &base_schema).await?;
        }

        // Install prometheus extension and update connector metadata
        const BASE_SCHEMA_VERSION: &str = "0.1.0-alpha.4.1"; // from timescale_prometheus
        if let Err(err) = conn.execute(INSTALL_PROMETHEUS_EXTENSION).await {
            debug_error(Error::new(err).context("timescale_prometheus_extra extension not installed"));
            update_metadata(&mut conn, false, "version", BASE_SCHEMA_VERSION).await;
            update_metadata(&mut conn, false, "commit_hash", "").await;
        } else {
            update_metadata(&mut conn, true, "version", BASE_SCHEMA_VERSION).await;
            update_metadata(&mut conn, true, "commit_hash", "").await;
        }

        Ok(())
    }

    async fn update_metadata(conn: &mut sqlx::postgres::PgConnection, with_ext: bool, key: &str, value: &str) {
        // Ignore error if it doesn't work
        if with_ext {
            sqlx::query(UPDATE_METADATA_WITH_EXTENSION)
        } else {
            sqlx::query(UPDATE_METADATA_NO_EXTENSION)
        }
        .bind(key)
        .bind(value)
        .bind(true)
        .execute(conn)
        .await
        .ok();
    }

    async fn must_run_migrations(conn: &mut sqlx::postgres::PgConnection, table: &str, min_version: i64) -> Result<bool> {
        let create_migration_table = format!("CREATE TABLE IF NOT EXISTS {} (version int8 PRIMARY KEY, dirty bool NOT NULL);", table);
        let select_migration_status = format!("SELECT MAX(version), BOOL_OR(dirty) FROM {};", table);

        // Ensure the migrations table exists
        conn.execute(create_migration_table.as_str()).await?;

        // Check if we need to run migrations
        let (latest_version, dirty): (Option<i64>, Option<bool>) =
            sqlx::query_as(select_migration_status.as_str()).fetch_one(conn).await?;
        if latest_version.unwrap_or(0) < min_version {
            // Quit if a previous migration failed
            if dirty.unwrap_or(false) {
                return Err(anyhow::format_err!("will not run '{}' because database is dirty", table));
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn run_migration(conn: &mut sqlx::postgres::PgConnection, table: &str, version: i64, content: &str) -> Result<()> {
        // Start the migration
        let start_migration = format!("INSERT INTO {} (version, dirty) VALUES ({}, true);", table, version);
        conn.execute(start_migration.as_str()).await?;

        // Run the migration
        conn.execute(content).await?;

        // Finish the migration
        let finish_migration = format!("UPDATE {} SET dirty = false WHERE version = {};", table, version);
        conn.execute(finish_migration.as_str()).await?;

        Ok(())
    }
}

mod encoding {
    #![allow(dead_code)]

    pub struct U32VarintEncoder {
        output: Vec<u8>,
    }

    impl U32VarintEncoder {
        pub fn sized_for(num_ints: usize) -> Self {
            Self {
                // Ballpark the memory we'll need; to avoid excess re-allocations
                output: Vec::with_capacity(num_ints * 2),
            }
        }

        pub fn encode(&mut self, value: u32) {
            let mut buffer = [0; 5];
            let encoded = unsigned_varint::encode::u32(value, &mut buffer);
            self.output.extend_from_slice(encoded);
        }

        pub fn finish(self) -> Vec<u8> {
            self.output
        }
    }

    pub struct U32VarintDecoder<'a> {
        input: &'a [u8],
    }

    impl<'a> U32VarintDecoder<'a> {
        pub fn new(input: &'a [u8]) -> Self {
            Self { input }
        }

        pub fn decode_all(mut self) -> Option<Vec<u32>> {
            let min_results = 1 + (self.input.len() / 5); // results will contain _at least_ one integer per 5 bytes
            let mut results = Vec::with_capacity(min_results);
            while !self.input.is_empty() {
                results.push(self.decode()?);
            }
            Some(results)
        }

        #[inline]
        pub fn decode(&mut self) -> Option<u32> {
            let (head, tail) = unsigned_varint::decode::u32(self.input).ok()?;
            self.input = tail;
            Some(head)
        }
    }

    pub struct U32ZigZagEncoder {
        output: Vec<u8>,
        prev: u32,
    }

    impl U32ZigZagEncoder {
        pub fn sized_for(num_ints: usize) -> Self {
            Self {
                // Assumes upto 5-bytes for the first integer and assuming 1 byte for all subsequent integers.
                //
                // While this won't always be exact, it should be within a single power of 2 of the result size
                // which will avoid many unnecessary re-allocations without overzealously over-allocatiing.
                output: Vec::with_capacity(num_ints + 5),
                prev: 0,
            }
        }

        pub fn encode(&mut self, value: u32) {
            let mut buffer = [0; 5];
            let signed = value as i32 - self.prev as i32;
            let zigzag = (signed << 1) ^ (signed >> 31);
            let encoded = unsigned_varint::encode::u32(zigzag as u32, &mut buffer);
            self.output.extend_from_slice(encoded);
            self.prev = value;
        }

        pub fn finish(self) -> Vec<u8> {
            self.output
        }
    }

    pub struct U32ZigZagDecoder<'a> {
        input: &'a [u8],
        prev: u32,
    }

    impl<'a> U32ZigZagDecoder<'a> {
        pub fn new(input: &'a [u8]) -> Self {
            Self { input, prev: 0 }
        }

        pub fn decode_all(mut self) -> Option<Vec<u32>> {
            let min_results = 1 + (self.input.len() / 10); // results will contain _at least_ one integer per 10 bytes
            let mut results = Vec::with_capacity(min_results);
            while !self.input.is_empty() {
                results.push(self.decode()?);
            }
            Some(results)
        }

        #[inline]
        pub fn decode(&mut self) -> Option<u32> {
            let (head, tail) = unsigned_varint::decode::u32(self.input).ok()?;
            let delta = (head as u32 >> 1) as i32 ^ -((head & 1) as i32);
            let value = (self.prev as i32 + delta) as u32;
            self.input = tail;
            self.prev = value;
            Some(value)
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn u32_varint_encoder_works() {
            let inputs: [&[u32]; 9] = [
                &[0].as_ref(),
                &[0, 0, 0, 0, 0, 0, 0, 0, 0].as_ref(),
                &[1].as_ref(),
                &[1, 1, 1, 1, 1, 1, 1, 1, 1].as_ref(),
                &[5].as_ref(),
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10].as_ref(),
                &[u32::MIN].as_ref(),
                &[u32::MAX].as_ref(),
                &[u32::MAX, u32::MAX, u32::MAX].as_ref(),
            ];
            for values in &inputs {
                let mut encoder = U32VarintEncoder::sized_for(inputs.len());
                for n in *values {
                    encoder.encode(*n);
                }

                // Compress the inputs
                let encoded = encoder.finish();

                // NOTE: Encoding of MAX-range values will cause compressed values to be large than expected
                // if values.len() > 1 {
                //     assert!(encoded.len() < std::mem::size_of::<u32>() * values.len());
                // }

                // Ensure that the decoded value matches the input
                let decoded = U32VarintDecoder::new(&encoded).decode_all().unwrap();
                assert_eq!(&decoded, values);
            }
        }

        #[test]
        fn u32_zigzag_encoder_works() {
            let inputs: [&[u32]; 9] = [
                &[0].as_ref(),
                &[0, 0, 0, 0, 0, 0, 0, 0, 0].as_ref(),
                &[1].as_ref(),
                &[1, 1, 1, 1, 1, 1, 1, 1, 1].as_ref(),
                &[5].as_ref(),
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10].as_ref(),
                &[u32::MIN].as_ref(),
                &[u32::MAX].as_ref(),
                &[u32::MAX, u32::MAX, u32::MAX].as_ref(),
            ];
            for values in &inputs {
                let mut encoder = U32ZigZagEncoder::sized_for(inputs.len());
                for n in *values {
                    encoder.encode(*n);
                }

                // Compress the inputs
                let encoded = encoder.finish();
                if values.len() > 1 {
                    assert!(encoded.len() < std::mem::size_of::<u32>() * values.len());
                }

                // Ensure that the decoded value matches the input
                let decoded = U32ZigZagDecoder::new(&encoded).decode_all().unwrap();
                assert_eq!(&decoded, values);
            }
        }
    }
}
