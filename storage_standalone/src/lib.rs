use anyhow::{Context, Error, Result};
use async_std::sync::{Receiver, Sender};
use chrono::prelude::*;
use dashmap::DashMap;
use futures::prelude::*;
use lasso::{Spur, ThreadedRodeo};
use sqlx::prelude::*;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use telemetry_core::debug::DEBUG;
use telemetry_core::error::debug_error;
use telemetry_core::parser::Sample;

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct LabelNameKey(Spur);

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct LabelValueKey(Spur);

#[repr(transparent)]
#[derive(Eq, PartialEq, Hash)]
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

    /// A map of metric table names to insert tasks (via a channel)
    inserters: DashMap<Arc<String>, Sender<Vec<SampleRow>>>,

    /// An interned map of label names to integer ids
    label_names: ThreadedRodeo<Spur>,

    /// An interned map of label values to integer ids
    label_values: ThreadedRodeo<Spur>,
}

impl Connector {
    pub fn new(db: sqlx::postgres::PgPool) -> Self {
        Self {
            db,
            inserters: DashMap::new(),
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

    fn build_series_key(&self, label_pairs: &Vec<(String, String)>) -> SeriesKey {
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
        scraped_samples: Vec<Sample>,
        static_labels: Vec<(String, String)>,
    ) -> (usize, Vec<Error>) {
        let mut errors = Vec::new();

        // Collect samples as rows per table
        let mut rows: HashMap<Arc<String>, Vec<SampleRow>> =
            HashMap::with_capacity(scraped_samples.len());
        for sample in scraped_samples {
            // Get the normal metric table name
            let metric_table = if let Some(table_name) = self.metrics.get(&sample.name) {
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
            label_pairs.push(("__name__".to_owned(), sample.name.clone()));
            label_pairs.extend(static_labels.clone());
            label_pairs.extend(sample.labels);

            // Labels must be sorted by name alphabetically
            label_pairs.sort_by(|a, b| a.0.cmp(&b.0));

            // Create a comparable + hashable key for the series
            let series_key = self.build_series_key(&label_pairs);

            // Find the series id and save the data row to be inserted
            let timestamp = sample.timestamp.unwrap_or(default_timestamp);
            let timestamp = DateTime::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc);
            if let Some(series_id) = self.series.get(&series_key) {
                let row = (timestamp, sample.value, *series_id);
                rows.entry(metric_table).or_default().push(row);
            } else {
                match self
                    .upsert_series(&sample.name, series_key, &label_pairs)
                    .await
                {
                    Ok(series_id) => {
                        let row = (timestamp, sample.value, series_id);
                        rows.entry(metric_table).or_default().push(row);
                    }
                    Err(err) => {
                        errors.push(err);
                        continue;
                    }
                }
            }
        }

        // Insert the data rows
        let mut inserted = 0;
        for (table_name, rows) in rows {
            inserted += rows.len();
            let insert_key = Arc::clone(&table_name);
            let insert_task = self.inserters.entry(insert_key).or_insert_with(move || {
                let (tx, rx) = async_std::sync::channel(1024);
                async_std::task::spawn(process_samples(self.db.clone(), table_name, rx));
                tx
            });
            insert_task.send(rows).await;
        }

        (inserted, errors)
    }

    /// Ensure tables for the metric with the given name exist, and cache the table name
    async fn upsert_metric(&self, metric_name: &str) -> Result<Arc<String>> {
        let (table_name, finalize_metric): (String, bool) =
            sqlx::query_as(schema::UPSERT_METRIC_TABLE_NAME)
                .bind(metric_name)
                .fetch_one(&self.db)
                .await?;

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

    /// Ensure a series id for the set of label pairs exists, and cache the id
    async fn upsert_series(
        &self,
        metric: &str,
        series_key: SeriesKey,
        label_pairs: &[(String, String)],
    ) -> Result<i32> {
        // Prepare labels and values
        let labels = label_pairs
            .iter()
            .map(|(a, _)| a.as_str())
            .collect::<Vec<_>>();
        let values = label_pairs
            .iter()
            .map(|(_, b)| b.as_str())
            .collect::<Vec<_>>();

        // Acquire a connection (retry once after timeout)
        let mut conn = match self.db.acquire().await {
            Err(sqlx::Error::PoolTimedOut(..)) => self.db.acquire().await,
            Err(err) => Err(err),
            Ok(conn) => Ok(conn),
        }
        .context("error upserting series")?;

        // Perform the upsert
        let (_table_name, series_id): (String, i64) =
            sqlx::query_as(schema::UPSERT_SERIES_ID_FOR_LABELS)
                .bind(metric)
                .bind(&labels)
                .bind(&values)
                .fetch_one(&mut conn)
                .await
                .context("error upserting series")?;

        // Ensure the connection is released
        std::mem::drop(conn);

        // While the `prom_data_series.*` tables use an int8,
        // the `prom_data` table uses an int4.
        let series_id = series_id
            .try_into()
            .context("invalid series id (too large)")?;

        // Cache the id for the series
        let size_bytes = series_key.size_of();
        if self.series.insert(series_key, series_id).is_none() {
            DEBUG.series_added(size_bytes);
        }

        Ok(series_id)
    }
}

const MAX_BATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// A spawnable async task that recv rows from a channel and inserts them (batched) into the database.
async fn process_samples(
    db: sqlx::postgres::PgPool,
    table_name: Arc<String>,
    rx: Receiver<Vec<SampleRow>>,
) {
    let mut batch = Vec::new();
    let mut next_rows = rx.recv().boxed().fuse();

    'receive: loop {
        // Wait up to 1 second to receive inserts
        let mut did_timeout = false;
        let mut timeout = async_std::task::sleep(MAX_BATCH_TIMEOUT).boxed().fuse();
        futures::select! {
            result = next_rows => match result {
                // When rows are received, add them to the batch
                Ok(rows) => {
                    if batch.is_empty() {
                        batch = rows;
                    } else {
                        batch.extend(rows);
                    }

                    // Reset our recv promise
                    next_rows = rx.recv().boxed().fuse();
                }

                // When the channel closes, stop processing samples
                Err(_) => break 'receive,
            },

            () = timeout => did_timeout = true,
        }

        // If we've waited for more than a second, or collected at least a 1000 rows
        // then insert whatever rows we currently have collected.
        if !batch.is_empty() && (did_timeout || batch.len() >= 1000) {
            let inserts = std::mem::take(&mut batch);
            if let Err(err) = insert_samples(&db, &table_name, inserts).await {
                debug_error(err.context("error inserting samples"));
            }
        }
    }

    // If there are any uninserted rows after the channel closes, insert them now
    if !batch.is_empty() {
        if let Err(err) = insert_samples(&db, &table_name, batch).await {
            debug_error(err.context("error inserting samples"));
        }
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
    let mut conn = wait_for_connection(db).await?;
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
    .await?;

    Ok(())
}

async fn wait_for_connection(
    db: &sqlx::postgres::PgPool,
) -> sqlx::Result<sqlx::pool::PoolConnection<sqlx::postgres::PgConnection>> {
    const MAX_RETRIES: usize = 10;
    let mut attempts = 1;
    loop {
        match db.acquire().await {
            Err(sqlx::Error::PoolTimedOut(..)) if attempts < MAX_RETRIES => {
                attempts += 1;
                continue;
            }
            Err(err) => return Err(err),
            Ok(conn) => return Ok(conn),
        }
    }
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
