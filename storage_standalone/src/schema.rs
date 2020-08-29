#![rustfmt::skip]

use anyhow::{Context, Error, Result};
use crossbeam_queue::ArrayQueue;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use r2d2_postgres::PostgresConnectionManager;
use semver::Version;
use std::sync::Arc;
use std::time::Duration;

use telemetry_core::error::debug_error;

type PgManager = PostgresConnectionManager<MakeTlsConnector>;
type PgPool = r2d2::Pool<PgManager>;
type PgConnection = r2d2::PooledConnection<PgManager>;

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
const PROM_SCHEMA_VERSION: &str = "0.1.0-beta.3";

// Pre-defined queries that can be built statically at compile time
pub const UPSERT_METRIC_TABLE_NAME: &str =
    concat!("SELECT table_name, possibly_new FROM ", catalog_schema!(), ".get_or_create_metric_table_name($1)");
pub const UPSERT_SERIES_ID_FOR_LABELS: &str =
    concat!("SELECT * FROM ", catalog_schema!(), ".get_or_create_series_id_for_kv_array($1, $2, $3)");
const CALL_FINALIZE_METRIC_CREATION: &str =
    concat!("CALL ", catalog_schema!(), ".finalize_metric_creation()");

// Helpers to manage timescale extensions (copied from timescale-prometheus)
const INSTALL_TIMESCALE_EXTENSION: &str =
    "CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;";
const INSTALL_PROMETHEUS_EXTENSION: &str =
    concat!("CREATE EXTENSION IF NOT EXISTS timescale_prometheus_extra WITH SCHEMA ", ext_schema!(), ";");
const UPDATE_METADATA_WITH_EXTENSION: &str =
    "SELECT update_tsprom_metadata($1, $2, $3);";
const UPDATE_METADATA_NO_EXTENSION: &str =
    "INSERT INTO _timescaledb_catalog.metadata(key, value, include_in_telemetry) VALUES ($1, $2, $3) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, include_in_telemetry = EXCLUDED.include_in_telemetry;";

// Helpers to manage prom_schema_migrations (copied from timescale-prometheus)
const CREATE_MIGRATIONS_TABLE: &str =
    "CREATE TABLE IF NOT EXISTS prom_schema_migrations (version TEXT NOT NULL PRIMARY KEY)";
const TRUNCATE_MIGRATIONS_TABLE: &str =
    "TRUNCATE prom_schema_migrations";
const SELECT_MIGRATION_VERSION: &str =
    "SELECT version FROM prom_schema_migrations LIMIT 1";
const INSERT_MIGRATION_VERSION: &str =
    "INSERT INTO prom_schema_migrations (version) VALUES ($1)";
macro_rules! define_migration {
    ($version:literal, $filename:literal) => {
        (
            $version.parse::<Version>().expect("invalid migration filename"),
            build_migration(include_str!(concat!("../sql/versions/dev/", $version, "/", $filename))),
        )
    }
}
const fn build_migration(source: &str) -> String {
    source
        .replace("SCHEMA_CATALOG", SCHEMA_CATALOG)
        .replace("SCHEMA_EXT", SCHEMA_EXT)
        .replace("SCHEMA_PROM", SCHEMA_PROM)
        .replace("SCHEMA_SERIES", SCHEMA_SERIES)
        .replace("SCHEMA_METRIC", SCHEMA_METRIC)
        .replace("SCHEMA_DATA", SCHEMA_DATA)
        .replace("SCHEMA_DATA_SERIES", SCHEMA_DATA_SERIES)
        .replace("SCHEMA_INFO", SCHEMA_INFO)
}
fn update_extension_metadata(conn: &mut PgConnection, with_ext: bool, key: &str, value: &str) {
    if with_ext {
        conn.execute(UPDATE_METADATA_WITH_EXTENSION, &[&key, &value, &true]).ok();
    } else {
        conn.execute(UPDATE_METADATA_NO_EXTENSION, &[&key, &value, &true]).ok();
    }
}

/// A schema manager to run migrations for and manage metric tables in the prometheus schema.
#[derive(Clone)]
pub struct SchemaManager {
    pool: PgPool,
    finalize_queue: Arc<ArrayQueue<()>>,
}

impl SchemaManager {
    // Creates a new schema manager that connects to the database and
    // starts background workers to manage schema finalize + cleanup tasks.
    pub fn connect(url: &str) -> Result<Self> {
        // Create a connection pool for schema migrations.
        //
        // The connection pool is intended to only use one connection at
        // a time but includes pooling behavior so that the connection can
        // be easily reconnected on connection failure and cycled periodically
        // to improve resiliency for the "long-lived" connection.
        //
        // We use a separate pool, because we don't want to impact the separate
        // concurrency concerns of tasks accessing the other pool(s).
        let config = url.parse().context("error parsing postgres url")?;
        let connector = TlsConnector::new()?;
        let connector = MakeTlsConnector::new(connector);
        let manager = PostgresConnectionManager::new(config, connector);
        let pool = r2d2::Pool::builder()
            .idle_timeout(Some(Duration::from_secs(15)))
            .min_idle(Some(1))
            .max_size(2)
            .build(manager)?;

        // Create a queue with space for exactly 2 items.
        //
        // This is intend to implement the "keep latest" concurrency job pattern,
        // where only the "active" job and "most recently queued" jobs are kept,
        // and any intermediate jobs are dropped as duplicates.
        let finalize_queue = Arc::new(ArrayQueue::new(2));

        Ok(SchemaManager { pool, finalize_queue })
    }

    /// Run the prometheus schema migrations
    pub fn migrate(&self) -> Result<()> {
        let mut conn = self.pool.get()?;

        // Ensure the migrations table exists and load the current database version
        conn.execute(CREATE_MIGRATIONS_TABLE, &[])
            .context("error creating migrations table")?;
        let db_version_str: String = conn.query_opt(SELECT_MIGRATION_VERSION, &[])
            .context("error getting migration version")?
            .map(|row| row.get(0))
            .unwrap_or_else(|| "0.0.0".to_string());
        let db_version: Version = db_version_str.parse()
            .context("error parsing migration version")?;

        // If the db's current version is less than the minimum required, run migrations
        let min_schema_version = PROM_SCHEMA_VERSION.parse()
            .expect("builtin PROM_SCHEMA_VERSION is invalid");
        if db_version < min_schema_version {
            let tx = conn.transaction()?;

            // If the database version was none, run the pre-installation scripts.
            if db_version_str == "0.0.0" {
                let preinstall = &[
                    build_migration(include_str!("../sql/preinstall/001-users.sql")),
                    build_migration(include_str!("../sql/preinstall/002-schemas.sql")),
                    build_migration(include_str!("../sql/preinstall/003-tables.sql")),
                    build_migration(include_str!("../sql/preinstall/004-matcher_operators.sql")),
                ];
            }
            // Otherwise, upgrade to the current version
            else {
                let migrations = &[
                    define_migration!("0.1.0-beta.2.dev", "1-drop_procedure_named_drop_chunks.sql"),
                ];
                for (version, migration) in migrations {
                    if version < &db_version {
                        tx.batch_execute(&migration)?;
                    }
                }
            }

            // If any migrations were required, run the idempotent scripts.
            let idempotent = &[
                build_migration(include_str!("../sql/idempotent/base.sql")),
                build_migration(include_str!("../sql/idempotent/matcher-functions.sql")),
            ];
            for migration in idempotent {
                tx.batch_execute(&migration)?;
            }

            // Set the new database version
            tx.execute(TRUNCATE_MIGRATIONS_TABLE, &[])?;
            tx.execute(INSERT_MIGRATION_VERSION, &[&PROM_SCHEMA_VERSION])?;

            tx.commit()?;
        }
        if db_version > min_schema_version {
            println!("Info: timescale prometheus schema version is above the minimum required version ({} > {})", db_version, min_schema_version);
        }

        // Always install prometheus extension and update connector metadata
        if let Err(err) = conn.execute(INSTALL_PROMETHEUS_EXTENSION, &[]) {
            debug_error(Error::new(err).context("timescale_prometheus_extra extension not installed"));
            update_extension_metadata(&mut conn, false, "version", PROM_SCHEMA_VERSION);
            update_extension_metadata(&mut conn, false, "commit_hash", "");
        } else {
            update_extension_metadata(&mut conn, true, "version", PROM_SCHEMA_VERSION);
            update_extension_metadata(&mut conn, true, "commit_hash", "");
        }

        Ok(())
    }

    /// Notify the finalize metric task to eventually finalize any
    /// newly created metrics asynchronously in a background thread.
    pub fn schedule_finalize_metric(&self) {
        self.finalize_queue.push(()).ok();
    }
}

/// A task intended to be run in a background thread to finalize metric creation
fn finalize_metric_creation_task(pool: PgPool, queue: Arc<ArrayQueue<()>>) {
    loop {
        std::thread::sleep(Duration::from_secs(1));
        if queue.pop().ok().is_none() {
            continue; // empty queue; lets go back to sleep
        }

        // run finalize once
        if let Err(err) = finalize_metric_creation(&pool) {
            // TODO: RECORD METRIC FINALIZE FAILURE COUNT
            debug_error(err.context("error finalizing metric"));

            // if we failed, refill our queue
            queue.push(()).ok();
            continue;
        }
    }
}

fn finalize_metric_creation(db: &PgPool) -> Result<()> {
    let conn = db.get()?;
    conn.execute(CALL_FINALIZE_METRIC_CREATION, &[])?;
    Ok(())
}
