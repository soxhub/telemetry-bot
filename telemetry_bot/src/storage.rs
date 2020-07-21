use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use telemetry_core::parser::SampleSet;

use crate::config::Config;

/// Configures the storage backend from environment variables.
pub async fn from_config(config: &Config) -> Result<Box<dyn Storage>> {
    match config.storage_type.as_str() {
        "standalone" | "compat" => {
            #[cfg(feature = "storage-timescale-standalone")]
            {
                let storage = StandaloneStorage::from_config(config).await?;
                Ok(Box::new(storage))
            }

            #[cfg(not(feature = "storage-timescale-standalone"))]
            Err(anyhow::format_err!("can't use storage type 'standalone' or 'compat': must compiled wit 'feature = storage-timescale-standalone'"))
        }
        "remote" => {
            #[cfg(feature = "storage-remote-write")]
            {
                let storage = RemoteWriteStorage::from_config(config)?;
                Ok(Box::new(storage))
            }

            #[cfg(not(feature = "storage-remote-write"))]
            Err(anyhow::format_err!("can't use storage type 'remote': must compiled wit 'feature = storage-remote-write"))
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
        sampleset: SampleSet<'_>,
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
        sampleset: SampleSet<'_>,
        static_labels: &[(String, String)],
    ) -> (usize, Vec<Error>) {
        let result = telemetry_remote_write::write_samples(
            &self.write_url,
            default_timestamp,
            sampleset.samples,
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
    connector: &'static telemetry_standalone::Connector,
}

#[cfg(feature = "storage-timescale-standalone")]
impl StandaloneStorage {
    pub async fn from_config(config: &Config) -> Result<StandaloneStorage> {
        // Open a sqlx connection pool
        let mut db_url = config
            .database_url
            .clone()
            .ok_or_else(|| anyhow::format_err!("missing DATABASE_URL"))?;
        if !db_url.contains("application_name") {
            if db_url.contains('?') {
                db_url = format!("{}&application_name=telemetry-bot", db_url);
            } else {
                db_url = format!("{}?application_name=telemetry-bot", db_url);
            }
        }
        let db_min_conn = num_cpus::get().max(1);
        let db_max_conn = config.database_conn_per_cpu as usize * db_min_conn;

        println!("Connecting to database...");
        let db = sqlx::postgres::PgPool::builder()
            .min_size(db_min_conn as u32)
            .max_size(db_max_conn as u32)
            .build(&db_url)
            .await
            .context("error connecting to timescaledb")?;
        println!("Connected to {}", db_url);

        // Build storage
        let connector = telemetry_standalone::Connector::new(db);
        connector
            .resume()
            .await
            .context("error resuming connector")?;
        let connector: &'static _ = Box::leak(Box::new(connector));
        Ok(StandaloneStorage { connector })
    }
}

#[async_trait]
#[cfg(feature = "storage-timescale-standalone")]
impl Storage for StandaloneStorage {
    async fn write(
        &self,
        default_timestamp: i64,
        sampleset: SampleSet<'_>,
        static_labels: &[(String, String)],
    ) -> (usize, Vec<Error>) {
        self.connector
            .write_samples(default_timestamp, sampleset, static_labels)
            .await
    }
}
