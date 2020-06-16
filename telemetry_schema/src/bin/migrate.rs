use anyhow::{Context, Result};
use sqlx::prelude::*;

#[rustfmt::skip]
const SQL_MIGRATIONS: &[(i32, &str, &str)] = &[
    (1, "V1__catalog.sql", include_str!("../../db/migrations/V1__catalog.sql")),
];

/// The program's main entry point.
fn main() -> Result<()> {
    async_std::task::block_on(run_migrations())
}

async fn run_migrations() -> Result<()> {
    // Connect to the database
    let db_url = dotenv::var("DATABASE_URL").context("missing DATABASE_URL")?;
    let mut conn = sqlx::postgres::PgConnection::connect(&db_url)
        .await
        .context("failed to connect to database")?;

    // Ensure the migrations table exists
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS telemetry_schema_migrations (version int PRIMARY KEY);",
    )
    .execute(&mut conn)
    .await
    .context("failed to create migrations table")?;

    // Get the most recently run migration
    let (curr_version,) =
        sqlx::query_as::<_, (Option<i32>,)>("SELECT MAX(version) FROM telemetry_schema_migrations")
            .fetch_one(&mut conn)
            .await
            .context("failed to detect migration version")?;
    let curr_version = curr_version.unwrap_or(0);

    // Run the migrations
    println!("Starting migrations...");
    for (version, filename, raw_sql) in SQL_MIGRATIONS {
        if *version <= curr_version {
            continue;
        }

        println!(" - Migration: {}", filename.trim_end_matches(".sql"));
        for stmt in raw_sql.split(";") {
            sqlx::query(stmt)
                .execute(&mut conn)
                .await
                .context("error during migration")?;
        }
        sqlx::query("INSERT INTO telemetry_schema_migrations (version) VALUES ($1)")
            .bind(version)
            .execute(&mut conn)
            .await
            .context("failed to insert into migrations table")?;
    }
    println!("Finished migrations.");
    Ok(())
}
