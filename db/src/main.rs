use anyhow::{Context, Result};
use refinery::config::{Config, ConfigDbType};
use url::Url;

// Include database migrations in the compiled binary
refinery::embed_migrations!("migrations");

/// The program's main entry point.
fn main() -> Result<()> {
    // Parse database connection params
    let db_url = dotenv::var("DATABASE_URL").context("missing DATABASE_URL")?;
    let url = Url::parse(&db_url).context("invalid DATABASE_URL")?;
    let mut db = Config::new(ConfigDbType::Postgres);
    if !url.username().is_empty() {
        db = db.set_db_user(url.username());
    }
    if let Some(password) = url.password() {
        db = db.set_db_user(password);
    }
    if let Some(host) = url.host_str() {
        db = db.set_db_host(host);
    }
    if let Some(port) = url.port() {
        db = db.set_db_port(&port.to_string());
    }
    let mut db_name = url.path().trim_start_matches('/').to_string();
    if let Some(query) = url.query() {
        db_name.push('?');
        db_name.push_str(query);
    }
    if !db_name.is_empty() {
        db = db.set_db_name(&db_name);
    }

    // Run migrations
    println!("Checking database migrations...");
    let report = migrations::runner().run(&mut db)?;
    let migrations = report.applied_migrations();
    if migrations.is_empty() {
        println!("No unapplied database migrations.");
    } else {
        println!("Applied migrations:");
    }
    for migration in migrations {
        println!(" - Migration: {}", migration);
    }

    Ok(())
}
