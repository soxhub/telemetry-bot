//! This module defines database migrations and exposes a migration runner.

// Include database migrations in the compiled binary
refinery::embed_migrations!("db/migrations");

// Re-export the migrations
pub use migrations::runner;
