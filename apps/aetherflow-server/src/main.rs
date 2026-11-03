//! Loopback-only HTTP process for the AetherFlow v1 control plane.
//!
//! The process deliberately refuses non-loopback binds until authentication
//! and tenant isolation exist. DATABASE_URL enables durable routes; without it,
//! only the health endpoint is available.

mod api;

use std::error::Error;
use std::net::SocketAddr;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{Config, NoTls};

use aetherflow_storage_postgres::PgStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address = std::env::var("AETHERFLOW_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_owned())
        .parse::<SocketAddr>()?;
    if !address.ip().is_loopback() {
        return Err(format!(
            "refusing non-loopback bind {address}: authentication and tenant isolation are not implemented"
        )
        .into());
    }

    let state = match std::env::var("DATABASE_URL") {
        Ok(database_url) => {
            let pool = postgres_pool(&database_url)?;
            let store = PgStore::from_pool(pool.clone());
            let migration = store.migrate().await?;
            eprintln!("PostgreSQL schema ready: {migration:?}");
            api::AppState::from_pool(pool)
        }
        Err(_) => {
            eprintln!(
                "DATABASE_URL is not set; starting health-only mode, durable API routes return 503"
            );
            api::AppState::without_database()
        }
    };

    let listener = tokio::net::TcpListener::bind(address).await?;
    println!(
        "AetherFlow {} control plane listening on {}",
        aetherflow_core::VERSION,
        listener.local_addr()?
    );
    axum::serve(listener, api::router(state)).await?;
    Ok(())
}

fn postgres_pool(database_url: &str) -> Result<Pool, Box<dyn Error>> {
    let config: Config = database_url.parse()?;
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Ok(Pool::builder(manager).max_size(16).build()?)
}
