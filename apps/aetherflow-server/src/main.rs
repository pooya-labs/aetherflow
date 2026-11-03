//! Loopback-only HTTP process for the AetherFlow v1 control plane.
//!
//! The process deliberately refuses non-loopback binds until authentication
//! and tenant isolation exist. DATABASE_URL enables durable routes; without it,
//! only the health endpoint is available.

mod api;

use std::error::Error;
use std::net::SocketAddr;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::config::SslMode;
use tokio_postgres::Config;
use tokio_postgres_rustls::MakeRustlsConnect;

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
            let api_token = std::env::var("AETHERFLOW_API_TOKEN").map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "AETHERFLOW_API_TOKEN is required when DATABASE_URL is configured",
                )
            })?;
            let pool = postgres_pool(&database_url)?;
            let store = PgStore::from_pool(pool.clone());
            let migration = store.migrate().await?;
            eprintln!("PostgreSQL schema ready: {migration:?}");
            api::AppState::from_pool(pool, &api_token)?
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
    if rustls::crypto::CryptoProvider::get_default().is_none()
        && rustls::crypto::ring::default_provider()
            .install_default()
            .is_err()
    {
        return Err(std::io::Error::other("unable to install the rustls ring crypto provider").into());
    }

    let ssl_mode = database_ssl_mode()?;
    let tls = if ssl_mode == SslMode::Disable {
        MakeRustlsConnect::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        )
    } else {
        let (tls, certificate_warnings) =
            MakeRustlsConnect::with_native_certs().map_err(|errors| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "unable to load a trusted PostgreSQL CA certificate store ({} errors)",
                        errors.len()
                    ),
                )
            })?;
        if !certificate_warnings.is_empty() {
            eprintln!(
                "PostgreSQL native certificate loader reported {} warning(s); valid certificates were retained",
                certificate_warnings.len()
            );
        }
        tls
    };

    let mut config: Config = database_url.parse()?;
    config.ssl_mode(ssl_mode);
    let manager = Manager::from_config(
        config,
        tls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Ok(Pool::builder(manager).max_size(16).build()?)
}

fn database_ssl_mode() -> Result<SslMode, std::io::Error> {
    let configured = std::env::var("DATABASE_SSL_MODE").ok();
    parse_database_ssl_mode(configured.as_deref())
}

fn parse_database_ssl_mode(value: Option<&str>) -> Result<SslMode, std::io::Error> {
    match value.unwrap_or("require") {
        "disable" => Ok(SslMode::Disable),
        "prefer" => Ok(SslMode::Prefer),
        "require" => Ok(SslMode::Require),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "DATABASE_SSL_MODE must be disable, prefer or require",
        )),
    }
}

#[cfg(test)]
mod tests {
    use tokio_postgres::config::SslMode;

    use super::parse_database_ssl_mode;

    #[test]
    fn database_tls_defaults_to_required_and_accepts_only_supported_modes() {
        assert_eq!(
            parse_database_ssl_mode(None).expect("default TLS mode"),
            SslMode::Require
        );
        assert_eq!(
            parse_database_ssl_mode(Some("disable")).expect("disabled mode"),
            SslMode::Disable
        );
        assert_eq!(
            parse_database_ssl_mode(Some("prefer")).expect("preferred mode"),
            SslMode::Prefer
        );
        assert_eq!(
            parse_database_ssl_mode(Some("require")).expect("required mode"),
            SslMode::Require
        );
        assert!(parse_database_ssl_mode(Some("verify-full")).is_err());
        assert!(parse_database_ssl_mode(Some("unknown")).is_err());
    }
}
