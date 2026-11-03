//! Loopback-only HTTP process for the AetherFlow v1 control plane.
//!
//! The process deliberately refuses non-loopback binds until HTTPS and the
//! external-network deployment boundary are configured. DATABASE_URL is the
//! restricted runtime connection; migrations use a separate owner connection.

mod api;

use std::error::Error;
use std::net::SocketAddr;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::Config;
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;

use aetherflow_storage_postgres::PgStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address = std::env::var("AETHERFLOW_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_owned())
        .parse::<SocketAddr>()?;
    if !address.ip().is_loopback() {
        return Err(format!(
            "refusing non-loopback bind {address}: HTTPS and the external-network deployment boundary are not configured"
        )
        .into());
    }

    let state = match std::env::var("DATABASE_URL") {
        Ok(runtime_database_url) => {
            let migration_database_url =
                std::env::var("AETHERFLOW_MIGRATION_DATABASE_URL").map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "AETHERFLOW_MIGRATION_DATABASE_URL is required when DATABASE_URL is configured",
                    )
                })?;
            let api_token = std::env::var("AETHERFLOW_API_TOKEN").map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "AETHERFLOW_API_TOKEN is required when DATABASE_URL is configured",
                )
            })?;

            // DDL runs only through the separately provisioned migration-owner
            // connection. The server never reuses this pool for API requests.
            let migration_pool = postgres_pool(&migration_database_url)?;
            let migration = PgStore::from_pool(migration_pool.clone()).migrate().await?;
            eprintln!("PostgreSQL schema ready: {migration:?}");
            drop(migration_pool);

            // The API pool must be a non-owner, non-superuser, non-BYPASSRLS
            // role. Refuse startup if even one tenant table is not FORCE RLS.
            let runtime_pool = postgres_pool(&runtime_database_url)?;
            verify_runtime_database_role(&runtime_pool).await?;
            api::AppState::from_pool(runtime_pool, &api_token)?
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

/// Ensures the API is connected through a role that cannot bypass tenant RLS.
async fn verify_runtime_database_role(pool: &Pool) -> Result<(), Box<dyn Error>> {
    const RLS_TABLES: [&str; 10] = [
        "workflow_runs",
        "task_runs",
        "task_run_dependencies",
        "task_attempts",
        "outbox_events",
        "consumer_inbox",
        "tenants",
        "tenant_principals",
        "tenant_credentials",
        "worker_tenant_grants",
    ];

    let client = pool.get().await?;
    let row = client
        .query_one(
            "SELECT
                role.rolsuper,
                role.rolbypassrls,
                role.rolcreaterole,
                role.rolcreatedb,
                role.rolreplication,
                has_schema_privilege(role.oid, 'public', 'CREATE'),
                EXISTS (
                    SELECT 1
                    FROM pg_class AS relation
                    JOIN pg_namespace AS namespace
                      ON namespace.oid = relation.relnamespace
                    WHERE namespace.nspname = 'public'
                      AND relation.relname = ANY($1)
                      AND relation.relkind IN ('r', 'p')
                      AND (
                          relation.relowner = role.oid
                          OR pg_has_role(role.oid, relation.relowner, 'MEMBER')
                      )
                ) AS owns_or_can_assume_table_owner,
                EXISTS (
                    SELECT 1
                    FROM pg_roles AS elevated_role
                    WHERE elevated_role.oid <> role.oid
                      AND (elevated_role.rolsuper OR elevated_role.rolbypassrls)
                      AND pg_has_role(role.oid, elevated_role.oid, 'MEMBER')
                ) AS can_assume_rls_bypass_role,
                (
                    SELECT count(*)::BIGINT
                    FROM pg_class AS relation
                    JOIN pg_namespace AS namespace
                      ON namespace.oid = relation.relnamespace
                    WHERE namespace.nspname = 'public'
                      AND relation.relname = ANY($1)
                      AND relation.relkind IN ('r', 'p')
                      AND relation.relrowsecurity
                      AND relation.relforcerowsecurity
                ) AS forced_rls_table_count
             FROM pg_roles AS role
             WHERE role.rolname = current_user",
            &[&RLS_TABLES[..]],
        )
        .await?;
    let is_superuser: bool = row.get(0);
    let bypasses_rls: bool = row.get(1);
    let can_create_roles: bool = row.get(2);
    let can_create_databases: bool = row.get(3);
    let can_replicate: bool = row.get(4);
    let can_create_public_objects: bool = row.get(5);
    let owns_or_can_assume_table_owner: bool = row.get(6);
    let can_assume_rls_bypass_role: bool = row.get(7);
    let forced_rls_table_count: i64 = row.get(8);

    if is_superuser
        || bypasses_rls
        || can_create_roles
        || can_create_databases
        || can_replicate
        || can_create_public_objects
        || owns_or_can_assume_table_owner
        || can_assume_rls_bypass_role
        || forced_rls_table_count != RLS_TABLES.len() as i64
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "DATABASE_URL must use a restricted runtime role: no superuser, BYPASSRLS, REPLICATION, role/database creation, public-schema CREATE, protected-table ownership, or membership in an owner/bypass role; all ten tenant tables must have ENABLE and FORCE ROW LEVEL SECURITY",
        )
        .into());
    }

    Ok(())
}

fn postgres_pool(database_url: &str) -> Result<Pool, Box<dyn Error>> {
    if rustls::crypto::CryptoProvider::get_default().is_none()
        && rustls::crypto::ring::default_provider()
            .install_default()
            .is_err()
    {
        return Err(
            std::io::Error::other("unable to install the rustls ring crypto provider").into(),
        );
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
    use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
    use tokio_postgres::config::SslMode;
    use tokio_postgres::{Config, NoTls};

    use super::{parse_database_ssl_mode, verify_runtime_database_role};

    #[tokio::test]
    async fn runtime_role_verifier_rejects_owner_and_accepts_restricted_login() {
        let Ok(admin_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
            eprintln!("AETHERFLOW_TEST_DATABASE_URL is not set; skipping runtime-role verifier test");
            return;
        };
        let Ok(runtime_url) = std::env::var("AETHERFLOW_RUNTIME_ROLE_TEST_DATABASE_URL") else {
            eprintln!("AETHERFLOW_RUNTIME_ROLE_TEST_DATABASE_URL is not set; skipping runtime-role verifier test");
            return;
        };

        async fn pool_from_test_url(database_url: &str) -> Pool {
            let config: Config = database_url.parse().expect("valid PostgreSQL test URL");
            let manager = Manager::from_config(
                config,
                NoTls,
                ManagerConfig {
                    recycling_method: RecyclingMethod::Fast,
                },
            );
            Pool::builder(manager)
                .max_size(1)
                .build()
                .expect("build runtime-role test pool")
        }

        // The migration/test owner must never be accepted as the application's
        // runtime identity, even though it can administer the schema.
        let owner_pool = pool_from_test_url(&admin_url).await;
        assert!(
            verify_runtime_database_role(&owner_pool).await.is_err(),
            "the database owner/superuser must be rejected as runtime"
        );

        // CI provisions a distinct login with only DML and sequence privileges.
        let restricted_pool = pool_from_test_url(&runtime_url).await;
        verify_runtime_database_role(&restricted_pool)
            .await
            .expect("the non-owner, non-BYPASSRLS runtime login is accepted");
    }

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
