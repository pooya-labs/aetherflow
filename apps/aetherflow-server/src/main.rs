//! Loopback-default HTTP/HTTPS process for the AetherFlow v1 control plane.
//!
//! Plain HTTP is permitted only on loopback. A non-loopback bind requires a
//! complete, parseable Rustls certificate/key pair. DATABASE_URL is the
//! restricted runtime connection; migrations use a separate owner connection.

mod api;

use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;

use axum_server::tls_rustls::RustlsConfig;
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
    // Validate the external-listener security boundary before applying DDL.
    // Non-loopback addresses are never served over cleartext HTTP.
    let tls_config = load_http_tls_config(address).await?;

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

    let router = api::router(state);
    if let Some(tls_config) = tls_config {
        println!(
            "AetherFlow {} HTTPS control plane listening on {}",
            aetherflow_core::VERSION,
            address
        );
        axum_server::bind_rustls(address, tls_config)
            .serve(router.into_make_service())
            .await?;
    } else {
        let listener = tokio::net::TcpListener::bind(address).await?;
        println!(
            "AetherFlow {} loopback HTTP control plane listening on {}",
            aetherflow_core::VERSION,
            listener.local_addr()?
        );
        axum::serve(listener, router).await?;
    }
    Ok(())
}

/// Loads a complete server certificate/key pair and enforces TLS for external binds.
async fn load_http_tls_config(address: SocketAddr) -> Result<Option<RustlsConfig>, Box<dyn Error>> {
    let certificate_path = std::env::var_os("AETHERFLOW_TLS_CERT_PATH").map(PathBuf::from);
    let private_key_path = std::env::var_os("AETHERFLOW_TLS_KEY_PATH").map(PathBuf::from);
    let use_tls = validate_http_transport(
        address,
        certificate_path.is_some(),
        private_key_path.is_some(),
    )?;

    if !use_tls {
        return Ok(None);
    }

    ensure_rustls_crypto_provider()?;
    let certificate_path = certificate_path.expect("TLS mode requires a certificate path");
    let private_key_path = private_key_path.expect("TLS mode requires a private-key path");
    let config = RustlsConfig::from_pem_file(&certificate_path, &private_key_path)
        .await
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unable to load the HTTPS certificate/key pair: {error}"),
            )
        })?;
    Ok(Some(config))
}

/// Returns true when TLS is required; rejects incomplete pairs and remote HTTP.
fn validate_http_transport(
    address: SocketAddr,
    certificate_configured: bool,
    private_key_configured: bool,
) -> Result<bool, std::io::Error> {
    match (certificate_configured, private_key_configured) {
        (true, true) => Ok(true),
        (true, false) | (false, true) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "AETHERFLOW_TLS_CERT_PATH and AETHERFLOW_TLS_KEY_PATH must be configured together",
        )),
        (false, false) if address.ip().is_loopback() => Ok(false),
        (false, false) => Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "refusing non-loopback bind {address}: configure a trusted TLS certificate chain and private key"
            ),
        )),
    }
}

/// Installs the explicitly selected Rustls crypto provider before constructing
/// PostgreSQL or HTTPS TLS configurations.
fn ensure_rustls_crypto_provider() -> Result<(), std::io::Error> {
    if rustls::crypto::CryptoProvider::get_default().is_none()
        && rustls::crypto::ring::default_provider()
            .install_default()
            .is_err()
    {
        return Err(std::io::Error::other(
            "unable to install the rustls ring crypto provider",
        ));
    }
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
                ) AS forced_rls_table_count,
                EXISTS (
                    SELECT 1
                    FROM pg_auth_members AS membership
                    WHERE membership.member = role.oid
                ) AS has_role_memberships
             FROM pg_roles AS role
             WHERE role.rolname = current_user",
            &[&&RLS_TABLES[..]],
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
    let has_role_memberships: bool = row.get(9);

    if is_superuser
        || bypasses_rls
        || can_create_roles
        || can_create_databases
        || can_replicate
        || can_create_public_objects
        || owns_or_can_assume_table_owner
        || can_assume_rls_bypass_role
        || has_role_memberships
        || forced_rls_table_count != RLS_TABLES.len() as i64
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "DATABASE_URL must use a restricted runtime role: no superuser, BYPASSRLS, REPLICATION, role/database creation, public-schema CREATE, protected-table ownership, or membership in any other role; all ten tenant tables must have ENABLE and FORCE ROW LEVEL SECURITY",
        )
        .into());
    }

    Ok(())
}

fn postgres_pool(database_url: &str) -> Result<Pool, Box<dyn Error>> {
    ensure_rustls_crypto_provider()?;

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
    use std::sync::Arc;
    use std::time::Duration;

    use axum::{Router, routing::get};
    use axum_server::tls_rustls::RustlsConfig;
    use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
    use rcgen::generate_simple_self_signed;
    use rustls::pki_types::ServerName;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_postgres::config::SslMode;
    use tokio_postgres::{Config, NoTls};
    use tokio_rustls::TlsConnector;

    use super::{
        ensure_rustls_crypto_provider, parse_database_ssl_mode, validate_http_transport,
        verify_runtime_database_role,
    };

    #[tokio::test]
    async fn runtime_role_verifier_rejects_owner_and_accepts_restricted_login() {
        let Ok(admin_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
            eprintln!(
                "AETHERFLOW_TEST_DATABASE_URL is not set; skipping runtime-role verifier test"
            );
            return;
        };
        let Ok(runtime_url) = std::env::var("AETHERFLOW_RUNTIME_ROLE_TEST_DATABASE_URL") else {
            eprintln!(
                "AETHERFLOW_RUNTIME_ROLE_TEST_DATABASE_URL is not set; skipping runtime-role verifier test"
            );
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

        let Ok(member_url) = std::env::var("AETHERFLOW_RUNTIME_ROLE_MEMBER_TEST_DATABASE_URL")
        else {
            eprintln!(
                "AETHERFLOW_RUNTIME_ROLE_MEMBER_TEST_DATABASE_URL is not set; skipping role-membership negative case"
            );
            return;
        };
        let member_pool = pool_from_test_url(&member_url).await;
        assert!(
            verify_runtime_database_role(&member_pool).await.is_err(),
            "a runtime login that can SET ROLE to another role must be rejected"
        );
    }

    #[test]
    fn external_bind_requires_a_complete_tls_pair() {
        let loopback: SocketAddr = "127.0.0.1:8080".parse().expect("valid loopback address");
        let remote: SocketAddr = "0.0.0.0:8443".parse().expect("valid wildcard address");

        assert_eq!(
            validate_http_transport(loopback, false, false).expect("loopback HTTP is allowed"),
            false
        );
        assert_eq!(
            validate_http_transport(loopback, true, true).expect("loopback HTTPS is allowed"),
            true
        );
        assert!(validate_http_transport(loopback, true, false).is_err());
        assert!(validate_http_transport(loopback, false, true).is_err());
        assert!(validate_http_transport(remote, false, false).is_err());
        assert!(validate_http_transport(remote, true, false).is_err());
        assert!(validate_http_transport(remote, false, true).is_err());
        assert_eq!(
            validate_http_transport(remote, true, true).expect("remote HTTPS is allowed"),
            true
        );
    }

    #[tokio::test]
    async fn https_listener_negotiates_tls_and_never_serves_plain_http() {
        ensure_rustls_crypto_provider().expect("install ring crypto provider");
        let certified = generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate ephemeral HTTPS test certificate");
        let expected_certificate = certified.cert.der().clone();

        let mut trusted_roots = rustls::RootCertStore::empty();
        trusted_roots
            .add(expected_certificate.clone())
            .expect("trust test certificate");
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(trusted_roots)
            .with_no_client_auth();

        let server_config = RustlsConfig::from_pem(
            certified.cert.pem().into_bytes(),
            certified.signing_key.serialize_pem().into_bytes(),
        )
        .await
        .expect("parse generated server certificate and key");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral TLS test port");
        let address = listener.local_addr().expect("read ephemeral TLS address");
        let server = axum_server::tls_rustls::from_tcp_rustls(
            listener
                .into_std()
                .expect("convert listener for axum-server"),
            server_config,
        )
        .expect("construct Rustls HTTPS server");
        let app = Router::new().route("/", get(|| async { "tls-only" }));
        let server_task = tokio::spawn(server.serve(app.into_make_service()));

        let tcp = TcpStream::connect(address)
            .await
            .expect("connect to HTTPS listener");
        let connector = TlsConnector::from(Arc::new(client_config));
        let server_name =
            ServerName::try_from("localhost".to_owned()).expect("valid TLS server name");
        let tls_stream = connector
            .connect(server_name, tcp)
            .await
            .expect("complete verified TLS handshake");
        assert_eq!(
            tls_stream
                .get_ref()
                .1
                .peer_certificates()
                .expect("peer certificate")[0],
            expected_certificate
        );
        drop(tls_stream);

        // A raw HTTP request sent to the TLS socket must never receive an HTTP
        // response; the TLS acceptor closes or emits a TLS alert instead.
        let mut plaintext = TcpStream::connect(address)
            .await
            .expect("connect a negative plain-HTTP client");
        plaintext
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write raw HTTP bytes");
        let mut response_prefix = [0_u8; 5];
        let read_result =
            tokio::time::timeout(Duration::from_secs(2), plaintext.read(&mut response_prefix))
                .await
                .expect("TLS listener should reject cleartext promptly")
                .expect("read TLS alert or closed connection");
        assert!(
            read_result == 0 || &response_prefix[..read_result] != b"HTTP/",
            "TLS listener must not return a cleartext HTTP response"
        );

        server_task.abort();
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
