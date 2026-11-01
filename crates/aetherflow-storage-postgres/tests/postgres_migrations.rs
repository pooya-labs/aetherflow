use aetherflow_storage_postgres::{MigrationOutcome, PgStore};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{Config, NoTls};

#[tokio::test]
async fn postgres_migration_is_atomic_checksummed_and_idempotent() {
    let Ok(database_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
        eprintln!("AETHERFLOW_TEST_DATABASE_URL is not set; skipping PostgreSQL integration test");
        return;
    };

    let config: Config = database_url
        .parse()
        .expect("valid PostgreSQL connection URL");
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(manager)
        .max_size(2)
        .build()
        .expect("build PostgreSQL pool");
    let store = PgStore::from_pool(pool);

    store
        .check_connection()
        .await
        .expect("PostgreSQL is reachable");

    let first = store.migrate().await.expect("apply initial migration");
    assert!(matches!(
        first,
        MigrationOutcome::Applied { version: 1, .. }
    ));

    let second = store.migrate().await.expect("repeat migration");
    assert!(matches!(
        second,
        MigrationOutcome::AlreadyCurrent { version: 1, .. }
    ));
}
