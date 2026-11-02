use aetherflow_storage_postgres::{
    MigrationOutcome, PgStore, StorageError, initial_migration_checksum,
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{Config, NoTls};

#[tokio::test]
async fn postgres_migration_is_atomic_locked_checksummed_and_fail_closed() {
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
        .max_size(4)
        .build()
        .expect("build PostgreSQL pool");
    let store = PgStore::from_pool(pool.clone());

    store
        .check_connection()
        .await
        .expect("PostgreSQL is reachable");

    // Simultaneous first-start calls must serialize: one applies, the other
    // observes the committed ledger and returns AlreadyCurrent.
    let (first_result, second_result) = tokio::join!(store.migrate(), store.migrate());
    let outcomes = [
        first_result.expect("first migration call succeeds"),
        second_result.expect("second migration call succeeds"),
    ];
    let applied = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, MigrationOutcome::Applied { version: 3, .. }))
        .count();
    let already_current = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, MigrationOutcome::AlreadyCurrent { version: 3, .. }))
        .count();
    assert_eq!(applied, 1);
    assert_eq!(already_current, 1);

    let expected_checksum = initial_migration_checksum();
    let client = pool.get().await.expect("get verification connection");

    // Checksum drift must fail closed.
    let invalid_checksum = if expected_checksum.starts_with('0') {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 1",
            &[&invalid_checksum],
        )
        .await
        .expect("corrupt migration checksum for negative test");
    assert!(matches!(
        store.migrate().await,
        Err(StorageError::MigrationChecksumMismatch { version: 1, .. })
    ));
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 1",
            &[&expected_checksum],
        )
        .await
        .expect("restore migration checksum");

    // The additive inbox migration has its own immutable checksum as well.
    let inbox_checksum: String = client
        .query_one(
            "SELECT checksum_sha256 FROM aetherflow_schema_migrations WHERE version = 2",
            &[],
        )
        .await
        .expect("read consumer inbox migration checksum")
        .get(0);
    let invalid_inbox_checksum = if inbox_checksum.starts_with('0') {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 2",
            &[&invalid_inbox_checksum],
        )
        .await
        .expect("corrupt inbox migration checksum for negative test");
    assert!(matches!(
        store.migrate().await,
        Err(StorageError::MigrationChecksumMismatch { version: 2, .. })
    ));
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 2",
            &[&inbox_checksum],
        )
        .await
        .expect("restore inbox migration checksum");

    // The node membership migration is independently checksummed.
    let membership_checksum: String = client
        .query_one(
            "SELECT checksum_sha256 FROM aetherflow_schema_migrations WHERE version = 3",
            &[],
        )
        .await
        .expect("read node membership migration checksum")
        .get(0);
    let invalid_membership_checksum = if membership_checksum.starts_with('0') {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 3",
            &[&invalid_membership_checksum],
        )
        .await
        .expect("corrupt node membership migration checksum for negative test");
    assert!(matches!(
        store.migrate().await,
        Err(StorageError::MigrationChecksumMismatch { version: 3, .. })
    ));
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 3",
            &[&membership_checksum],
        )
        .await
        .expect("restore node membership migration checksum");

    // Existing schema objects without a tracked migration row are not adopted.
    client
        .execute(
            "DELETE FROM aetherflow_schema_migrations WHERE version = 1",
            &[],
        )
        .await
        .expect("remove migration ledger row for negative test");
    assert!(matches!(
        store.migrate().await,
        Err(StorageError::UntrackedSchema)
    ));
    client
        .execute(
            "INSERT INTO aetherflow_schema_migrations \
             (version, migration_name, checksum_sha256) VALUES (1, $1, $2)",
            &[&"0001_initial_schema.sql", &expected_checksum],
        )
        .await
        .expect("restore migration ledger row");

    // A database schema newer than the binary is never downgraded or ignored.
    let future_version = 4_i64;
    let future_name = "0004_future_schema.sql";
    let future_checksum = "a".repeat(64);
    client
        .execute(
            "INSERT INTO aetherflow_schema_migrations \
             (version, migration_name, checksum_sha256) VALUES ($1, $2, $3)",
            &[&future_version, &future_name, &future_checksum],
        )
        .await
        .expect("insert future migration marker");
    assert!(matches!(
        store.migrate().await,
        Err(StorageError::NewerSchema {
            found: 4,
            supported: 3
        })
    ));
    client
        .execute(
            "DELETE FROM aetherflow_schema_migrations WHERE version = 4",
            &[],
        )
        .await
        .expect("remove future migration marker");
}
