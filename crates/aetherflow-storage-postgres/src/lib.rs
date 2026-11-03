//! Asynchronous PostgreSQL connection management and durable schema migrations.
//!
//! The caller configures the connection pool, including TLS and authentication.
//! This crate never holds a database transaction across task-handler execution.

use deadpool_postgres::Pool;
use sha2::{Digest, Sha256};
use std::fmt;

mod coordinator;
mod inbox;
mod membership;
mod outbox;
mod task_repository;
mod worker;

pub use coordinator::{PgWorkflowCoordinator, PgWorkflowSubmission};
pub use inbox::{InboxClaim, InboxClaimResult, PgInboxRepository};
pub use membership::{
    MembershipStatus, NodeMembership, PgMembershipRepository, WorkerRegistration,
};
pub use outbox::{
    OutboxDispatchReport, OutboxDispatcherConfig, OutboxDispatcherConfigError,
    OutboxDispatcherError, OutboxPublishError, OutboxPublishFuture, OutboxPublisher,
    PostgresOutboxDispatcher,
};
pub use task_repository::{
    DurableTaskRun, DurableWorkflowRun, NewTaskRun, OutboxClaim, PgTaskClaim, PgTaskRepository,
    RecoveredLease, TaskFailure,
};
pub use worker::{
    PostgresTaskWorker, PostgresWorkerConfig, PostgresWorkerConfigError, PostgresWorkerError,
};

const INITIAL_MIGRATION_SQL: &str =
    include_str!("../../aetherflow-runtime/migrations/postgres/0001_initial_schema.sql");
const CONSUMER_INBOX_MIGRATION_SQL: &str =
    include_str!("../../aetherflow-runtime/migrations/postgres/0002_consumer_inbox.sql");
const NODE_MEMBERSHIP_MIGRATION_SQL: &str =
    include_str!("../../aetherflow-runtime/migrations/postgres/0003_node_membership.sql");
const MIGRATION_VERSION: i64 = 3;
const MIGRATION_ADVISORY_LOCK: i64 = 0x4145_5448_4552;

#[derive(Clone, Copy)]
struct Migration {
    version: i64,
    name: &'static str,
    sql: &'static str,
}

const MIGRATIONS: [Migration; 3] = [
    Migration {
        version: 1,
        name: "0001_initial_schema.sql",
        sql: INITIAL_MIGRATION_SQL,
    },
    Migration {
        version: 2,
        name: "0002_consumer_inbox.sql",
        sql: CONSUMER_INBOX_MIGRATION_SQL,
    },
    Migration {
        version: 3,
        name: "0003_node_membership.sql",
        sql: NODE_MEMBERSHIP_MIGRATION_SQL,
    },
];

/// Result of applying the PostgreSQL migration set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationOutcome {
    /// The initial schema was applied and its checksum recorded atomically.
    Applied {
        /// Schema version now present in the database.
        version: i64,
        /// SHA-256 checksum of the exact migration source.
        checksum_sha256: String,
    },
    /// The database already has the expected schema and migration checksum.
    AlreadyCurrent {
        /// Schema version present in the database.
        version: i64,
        /// SHA-256 checksum recorded by the database.
        checksum_sha256: String,
    },
}

/// PostgreSQL storage and durable-task failures.
#[derive(Debug, Eq, PartialEq)]
pub enum StorageError {
    /// A connection could not be acquired from the pool.
    Pool(String),
    /// PostgreSQL rejected a query or transaction.
    Database(String),
    /// Migration metadata exists but does not describe the expected schema.
    UntrackedSchema,
    /// The database schema is newer than this binary supports.
    NewerSchema { found: i64, supported: i64 },
    /// An applied migration has a different name than this binary expects.
    MigrationNameMismatch {
        version: i64,
        expected: String,
        actual: String,
    },
    /// An applied migration's checksum differs from the immutable source.
    MigrationChecksumMismatch {
        version: i64,
        expected: String,
        actual: String,
    },
    /// A task run does not exist.
    TaskRunNotFound(String),
    /// A workflow run does not exist.
    WorkflowRunNotFound(String),
    /// A task claim is stale, expired, or belongs to another worker.
    ClaimLost(String),
    /// A consumer name does not satisfy the bounded inbox identifier contract.
    InvalidInboxConsumerName,
    /// An event ID does not satisfy the bounded visible-ASCII contract.
    InvalidInboxEventId,
    /// An inbox diagnostic exceeds the persisted size limit.
    InvalidInboxError,
    /// An inbox claim is stale, expired, or owned by another worker.
    InboxClaimLost {
        consumer_name: String,
        event_id: String,
    },
    /// Another incarnation currently owns an unexpired node membership lease.
    NodeLeaseBusy(String),
    /// The supplied node incarnation or membership epoch is no longer current.
    NodeMembershipLost(String),
    /// A worker registration ID is already held by a different live owner.
    WorkerRegistrationBusy(String),
    /// The worker registration or its parent node epoch is no longer current.
    WorkerRegistrationLost(String),
    /// Worker concurrency capacity cannot fit a positive PostgreSQL INTEGER.
    InvalidWorkerCapacity,
    /// A lease duration is zero or cannot be represented as signed milliseconds.
    InvalidLeaseDuration,
    /// A retry policy cannot be represented by PostgreSQL columns.
    InvalidRetryPolicy,
    /// A workflow or task definition is invalid for durable creation.
    InvalidWorkflow(String),
    /// Data read from PostgreSQL violates a runtime contract.
    InvalidStoredValue(String),
    /// A related row expected inside an atomic operation is missing.
    InconsistentState(String),
    /// The persisted attempt budget does not permit another retry.
    RetryBudgetExhausted(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pool(message) => write!(formatter, "PostgreSQL pool error: {message}"),
            Self::Database(message) => write!(formatter, "PostgreSQL error: {message}"),
            Self::UntrackedSchema => formatter.write_str(
                "PostgreSQL schema metadata exists without the expected migration history",
            ),
            Self::NewerSchema { found, supported } => write!(
                formatter,
                "database schema version {found} is newer than supported version {supported}"
            ),
            Self::MigrationNameMismatch {
                version,
                expected,
                actual,
            } => write!(
                formatter,
                "migration {version} name mismatch: expected {expected}, found {actual}"
            ),
            Self::MigrationChecksumMismatch {
                version,
                expected,
                actual,
            } => write!(
                formatter,
                "migration {version} checksum mismatch: expected {expected}, found {actual}"
            ),
            Self::TaskRunNotFound(id) => write!(formatter, "task run not found: {id}"),
            Self::WorkflowRunNotFound(id) => write!(formatter, "workflow run not found: {id}"),
            Self::ClaimLost(id) => write!(formatter, "task claim is no longer owned: {id}"),
            Self::InvalidInboxConsumerName => formatter
                .write_str("consumer inbox name must be a 1-128 character ASCII identifier"),
            Self::InvalidInboxEventId => formatter
                .write_str("consumer inbox event ID must be 1-255 visible ASCII characters"),
            Self::InvalidInboxError => {
                formatter.write_str("consumer inbox error exceeds 4096 bytes")
            }
            Self::InboxClaimLost {
                consumer_name,
                event_id,
            } => write!(
                formatter,
                "consumer inbox claim is no longer owned: {consumer_name}/{event_id}"
            ),
            Self::NodeLeaseBusy(node_id) => {
                write!(formatter, "node membership lease is still owned: {node_id}")
            }
            Self::NodeMembershipLost(node_id) => {
                write!(formatter, "node membership is no longer current: {node_id}")
            }
            Self::WorkerRegistrationBusy(worker_id) => {
                write!(formatter, "worker registration is still owned: {worker_id}")
            }
            Self::WorkerRegistrationLost(worker_id) => {
                write!(
                    formatter,
                    "worker registration is no longer current: {worker_id}"
                )
            }
            Self::InvalidWorkerCapacity => formatter
                .write_str("worker concurrency capacity must fit a positive PostgreSQL INTEGER"),
            Self::InvalidLeaseDuration => {
                formatter.write_str("lease duration must be a positive signed millisecond value")
            }
            Self::InvalidRetryPolicy => {
                formatter.write_str("retry policy cannot be represented by PostgreSQL columns")
            }
            Self::InvalidWorkflow(message) => write!(formatter, "invalid workflow: {message}"),
            Self::InvalidStoredValue(message) => {
                write!(formatter, "invalid value stored in PostgreSQL: {message}")
            }
            Self::InconsistentState(message) => {
                write!(formatter, "inconsistent PostgreSQL state: {message}")
            }
            Self::RetryBudgetExhausted(id) => {
                write!(formatter, "task retry budget is exhausted: {id}")
            }
        }
    }
}

impl std::error::Error for StorageError {}

/// A PostgreSQL-backed storage boundary.
///
/// The caller supplies a configured pool so TLS, authentication, connection
/// parameters, and pool sizing remain deployment decisions.
#[derive(Clone)]
pub struct PgStore {
    pool: Pool,
}

impl PgStore {
    /// Creates the storage adapter from a caller-configured PostgreSQL pool.
    #[must_use]
    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }

    /// Verifies that a live pooled connection can execute a trivial query.
    pub async fn check_connection(&self) -> Result<(), StorageError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        client
            .simple_query("SELECT 1")
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(())
    }

    /// Applies every pending immutable schema migration and records each
    /// version/name/checksum in the same transaction as its DDL.
    ///
    /// Repeated calls are idempotent. An untracked, newer, or checksum-drifted
    /// schema fails closed. Migration execution is serialized by an advisory lock.
    pub async fn migrate(&self) -> Result<MigrationOutcome, StorageError> {
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock($1)",
                &[&MIGRATION_ADVISORY_LOCK],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        let metadata_exists: bool = transaction
            .query_one(
                "SELECT to_regclass('public.aetherflow_schema_migrations') IS NOT NULL",
                &[],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?
            .get(0);

        let rows = if metadata_exists {
            transaction
                .query(
                    "SELECT version, migration_name, checksum_sha256 FROM aetherflow_schema_migrations ORDER BY version",
                    &[],
                )
                .await
                .map_err(|error| StorageError::Database(error.to_string()))?
        } else {
            Vec::new()
        };

        if metadata_exists && rows.is_empty() {
            return Err(StorageError::UntrackedSchema);
        }

        for (index, row) in rows.iter().enumerate() {
            let version: i64 = row.get(0);
            if version > MIGRATION_VERSION {
                return Err(StorageError::NewerSchema {
                    found: version,
                    supported: MIGRATION_VERSION,
                });
            }

            let expected = MIGRATIONS.get(index).ok_or(StorageError::UntrackedSchema)?;
            if version != expected.version {
                return Err(StorageError::UntrackedSchema);
            }

            let name: String = row.get(1);
            if name != expected.name {
                return Err(StorageError::MigrationNameMismatch {
                    version,
                    expected: expected.name.to_owned(),
                    actual: name,
                });
            }

            let stored_checksum: String = row.get(2);
            let checksum = migration_checksum(expected);
            if stored_checksum.trim_end() != checksum {
                return Err(StorageError::MigrationChecksumMismatch {
                    version,
                    expected: checksum,
                    actual: stored_checksum,
                });
            }
        }

        let applied_count = rows.len();
        for migration in MIGRATIONS.iter().skip(applied_count) {
            let checksum = migration_checksum(migration);
            transaction
                .batch_execute(migration.sql)
                .await
                .map_err(|error| StorageError::Database(error.to_string()))?;
            transaction
                .execute(
                    "INSERT INTO aetherflow_schema_migrations (version, migration_name, checksum_sha256) VALUES ($1, $2, $3)",
                    &[&migration.version, &migration.name, &checksum],
                )
                .await
                .map_err(|error| StorageError::Database(error.to_string()))?;
        }

        let latest = MIGRATIONS
            .last()
            .expect("the migration set must contain an initial schema");
        let latest_checksum = migration_checksum(latest);
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        if applied_count == MIGRATIONS.len() {
            Ok(MigrationOutcome::AlreadyCurrent {
                version: latest.version,
                checksum_sha256: latest_checksum,
            })
        } else {
            Ok(MigrationOutcome::Applied {
                version: latest.version,
                checksum_sha256: latest_checksum,
            })
        }
    }
}

/// Computes the lower-case SHA-256 checksum for one immutable migration.
fn migration_checksum(migration: &Migration) -> String {
    let digest = Sha256::digest(migration.sql.as_bytes());
    format!("{digest:x}")
}

/// Computes the lower-case SHA-256 checksum of the initial schema migration.
#[must_use]
pub fn initial_migration_checksum() -> String {
    migration_checksum(&MIGRATIONS[0])
}

#[cfg(test)]
mod tests {
    use super::{MIGRATION_VERSION, MIGRATIONS, initial_migration_checksum, migration_checksum};

    #[test]
    fn migration_set_has_stable_order_names_and_sha256_checksums() {
        assert_eq!(MIGRATION_VERSION, 3);
        assert_eq!(MIGRATIONS[0].version, 1);
        assert_eq!(MIGRATIONS[0].name, "0001_initial_schema.sql");
        assert_eq!(MIGRATIONS[1].version, 2);
        assert_eq!(MIGRATIONS[1].name, "0002_consumer_inbox.sql");
        assert_eq!(MIGRATIONS[2].version, 3);
        assert_eq!(MIGRATIONS[2].name, "0003_node_membership.sql");

        let initial = initial_migration_checksum();
        let inbox = migration_checksum(&MIGRATIONS[1]);
        let membership = migration_checksum(&MIGRATIONS[2]);
        for checksum in [initial, inbox, membership] {
            assert_eq!(checksum.len(), 64);
            assert!(checksum.bytes().all(|byte| byte.is_ascii_hexdigit()));
            assert!(checksum.bytes().all(|byte| !byte.is_ascii_uppercase()));
        }
    }
}
