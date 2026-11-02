//! Asynchronous PostgreSQL connection management and durable schema migrations.
//!
//! The caller configures the connection pool, including TLS and authentication.
//! This crate never holds a database transaction across task-handler execution.

use deadpool_postgres::Pool;
use sha2::{Digest, Sha256};
use std::fmt;

mod coordinator;
mod task_repository;
mod worker;

pub use task_repository::{
    DurableTaskRun, NewTaskRun, OutboxClaim, PgTaskClaim, PgTaskRepository, RecoveredLease,
    TaskFailure,
};
pub use coordinator::{PgWorkflowCoordinator, PgWorkflowSubmission};
pub use worker::{
    PostgresTaskWorker, PostgresWorkerConfig, PostgresWorkerConfigError, PostgresWorkerError,
};

const INITIAL_MIGRATION_SQL: &str =
    include_str!("../../aetherflow-runtime/migrations/postgres/0001_initial_schema.sql");
const MIGRATION_VERSION: i64 = 1;
const MIGRATION_NAME: &str = "0001_initial_schema.sql";
const MIGRATION_ADVISORY_LOCK: i64 = 0x4145_5448_4552;

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

    /// Applies the initial schema once, recording version and checksum in the
    /// same transaction as the DDL. Repeated calls are idempotent.
    ///
    /// An untracked, newer, or checksum-drifted schema fails closed. Migration
    /// execution is serialized by a transaction-scoped advisory lock.
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

        let checksum = initial_migration_checksum();

        if metadata_exists {
            let rows = transaction
                .query(
                    "SELECT version, migration_name, checksum_sha256 \
                     FROM aetherflow_schema_migrations ORDER BY version",
                    &[],
                )
                .await
                .map_err(|error| StorageError::Database(error.to_string()))?;

            for row in &rows {
                let version: i64 = row.get(0);
                if version > MIGRATION_VERSION {
                    return Err(StorageError::NewerSchema {
                        found: version,
                        supported: MIGRATION_VERSION,
                    });
                }
            }

            if rows.len() != 1 {
                return Err(StorageError::UntrackedSchema);
            }

            let row = &rows[0];
            let version: i64 = row.get(0);
            let name: String = row.get(1);
            let stored_checksum: String = row.get(2);

            if version != MIGRATION_VERSION {
                return Err(StorageError::UntrackedSchema);
            }
            if name != MIGRATION_NAME {
                return Err(StorageError::MigrationNameMismatch {
                    version,
                    expected: MIGRATION_NAME.to_owned(),
                    actual: name,
                });
            }
            if stored_checksum.trim_end() != checksum {
                return Err(StorageError::MigrationChecksumMismatch {
                    version,
                    expected: checksum,
                    actual: stored_checksum,
                });
            }

            transaction
                .commit()
                .await
                .map_err(|error| StorageError::Database(error.to_string()))?;
            return Ok(MigrationOutcome::AlreadyCurrent {
                version,
                checksum_sha256: initial_migration_checksum(),
            });
        }

        transaction
            .batch_execute(INITIAL_MIGRATION_SQL)
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        transaction
            .execute(
                "INSERT INTO aetherflow_schema_migrations \
                 (version, migration_name, checksum_sha256) VALUES ($1, $2, $3)",
                &[&MIGRATION_VERSION, &MIGRATION_NAME, &checksum],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        Ok(MigrationOutcome::Applied {
            version: MIGRATION_VERSION,
            checksum_sha256: checksum,
        })
    }
}

/// Computes the lower-case SHA-256 checksum over the exact migration bytes.
#[must_use]
pub fn initial_migration_checksum() -> String {
    let digest = Sha256::digest(INITIAL_MIGRATION_SQL.as_bytes());
    format!("{digest:x}")
}

#[cfg(test)]
mod tests {
    use super::{MIGRATION_NAME, MIGRATION_VERSION, initial_migration_checksum};

    #[test]
    fn initial_migration_has_stable_name_and_sha256_checksum_shape() {
        assert_eq!(MIGRATION_VERSION, 1);
        assert_eq!(MIGRATION_NAME, "0001_initial_schema.sql");

        let checksum = initial_migration_checksum();
        assert_eq!(checksum.len(), 64);
        assert!(checksum.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(checksum.bytes().all(|byte| !byte.is_ascii_uppercase()));
    }
}
