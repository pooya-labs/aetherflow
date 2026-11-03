//! Authenticated tenant and authorization context types.
//!
//! A TenantContext can only be minted inside this crate after a credential
//! has been validated against active credential, principal, and tenant rows.

use aetherflow_core::domain::{PrincipalId, TenantId, WorkerId};
use tokio_postgres::Transaction;

use crate::StorageError;

/// A role assigned to one tenant principal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TenantRole {
    /// May read, submit, cancel, and administer tenant principals/credentials.
    Admin,
    /// May read, submit, and cancel tenant workflows and tasks.
    Operator,
    /// May inspect tenant resources without mutating them.
    Reader,
}

impl TenantRole {
    pub(crate) fn from_database(value: &str) -> Option<Self> {
        match value {
            "tenant_admin" => Some(Self::Admin),
            "tenant_operator" => Some(Self::Operator),
            "tenant_reader" => Some(Self::Reader),
            _ => None,
        }
    }

    /// Returns the stable database role name.
    #[must_use]
    pub const fn as_database_value(self) -> &'static str {
        match self {
            Self::Admin => "tenant_admin",
            Self::Operator => "tenant_operator",
            Self::Reader => "tenant_reader",
        }
    }

    /// Returns whether this role grants the requested tenant permission.
    #[must_use]
    pub const fn allows(self, permission: TenantPermission) -> bool {
        match (self, permission) {
            (Self::Admin, _) => true,
            (Self::Operator, TenantPermission::Read)
            | (Self::Operator, TenantPermission::Submit)
            | (Self::Operator, TenantPermission::Cancel) => true,
            (Self::Reader, TenantPermission::Read) => true,
            _ => false,
        }
    }
}

/// A tenant-scoped action used by route and repository authorization checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TenantPermission {
    /// Read workflow and task state belonging to the authenticated tenant.
    Read,
    /// Submit a workflow owned by the authenticated tenant.
    Submit,
    /// Cancel a task owned by the authenticated tenant.
    Cancel,
    /// Create, revoke, or inspect tenant principals and credentials.
    ManageCredentials,
}

/// Opaque proof that a tenant credential was accepted by PostgreSQL.
///
/// Fields are private and the only constructor is crate-private. External
/// callers must obtain this value through PgTenantCredentialRepository;
/// request payloads, headers, and caller-supplied tenant IDs cannot create it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TenantContext {
    tenant_id: TenantId,
    principal_id: PrincipalId,
    role: TenantRole,
}

/// Installs a worker identity as transaction-local PostgreSQL state.
///
/// The active grant still has to be checked and locked by the operation. This
/// GUC lets future forced-RLS policies restrict set-based worker queries to
/// tenants granted to this registered worker.
pub(crate) async fn apply_worker_identity_to_transaction(
    transaction: &Transaction<'_>,
    worker_id: &WorkerId,
) -> Result<(), StorageError> {
    transaction
        .query_one(
            "SELECT
                set_config('aetherflow.tenant_id', '', true),
                set_config('aetherflow.worker_id', $1, true),
                set_config('aetherflow.principal_id', '', true),
                set_config('aetherflow.tenant_role', 'worker', true)",
            &[&worker_id.as_str()],
        )
        .await
        .map_err(|error| StorageError::Database(error.to_string()))?;
    Ok(())
}

/// Installs the reserved legacy tenant for compatibility worker operations.
pub(crate) async fn apply_legacy_worker_context_to_transaction(
    transaction: &Transaction<'_>,
) -> Result<(), StorageError> {
    transaction
        .query_one(
            "SELECT
                set_config('aetherflow.tenant_id', 'legacy-default', true),
                set_config('aetherflow.worker_id', '', true),
                set_config('aetherflow.principal_id', '', true),
                set_config('aetherflow.tenant_role', 'legacy_worker', true)",
            &[],
        )
        .await
        .map_err(|error| StorageError::Database(error.to_string()))?;
    Ok(())
}

/// Changes the active tenant for the remainder of one database transaction.
///
/// Only call this after validating the relevant tenant credential or worker
/// grant. The setting is transaction-local and cannot leak through the pool.
pub(crate) async fn apply_tenant_id_to_transaction(
    transaction: &Transaction<'_>,
    tenant_id: &str,
) -> Result<(), StorageError> {
    transaction
        .query_one(
            "SELECT set_config('aetherflow.tenant_id', $1, true)",
            &[&tenant_id],
        )
        .await
        .map_err(|error| StorageError::Database(error.to_string()))?;
    Ok(())
}

impl TenantContext {
    pub(crate) fn from_authenticated_credential(
        tenant_id: TenantId,
        principal_id: PrincipalId,
        role: TenantRole,
    ) -> Self {
        Self {
            tenant_id,
            principal_id,
            role,
        }
    }

    /// Returns the tenant bound to the validated credential.
    #[must_use]
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the principal that owns the validated credential.
    #[must_use]
    pub fn principal_id(&self) -> &PrincipalId {
        &self.principal_id
    }

    /// Returns the role read from the validated credential record.
    #[must_use]
    pub const fn role(&self) -> TenantRole {
        self.role
    }

    /// Checks one explicit tenant permission.
    #[must_use]
    pub const fn allows(&self, permission: TenantPermission) -> bool {
        self.role.allows(permission)
    }

    /// Installs this authenticated identity as transaction-local PostgreSQL state.
    ///
    /// The third argument to set_config is true, so PostgreSQL restores the
    /// previous settings when the transaction commits or rolls back. Never
    /// replace this with session-level SET on a pooled connection.
    pub(crate) async fn apply_to_transaction(
        &self,
        transaction: &Transaction<'_>,
    ) -> Result<(), StorageError> {
        transaction
            .query_one(
                "SELECT
                    set_config('aetherflow.tenant_id', $1, true),
                    set_config('aetherflow.principal_id', $2, true),
                    set_config('aetherflow.tenant_role', $3, true),
                set_config('aetherflow.worker_id', '', true)",
                &[
                    &self.tenant_id.as_str(),
                    &self.principal_id.as_str(),
                    &self.role.as_database_value(),
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{TenantPermission, TenantRole};

    #[test]
    fn tenant_roles_have_explicit_least_privilege_permissions() {
        for permission in [
            TenantPermission::Read,
            TenantPermission::Submit,
            TenantPermission::Cancel,
            TenantPermission::ManageCredentials,
        ] {
            assert!(TenantRole::Admin.allows(permission));
        }

        assert!(TenantRole::Operator.allows(TenantPermission::Read));
        assert!(TenantRole::Operator.allows(TenantPermission::Submit));
        assert!(TenantRole::Operator.allows(TenantPermission::Cancel));
        assert!(!TenantRole::Operator.allows(TenantPermission::ManageCredentials));

        assert!(TenantRole::Reader.allows(TenantPermission::Read));
        assert!(!TenantRole::Reader.allows(TenantPermission::Submit));
        assert!(!TenantRole::Reader.allows(TenantPermission::Cancel));
        assert!(!TenantRole::Reader.allows(TenantPermission::ManageCredentials));
    }

    #[test]
    fn tenant_roles_round_trip_stable_database_values() {
        for role in [TenantRole::Admin, TenantRole::Operator, TenantRole::Reader] {
            assert_eq!(
                TenantRole::from_database(role.as_database_value()),
                Some(role)
            );
        }
        assert_eq!(TenantRole::from_database("platform_admin"), None);
    }

    #[tokio::test]
    async fn tenant_context_is_transaction_local_and_does_not_leak_on_pool_reuse() {
        use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
        use tokio_postgres::{Config, NoTls};

        let Ok(database_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
            eprintln!(
                "AETHERFLOW_TEST_DATABASE_URL is not set; skipping tenant context PostgreSQL test"
            );
            return;
        };
        let config: Config = database_url.parse().expect("valid PostgreSQL URL");
        let manager = Manager::from_config(
            config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(1)
            .build()
            .expect("build single-connection test pool");
        let mut client = pool.get().await.expect("acquire test connection");
        let context = super::TenantContext::from_authenticated_credential(
            aetherflow_core::domain::TenantId::parse("tenant_context_test")
                .expect("valid tenant ID"),
            aetherflow_core::domain::PrincipalId::parse("principal_context_test")
                .expect("valid principal ID"),
            TenantRole::Operator,
        );

        let transaction = client
            .transaction()
            .await
            .expect("begin tenant-scoped transaction");
        context
            .apply_to_transaction(&transaction)
            .await
            .expect("apply tenant context");
        let active = transaction
            .query_one(
                "SELECT current_setting('aetherflow.tenant_id', true),
                        current_setting('aetherflow.principal_id', true),
                        current_setting('aetherflow.tenant_role', true)",
                &[],
            )
            .await
            .expect("read active tenant context");
        assert_eq!(active.get::<_, String>(0), "tenant_context_test");
        assert_eq!(active.get::<_, String>(1), "principal_context_test");
        assert_eq!(active.get::<_, String>(2), "tenant_operator");
        transaction
            .commit()
            .await
            .expect("commit tenant transaction");

        let after_commit = client
            .query_one(
                "SELECT
                    COALESCE(current_setting('aetherflow.tenant_id', true), ''),
                    COALESCE(current_setting('aetherflow.principal_id', true), ''),
                    COALESCE(current_setting('aetherflow.tenant_role', true), '')",
                &[],
            )
            .await
            .expect("inspect reused pooled connection");
        assert_eq!(after_commit.get::<_, String>(0), "");
        assert_eq!(after_commit.get::<_, String>(1), "");
        assert_eq!(after_commit.get::<_, String>(2), "");

        // A worker transaction receives only its worker identity until a
        // selected, grant-validated tenant is applied.
        let worker_transaction = client
            .transaction()
            .await
            .expect("begin worker-scoped transaction");
        let worker_id = aetherflow_core::domain::WorkerId::parse("worker_guc_test")
            .expect("valid worker ID");
        super::apply_worker_identity_to_transaction(&worker_transaction, &worker_id)
            .await
            .expect("apply worker identity");
        super::apply_tenant_id_to_transaction(&worker_transaction, "tenant_worker_test")
            .await
            .expect("apply selected tenant");
        let worker_active = worker_transaction
            .query_one(
                "SELECT current_setting('aetherflow.tenant_id', true),
                        current_setting('aetherflow.worker_id', true),
                        current_setting('aetherflow.principal_id', true),
                        current_setting('aetherflow.tenant_role', true)",
                &[],
            )
            .await
            .expect("read worker transaction context");
        assert_eq!(worker_active.get::<_, String>(0), "tenant_worker_test");
        assert_eq!(worker_active.get::<_, String>(1), "worker_guc_test");
        assert_eq!(worker_active.get::<_, String>(2), "");
        assert_eq!(worker_active.get::<_, String>(3), "worker");
        worker_transaction
            .commit()
            .await
            .expect("commit worker transaction");

        // Compatibility operations explicitly use legacy-default and must
        // also restore the connection's prior settings on commit.
        let legacy_transaction = client
            .transaction()
            .await
            .expect("begin legacy transaction");
        super::apply_legacy_worker_context_to_transaction(&legacy_transaction)
            .await
            .expect("apply legacy worker context");
        let legacy_active = legacy_transaction
            .query_one(
                "SELECT current_setting('aetherflow.tenant_id', true),
                        current_setting('aetherflow.worker_id', true),
                        current_setting('aetherflow.principal_id', true),
                        current_setting('aetherflow.tenant_role', true)",
                &[],
            )
            .await
            .expect("read legacy transaction context");
        assert_eq!(legacy_active.get::<_, String>(0), "legacy-default");
        assert_eq!(legacy_active.get::<_, String>(1), "");
        assert_eq!(legacy_active.get::<_, String>(2), "");
        assert_eq!(legacy_active.get::<_, String>(3), "legacy_worker");
        legacy_transaction
            .commit()
            .await
            .expect("commit legacy transaction");

        let after_worker_and_legacy = client
            .query_one(
                "SELECT
                    COALESCE(current_setting('aetherflow.tenant_id', true), ''),
                    COALESCE(current_setting('aetherflow.worker_id', true), ''),
                    COALESCE(current_setting('aetherflow.principal_id', true), ''),
                    COALESCE(current_setting('aetherflow.tenant_role', true), '')",
                &[],
            )
            .await
            .expect("inspect connection after worker and legacy transactions");
        for index in 0..4 {
            assert_eq!(after_worker_and_legacy.get::<_, String>(index), "");
        }
    }
}
