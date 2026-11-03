//! Authenticated tenant and authorization context types.
//!
//! A TenantContext can only be minted inside this crate after a credential
//! has been validated against active credential, principal, and tenant rows.

use aetherflow_core::domain::{PrincipalId, TenantId};

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
}
