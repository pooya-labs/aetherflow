//! PostgreSQL-backed tenant credential authentication.

use aetherflow_core::domain::{PrincipalId, TenantId};
use deadpool_postgres::Pool;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::{StorageError, TenantContext, TenantRole};

const MIN_TOKEN_BYTES: usize = 32;
const MAX_TOKEN_BYTES: usize = 256;

/// Authenticates opaque tenant credentials against the durable credential registry.
///
/// The raw token is never persisted. Authentication locks the credential,
/// principal, and tenant rows while validating status and expiry, then updates
/// only the credential's last-used timestamp before committing.
#[derive(Clone)]
pub struct PgTenantCredentialRepository {
    pool: Pool,
}

impl PgTenantCredentialRepository {
    /// Creates an authenticator over a caller-configured PostgreSQL pool.
    #[must_use]
    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }

    /// Authenticates a raw high-entropy token and returns its opaque tenant context.
    ///
    /// All credential failures share InvalidTenantCredential; callers must not
    /// disclose whether a token was unknown, revoked, expired, or bound to a
    /// disabled principal/tenant.
    pub async fn authenticate_token(
        &self,
        token: &str,
    ) -> Result<TenantContext, StorageError> {
        if !valid_token_shape(token) {
            return Err(StorageError::InvalidTenantCredential);
        }

        let digest = Sha256::digest(token.as_bytes());
        let digest_hex = format!("{digest:x}");

        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        let row = transaction
            .query_opt(
                "SELECT c.credential_id, c.tenant_id, c.principal_id,
                        c.token_sha256, c.role
                 FROM tenant_credentials AS c
                 JOIN tenant_principals AS p
                   ON p.tenant_id = c.tenant_id AND p.principal_id = c.principal_id
                 JOIN tenants AS t ON t.tenant_id = c.tenant_id
                 WHERE c.token_sha256 = $1
                   AND c.status = 'active'
                   AND p.status = 'active'
                   AND t.status = 'active'
                   AND (c.expires_at IS NULL OR c.expires_at > clock_timestamp())
                 FOR SHARE OF c, p, t",
                &[&digest_hex],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?
            .ok_or(StorageError::InvalidTenantCredential)?;

        let credential_id: String = row.get(0);
        let tenant_id: String = row.get(1);
        let principal_id: String = row.get(2);
        let stored_digest: String = row.get(3);
        let stored_role: String = row.get(4);

        let expected_digest: [u8; 32] = digest.into();
        let stored_digest = decode_sha256_hex(stored_digest.trim_end())
            .ok_or(StorageError::InvalidTenantCredential)?;
        if !bool::from(expected_digest.ct_eq(&stored_digest)) {
            return Err(StorageError::InvalidTenantCredential);
        }

        let tenant_id = TenantId::parse(tenant_id)
            .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))?;
        let principal_id = PrincipalId::parse(principal_id)
            .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))?;
        let role = TenantRole::from_database(&stored_role).ok_or_else(|| {
            StorageError::InvalidStoredValue("unknown tenant credential role".to_owned())
        })?;

        transaction
            .execute(
                "UPDATE tenant_credentials
                 SET last_used_at = clock_timestamp()
                 WHERE credential_id = $1",
                &[&credential_id],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        Ok(TenantContext::from_authenticated_credential(
            tenant_id,
            principal_id,
            role,
        ))
    }
}

fn valid_token_shape(token: &str) -> bool {
    (MIN_TOKEN_BYTES..=MAX_TOKEN_BYTES).contains(&token.len())
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn decode_sha256_hex(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }

    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        digest[index] = (high << 4) | low;
    }
    Some(digest)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_sha256_hex, valid_token_shape};

    #[test]
    fn tenant_tokens_require_bounded_url_safe_high_entropy_shape() {
        assert!(valid_token_shape("AetherFlowTenantCredential012345"));
        assert!(valid_token_shape("a".repeat(256).as_str()));
        assert!(!valid_token_shape("short"));
        assert!(!valid_token_shape("a".repeat(257).as_str()));
        assert!(!valid_token_shape("a-token-with spaces-and-padding"));
        assert!(!valid_token_shape("token.with.dots.and.symbols"));
    }

    #[test]
    fn sha256_hex_decoder_accepts_only_lowercase_64_digit_values() {
        let decoded = decode_sha256_hex(&"ab".repeat(32)).expect("valid digest");
        assert_eq!(decoded, [0xab; 32]);
        assert!(decode_sha256_hex("AA".repeat(32).as_str()).is_none());
        assert!(decode_sha256_hex("a".repeat(63).as_str()).is_none());
    }
}
