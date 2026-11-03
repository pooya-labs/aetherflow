//! Durable consumer inbox and event deduplication.
//!
//! The compatibility inbox is a lease-based idempotency ledger restricted to
//! legacy-default. Tenant-aware inbox operations need a tenant-scoped API and
//! tenant-leading primary key before customer event processing is enabled.
//! A broker message should be acknowledged only after the application has
//! successfully completed its work and this repository has acknowledged the
//! inbox claim. Delivery remains at-least-once: external side effects must use
//! the event ID as their own idempotency key, or be committed in the same
//! database transaction as the inbox completion.

use std::time::Duration;

use aetherflow_core::domain::{TenantId, WorkerId};
use tokio_postgres::Transaction;

use crate::membership::lock_worker_registration;
use crate::task_repository::lock_worker_tenant_grant;
use deadpool_postgres::Pool;

use crate::{StorageError, WorkerRegistration};

const MAX_LEASE_MILLIS: u128 = i64::MAX as u128;
const MAX_EVENT_ID_BYTES: usize = 255;
const MAX_ERROR_BYTES: usize = 4096;

const CLAIM_INBOX_SQL: &str = "
    INSERT INTO consumer_inbox AS inbox (
        tenant_id, consumer_name, event_id, status, attempts, claim_owner,
        claim_token, lease_expires_at
    )
    VALUES (
        $1, $2, $3, 'processing', 1, $4, 1,
        clock_timestamp() + ($5::BIGINT * INTERVAL '1 millisecond')
    )
    ON CONFLICT (tenant_id, consumer_name, event_id) DO UPDATE
    SET status = 'processing',
        attempts = inbox.attempts + 1,
        claim_owner = EXCLUDED.claim_owner,
        claim_token = inbox.claim_token + 1,
        lease_expires_at = EXCLUDED.lease_expires_at,
        last_error = NULL,
        updated_at = clock_timestamp()
    WHERE inbox.status = 'pending'
       OR (
            inbox.status = 'processing'
            AND inbox.lease_expires_at <= clock_timestamp()
       )
    RETURNING claim_token, attempts
";

/// A fenced lease for processing one event for one logical consumer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboxClaim {
    tenant_id: String,
    consumer_name: String,
    event_id: String,
    worker_id: WorkerId,
    registration: Option<WorkerRegistration>,
    claim_token: u64,
    attempts: u64,
}

impl InboxClaim {
    /// Returns the tenant that owns this inbox claim.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the logical consumer identity.
    #[must_use]
    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    /// Returns the stable event identity supplied by the broker or producer.
    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    /// Returns the worker that currently owns the lease.
    #[must_use]
    pub const fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    /// Returns the monotonically increasing fencing token for this claim.
    #[must_use]
    pub const fn claim_token(&self) -> u64 {
        self.claim_token
    }

    /// Returns the number of times this event has been claimed by this consumer.
    #[must_use]
    pub const fn attempts(&self) -> u64 {
        self.attempts
    }
}

/// Outcome of attempting to acquire an inbox event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InboxClaimResult {
    /// The caller owns a new or reclaimed processing lease.
    Acquired(InboxClaim),
    /// This consumer has already completed the event and may ack broker redelivery.
    AlreadyProcessed,
    /// Another worker currently owns an unexpired processing lease.
    Busy,
}

/// PostgreSQL-backed idempotency ledger for broker consumers.
#[derive(Clone)]
pub struct PgInboxRepository {
    pool: Pool,
}

impl PgInboxRepository {
    /// Creates an inbox repository using the caller's configured PostgreSQL pool.
    #[must_use]
    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }

    /// Attempts to claim an event in the reserved compatibility tenant.
    ///
    /// The legacy entry point cannot claim customer-owned tenant events.
    pub async fn try_claim(
        &self,
        consumer_name: &str,
        event_id: &str,
        worker_id: &WorkerId,
        lease_duration: Duration,
    ) -> Result<InboxClaimResult, StorageError> {
        self.try_claim_inner(
            "legacy-default",
            None,
            consumer_name,
            event_id,
            worker_id,
            lease_duration,
        )
        .await
    }

    /// Claims an event for a tenant explicitly granted to the registered worker.
    pub async fn try_claim_for_tenant(
        &self,
        tenant_id: &TenantId,
        registration: &WorkerRegistration,
        consumer_name: &str,
        event_id: &str,
        lease_duration: Duration,
    ) -> Result<InboxClaimResult, StorageError> {
        self.try_claim_inner(
            tenant_id.as_str(),
            Some(registration),
            consumer_name,
            event_id,
            registration.worker_id(),
            lease_duration,
        )
        .await
    }

    async fn try_claim_inner(
        &self,
        tenant_id: &str,
        registration: Option<&WorkerRegistration>,
        consumer_name: &str,
        event_id: &str,
        worker_id: &WorkerId,
        lease_duration: Duration,
    ) -> Result<InboxClaimResult, StorageError> {
        validate_consumer_name(consumer_name)?;
        validate_event_id(event_id)?;
        if registration.is_none() && tenant_id != "legacy-default" {
            return Err(StorageError::InvalidTenantCredential);
        }
        if let Some(registration) = registration {
            if registration.worker_id() != worker_id {
                return Err(StorageError::WorkerRegistrationLost(
                    worker_id.as_str().to_owned(),
                ));
            }
        }

        let lease_millis = lease_duration.as_millis();
        if lease_millis == 0 || lease_millis > MAX_LEASE_MILLIS {
            return Err(StorageError::InvalidLeaseDuration);
        }
        let lease_millis =
            i64::try_from(lease_millis).map_err(|_| StorageError::InvalidLeaseDuration)?;

        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if let Some(registration) = registration {
            lock_worker_registration(&transaction, registration, true).await?;
            lock_worker_tenant_grant(&transaction, registration, tenant_id).await?;
        }

        let row = transaction
            .query_opt(
                CLAIM_INBOX_SQL,
                &[
                    &tenant_id,
                    &consumer_name,
                    &event_id,
                    &worker_id.as_str(),
                    &lease_millis,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        let result = if let Some(row) = row {
            let claim_token: i64 = row.get(0);
            let attempts: i64 = row.get(1);
            InboxClaimResult::Acquired(InboxClaim {
                tenant_id: tenant_id.to_owned(),
                consumer_name: consumer_name.to_owned(),
                event_id: event_id.to_owned(),
                worker_id: worker_id.clone(),
                registration: registration.cloned(),
                claim_token: u64::try_from(claim_token).map_err(|_| {
                    StorageError::InvalidStoredValue(
                        "consumer inbox claim token is outside the supported range".to_owned(),
                    )
                })?,
                attempts: u64::try_from(attempts).map_err(|_| {
                    StorageError::InvalidStoredValue(
                        "consumer inbox attempt count is outside the supported range".to_owned(),
                    )
                })?,
            })
        } else {
            let row = transaction
                .query_opt(
                    "SELECT status FROM consumer_inbox
                     WHERE tenant_id = $1 AND consumer_name = $2 AND event_id = $3",
                    &[&tenant_id, &consumer_name, &event_id],
                )
                .await
                .map_err(|error| StorageError::Database(error.to_string()))?
                .ok_or_else(|| {
                    StorageError::InconsistentState(
                        "consumer inbox claim conflicted but its row disappeared".to_owned(),
                    )
                })?;
            let status: String = row.get(0);
            match status.as_str() {
                "processed" => InboxClaimResult::AlreadyProcessed,
                "pending" | "processing" => InboxClaimResult::Busy,
                other => {
                    return Err(StorageError::InvalidStoredValue(format!(
                        "unexpected consumer inbox status: {other}"
                    )));
                }
            }
        };

        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(result)
    }

    /// Marks a claim processed after its handler succeeds.
    ///
    /// Registered claims revalidate membership and tenant grant in this transaction.
    /// Repeating the same successful acknowledgement remains safe.
    pub async fn acknowledge(&self, claim: &InboxClaim) -> Result<(), StorageError> {
        let claim_token = claim_token_sql(claim)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        lock_claim_registration(&transaction, claim, false).await?;

        let updated = transaction
            .execute(
                "UPDATE consumer_inbox
                 SET status = 'processed',
                     claim_owner = NULL,
                     lease_expires_at = NULL,
                     last_error = NULL,
                     processed_at = clock_timestamp(),
                     updated_at = clock_timestamp()
                 WHERE tenant_id = $1 AND consumer_name = $2
                   AND event_id = $3
                   AND status = 'processing'
                   AND claim_owner = $4
                   AND claim_token = $5
                   AND lease_expires_at > clock_timestamp()",
                &[
                    &claim.tenant_id,
                    &claim.consumer_name,
                    &claim.event_id,
                    &claim.worker_id.as_str(),
                    &claim_token,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        if updated == 1 {
            transaction
                .commit()
                .await
                .map_err(|error| StorageError::Database(error.to_string()))?;
            return Ok(());
        }

        let row = transaction
            .query_opt(
                "SELECT status, claim_token FROM consumer_inbox
                 WHERE tenant_id = $1 AND consumer_name = $2 AND event_id = $3",
                &[&claim.tenant_id, &claim.consumer_name, &claim.event_id],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if let Some(row) = row {
            let status: String = row.get(0);
            let token: i64 = row.get(1);
            if status == "processed" && token == claim_token {
                transaction
                    .commit()
                    .await
                    .map_err(|error| StorageError::Database(error.to_string()))?;
                return Ok(());
            }
        }
        Err(inbox_claim_lost(claim))
    }

    /// Extends a live inbox claim using database time.
    ///
    /// A registered worker must still hold a live grant for the claim tenant.
    pub async fn renew(
        &self,
        claim: &InboxClaim,
        lease_duration: Duration,
    ) -> Result<(), StorageError> {
        let lease_millis = lease_duration.as_millis();
        if lease_millis == 0 || lease_millis > MAX_LEASE_MILLIS {
            return Err(StorageError::InvalidLeaseDuration);
        }
        let lease_millis =
            i64::try_from(lease_millis).map_err(|_| StorageError::InvalidLeaseDuration)?;
        let claim_token = claim_token_sql(claim)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        lock_claim_registration(&transaction, claim, false).await?;

        let updated = transaction
            .execute(
                "UPDATE consumer_inbox
                 SET lease_expires_at =
                         clock_timestamp() + ($6::BIGINT * INTERVAL '1 millisecond'),
                     updated_at = clock_timestamp()
                 WHERE tenant_id = $1 AND consumer_name = $2
                   AND event_id = $3
                   AND status = 'processing'
                   AND claim_owner = $4
                   AND claim_token = $5
                   AND lease_expires_at > clock_timestamp()",
                &[
                    &claim.tenant_id,
                    &claim.consumer_name,
                    &claim.event_id,
                    &claim.worker_id.as_str(),
                    &claim_token,
                    &lease_millis,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        if updated != 1 {
            return Err(inbox_claim_lost(claim));
        }
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(())
    }

    /// Releases a failed claim for immediate redelivery.
    ///
    /// A registered worker must still hold a live grant for the claim tenant.
    pub async fn release(
        &self,
        claim: &InboxClaim,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        if error.is_some_and(|message| message.len() > MAX_ERROR_BYTES) {
            return Err(StorageError::InvalidInboxError);
        }

        let claim_token = claim_token_sql(claim)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        lock_claim_registration(&transaction, claim, false).await?;

        let updated = transaction
            .execute(
                "UPDATE consumer_inbox
                 SET status = 'pending',
                     claim_owner = NULL,
                     lease_expires_at = NULL,
                     last_error = $6,
                     updated_at = clock_timestamp()
                 WHERE tenant_id = $1 AND consumer_name = $2
                   AND event_id = $3
                   AND status = 'processing'
                   AND claim_owner = $4
                   AND claim_token = $5
                   AND lease_expires_at > clock_timestamp()",
                &[
                    &claim.tenant_id,
                    &claim.consumer_name,
                    &claim.event_id,
                    &claim.worker_id.as_str(),
                    &claim_token,
                    &error,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        if updated != 1 {
            return Err(inbox_claim_lost(claim));
        }
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(())
    }

}

async fn lock_claim_registration(
    transaction: &Transaction<'_>,
    claim: &InboxClaim,
    require_active: bool,
) -> Result<(), StorageError> {
    match claim.registration.as_ref() {
        Some(registration) => {
            if registration.worker_id() != &claim.worker_id {
                return Err(StorageError::WorkerRegistrationLost(
                    claim.worker_id.as_str().to_owned(),
                ));
            }
            lock_worker_registration(transaction, registration, require_active).await?;
            lock_worker_tenant_grant(transaction, registration, &claim.tenant_id).await
        }
        None if claim.tenant_id == "legacy-default" => Ok(()),
        None => Err(StorageError::InvalidTenantCredential),
    }
}

fn claim_token_sql(claim: &InboxClaim) -> Result<i64, StorageError> {
    i64::try_from(claim.claim_token).map_err(|_| {
        StorageError::InvalidStoredValue(
            "consumer inbox claim token is outside the supported range".to_owned(),
        )
    })
}

fn validate_consumer_name(consumer_name: &str) -> Result<(), StorageError> {
    if !is_ascii_identifier(consumer_name) {
        Err(StorageError::InvalidInboxConsumerName)
    } else {
        Ok(())
    }
}

fn validate_event_id(event_id: &str) -> Result<(), StorageError> {
    if event_id.is_empty()
        || event_id.len() > MAX_EVENT_ID_BYTES
        || !event_id.bytes().all(|byte| (b'!'..=b'~').contains(&byte))
    {
        Err(StorageError::InvalidInboxEventId)
    } else {
        Ok(())
    }
}

fn is_ascii_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn inbox_claim_lost(claim: &InboxClaim) -> StorageError {
    StorageError::InboxClaimLost {
        consumer_name: claim.consumer_name.clone(),
        event_id: claim.event_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_consumer_name, validate_event_id};
    use crate::StorageError;

    #[test]
    fn inbox_consumer_names_use_bounded_ascii_identifiers() {
        assert!(validate_consumer_name("billing-worker_v1").is_ok());
        assert_eq!(
            validate_consumer_name("invalid consumer"),
            Err(StorageError::InvalidInboxConsumerName)
        );
    }

    #[test]
    fn inbox_event_ids_are_bounded_visible_ascii_tokens() {
        assert!(validate_event_id("outbox:184467").is_ok());
        assert_eq!(
            validate_event_id("event with spaces"),
            Err(StorageError::InvalidInboxEventId)
        );
        assert_eq!(
            validate_event_id(&"x".repeat(256)),
            Err(StorageError::InvalidInboxEventId)
        );
    }
}
