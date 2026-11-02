//! Durable consumer inbox and event deduplication.
//!
//! The inbox is a lease-based idempotency ledger keyed by (consumer, event ID).
//! A broker message should be acknowledged only after the application has
//! successfully completed its work and this repository has acknowledged the
//! inbox claim. Delivery remains at-least-once: external side effects must use
//! the event ID as their own idempotency key, or be committed in the same
//! database transaction as the inbox completion.

use std::time::Duration;

use aetherflow_core::domain::WorkerId;
use deadpool_postgres::Pool;

use crate::StorageError;

const MAX_LEASE_MILLIS: u128 = i64::MAX as u128;
const MAX_EVENT_ID_BYTES: usize = 255;
const MAX_ERROR_BYTES: usize = 4096;

const CLAIM_INBOX_SQL: &str = "
    INSERT INTO consumer_inbox AS inbox (
        consumer_name, event_id, status, attempts, claim_owner,
        claim_token, lease_expires_at
    )
    VALUES (
        $1, $2, 'processing', 1, $3, 1,
        clock_timestamp() + ($4::BIGINT * INTERVAL '1 millisecond')
    )
    ON CONFLICT (consumer_name, event_id) DO UPDATE
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
    consumer_name: String,
    event_id: String,
    worker_id: WorkerId,
    claim_token: u64,
    attempts: u64,
}

impl InboxClaim {
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

    /// Attempts to claim an event for a logical consumer.
    ///
    /// A repeated processed event returns AlreadyProcessed. A concurrent
    /// delivery with an unexpired lease returns Busy. Pending or expired
    /// processing rows are reclaimed atomically and receive a higher token.
    pub async fn try_claim(
        &self,
        consumer_name: &str,
        event_id: &str,
        worker_id: &WorkerId,
        lease_duration: Duration,
    ) -> Result<InboxClaimResult, StorageError> {
        validate_consumer_name(consumer_name)?;
        validate_event_id(event_id)?;

        let lease_millis = lease_duration.as_millis();
        if lease_millis == 0 || lease_millis > MAX_LEASE_MILLIS {
            return Err(StorageError::InvalidLeaseDuration);
        }
        let lease_millis =
            i64::try_from(lease_millis).map_err(|_| StorageError::InvalidLeaseDuration)?;

        let client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let row = client
            .query_opt(
                CLAIM_INBOX_SQL,
                &[
                    &consumer_name,
                    &event_id,
                    &worker_id.as_str(),
                    &lease_millis,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        if let Some(row) = row {
            let claim_token: i64 = row.get(0);
            let attempts: i64 = row.get(1);
            return Ok(InboxClaimResult::Acquired(InboxClaim {
                consumer_name: consumer_name.to_owned(),
                event_id: event_id.to_owned(),
                worker_id: worker_id.clone(),
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
            }));
        }

        let row = client
            .query_opt(
                "SELECT status FROM consumer_inbox WHERE consumer_name = $1 AND event_id = $2",
                &[&consumer_name, &event_id],
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
            "processed" => Ok(InboxClaimResult::AlreadyProcessed),
            "processing" => Ok(InboxClaimResult::Busy),
            other => Err(StorageError::InvalidStoredValue(format!(
                "unexpected consumer inbox status: {other}"
            ))),
        }
    }

    /// Marks a claim processed after its handler succeeds.
    ///
    /// The operation is fenced by consumer, event, worker, token, and unexpired
    /// database lease. Repeating the same successful acknowledgement is safe.
    pub async fn acknowledge(&self, claim: &InboxClaim) -> Result<(), StorageError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let claim_token = i64::try_from(claim.claim_token).map_err(|_| {
            StorageError::InvalidStoredValue(
                "consumer inbox claim token is outside the supported range".to_owned(),
            )
        })?;
        let updated = client
            .execute(
                "UPDATE consumer_inbox
                 SET status = 'processed',
                     claim_owner = NULL,
                     lease_expires_at = NULL,
                     last_error = NULL,
                     processed_at = clock_timestamp(),
                     updated_at = clock_timestamp()
                 WHERE consumer_name = $1
                   AND event_id = $2
                   AND status = 'processing'
                   AND claim_owner = $3
                   AND claim_token = $4
                   AND lease_expires_at > clock_timestamp()",
                &[
                    &claim.consumer_name,
                    &claim.event_id,
                    &claim.worker_id.as_str(),
                    &claim_token,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        if updated == 1 {
            return Ok(());
        }

        let row = client
            .query_opt(
                "SELECT status, claim_token FROM consumer_inbox
                 WHERE consumer_name = $1 AND event_id = $2",
                &[&claim.consumer_name, &claim.event_id],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if let Some(row) = row {
            let status: String = row.get(0);
            let token: i64 = row.get(1);
            if status == "processed" && token == claim_token {
                return Ok(());
            }
        }

        Err(inbox_claim_lost(claim))
    }

    /// Releases a failed claim for immediate redelivery.
    ///
    /// The next owner receives a higher fencing token. The caller should avoid
    /// acknowledging the broker message after releasing this claim.
    pub async fn release(
        &self,
        claim: &InboxClaim,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        if error.is_some_and(|message| message.len() > MAX_ERROR_BYTES) {
            return Err(StorageError::InvalidInboxError);
        }

        let claim_token = i64::try_from(claim.claim_token).map_err(|_| {
            StorageError::InvalidStoredValue(
                "consumer inbox claim token is outside the supported range".to_owned(),
            )
        })?;
        let client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let updated = client
            .execute(
                "UPDATE consumer_inbox
                 SET status = 'pending',
                     claim_owner = NULL,
                     lease_expires_at = NULL,
                     last_error = $5,
                     updated_at = clock_timestamp()
                 WHERE consumer_name = $1
                   AND event_id = $2
                   AND status = 'processing'
                   AND claim_owner = $3
                   AND claim_token = $4
                   AND lease_expires_at > clock_timestamp()",
                &[
                    &claim.consumer_name,
                    &claim.event_id,
                    &claim.worker_id.as_str(),
                    &claim_token,
                    &error,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        if updated == 1 {
            Ok(())
        } else {
            Err(inbox_claim_lost(claim))
        }
    }
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
