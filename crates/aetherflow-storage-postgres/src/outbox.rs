//! Transport-neutral PostgreSQL outbox dispatch.
//!
//! Publishing always happens outside database transactions. The event ID is
//! exposed to the publisher so a downstream transport or consumer can use it as
//! a stable deduplication key.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use aetherflow_core::domain::WorkerId;
use tokio::sync::watch;

use crate::{OutboxClaim, PgTaskRepository, StorageError};

/// Boxed asynchronous publication future.
pub type OutboxPublishFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), OutboxPublishError>> + Send + 'a>>;

/// Transport adapter used by the durable outbox dispatcher.
///
/// Implementations must treat `event_id` as the stable message identity. A
/// publish may be delivered more than once when the process crashes after the
/// remote system accepted it but before PostgreSQL acknowledges the claim.
pub trait OutboxPublisher: Send + Sync + 'static {
    /// Publishes one claimed event outside a database transaction.
    fn publish<'a>(&'a self, event: &'a OutboxClaim) -> OutboxPublishFuture<'a>;
}

/// A transient or permanent error returned by the transport adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxPublishError {
    message: String,
}

impl OutboxPublishError {
    /// Creates a publication error with a diagnostic message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Returns the diagnostic message persisted for a later retry.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for OutboxPublishError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for OutboxPublishError {}

/// Validation failures for an outbox dispatcher configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxDispatcherConfigError {
    /// Claim batch size must be positive.
    InvalidBatchSize,
    /// Poll interval must be greater than zero.
    ZeroPollInterval,
    /// Claim lease must be greater than zero.
    ZeroClaimLease,
    /// The initial retry delay must be greater than zero.
    ZeroInitialRetryDelay,
    /// Maximum retry delay cannot be shorter than the initial delay.
    MaximumRetryDelayBelowInitial,
}

impl std::fmt::Display for OutboxDispatcherConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBatchSize => formatter.write_str("outbox batch size must be positive"),
            Self::ZeroPollInterval => formatter.write_str("outbox poll interval must be non-zero"),
            Self::ZeroClaimLease => formatter.write_str("outbox claim lease must be non-zero"),
            Self::ZeroInitialRetryDelay => {
                formatter.write_str("outbox initial retry delay must be non-zero")
            }
            Self::MaximumRetryDelayBelowInitial => {
                formatter.write_str("outbox maximum retry delay is below its initial delay")
            }
        }
    }
}

impl std::error::Error for OutboxDispatcherConfigError {}

/// Bounded polling and retry settings for a PostgreSQL outbox dispatcher.
#[derive(Clone, Debug)]
pub struct OutboxDispatcherConfig {
    batch_size: i64,
    poll_interval: Duration,
    claim_lease: Duration,
    initial_retry_delay: Duration,
    maximum_retry_delay: Duration,
}

impl OutboxDispatcherConfig {
    /// Creates a dispatcher configuration with bounded exponential retry.
    pub fn new(
        batch_size: i64,
        poll_interval: Duration,
        claim_lease: Duration,
        initial_retry_delay: Duration,
        maximum_retry_delay: Duration,
    ) -> Result<Self, OutboxDispatcherConfigError> {
        if batch_size <= 0 {
            return Err(OutboxDispatcherConfigError::InvalidBatchSize);
        }
        if poll_interval.is_zero() {
            return Err(OutboxDispatcherConfigError::ZeroPollInterval);
        }
        if claim_lease.is_zero() {
            return Err(OutboxDispatcherConfigError::ZeroClaimLease);
        }
        if initial_retry_delay.is_zero() {
            return Err(OutboxDispatcherConfigError::ZeroInitialRetryDelay);
        }
        if maximum_retry_delay < initial_retry_delay {
            return Err(OutboxDispatcherConfigError::MaximumRetryDelayBelowInitial);
        }
        Ok(Self {
            batch_size,
            poll_interval,
            claim_lease,
            initial_retry_delay,
            maximum_retry_delay,
        })
    }

    /// Returns the configured event batch size.
    #[must_use]
    pub const fn batch_size(&self) -> i64 {
        self.batch_size
    }

    /// Returns the idle polling interval.
    #[must_use]
    pub const fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Returns the lease applied to claimed events.
    #[must_use]
    pub const fn claim_lease(&self) -> Duration {
        self.claim_lease
    }

    fn retry_delay(&self, publish_attempts: u32) -> Duration {
        let exponent = publish_attempts.saturating_sub(1).min(31);
        let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
        self.initial_retry_delay
            .saturating_mul(multiplier)
            .min(self.maximum_retry_delay)
    }
}

/// Summary of one bounded outbox dispatch pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OutboxDispatchReport {
    claimed: usize,
    acknowledged: usize,
    rescheduled: usize,
    ownership_lost: usize,
}

impl OutboxDispatchReport {
    /// Returns the number of events claimed from PostgreSQL.
    #[must_use]
    pub const fn claimed(&self) -> usize {
        self.claimed
    }

    /// Returns the number of events acknowledged after successful publication.
    #[must_use]
    pub const fn acknowledged(&self) -> usize {
        self.acknowledged
    }

    /// Returns the number of failed publications rescheduled for retry.
    #[must_use]
    pub const fn rescheduled(&self) -> usize {
        self.rescheduled
    }

    /// Returns the number of claims that were lost before ack or retry.
    #[must_use]
    pub const fn ownership_lost(&self) -> usize {
        self.ownership_lost
    }
}

/// Errors returned by the outbox dispatcher.
#[derive(Debug)]
pub enum OutboxDispatcherError {
    /// A PostgreSQL operation failed.
    Storage(StorageError),
}

impl std::fmt::Display for OutboxDispatcherError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for OutboxDispatcherError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
        }
    }
}

impl From<StorageError> for OutboxDispatcherError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

/// Polls PostgreSQL outbox claims and publishes events through a transport adapter.
///
/// The dispatcher claims a bounded batch, releases the database transaction,
/// publishes each event, then acknowledges or reschedules it with the exact
/// owner and fencing token. It never marks an event delivered before the
/// publisher confirms success.
pub struct PostgresOutboxDispatcher<P: OutboxPublisher> {
    repository: Arc<PgTaskRepository>,
    publisher_id: WorkerId,
    publisher: Arc<P>,
    config: OutboxDispatcherConfig,
}

impl<P: OutboxPublisher> Clone for PostgresOutboxDispatcher<P> {
    fn clone(&self) -> Self {
        Self {
            repository: Arc::clone(&self.repository),
            publisher_id: self.publisher_id.clone(),
            publisher: Arc::clone(&self.publisher),
            config: self.config.clone(),
        }
    }
}

impl<P: OutboxPublisher> PostgresOutboxDispatcher<P> {
    /// Creates a dispatcher over the durable repository and caller-supplied transport.
    #[must_use]
    pub fn new(
        repository: Arc<PgTaskRepository>,
        publisher_id: WorkerId,
        publisher: Arc<P>,
        config: OutboxDispatcherConfig,
    ) -> Self {
        Self {
            repository,
            publisher_id,
            publisher,
            config,
        }
    }

    /// Claims and processes one bounded batch.
    ///
    /// Publication errors are recorded and rescheduled. A lost claim is benign:
    /// another dispatcher owns the newer token, or the event will be reclaimed
    /// after lease expiry.
    pub async fn dispatch_once(
        &self,
    ) -> Result<OutboxDispatchReport, OutboxDispatcherError> {
        let claims = self
            .repository
            .claim_outbox_events(
                &self.publisher_id,
                self.config.batch_size,
                self.config.claim_lease,
            )
            .await?;
        let mut report = OutboxDispatchReport {
            claimed: claims.len(),
            ..OutboxDispatchReport::default()
        };

        for claim in claims {
            match self.publisher.publish(&claim).await {
                Ok(()) => {
                    match self
                        .repository
                        .acknowledge_outbox_event(
                            claim.event_id(),
                            &self.publisher_id,
                            claim.claim_token(),
                        )
                        .await
                    {
                        Ok(()) => report.acknowledged += 1,
                        Err(StorageError::ClaimLost(_)) => report.ownership_lost += 1,
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(error) => {
                    let delay = self.config.retry_delay(claim.publish_attempts());
                    match self
                        .repository
                        .retry_outbox_event(
                            claim.event_id(),
                            &self.publisher_id,
                            claim.claim_token(),
                            delay,
                            error.message(),
                        )
                        .await
                    {
                        Ok(()) => report.rescheduled += 1,
                        Err(StorageError::ClaimLost(_)) => report.ownership_lost += 1,
                        Err(storage_error) => return Err(storage_error.into()),
                    }
                }
            }
        }

        Ok(report)
    }

    /// Runs until shutdown is signaled, finishing any batch already claimed.
    ///
    /// If the process is interrupted during publication, the claim remains leased
    /// and can be reclaimed after expiry. A crash after remote acceptance but
    /// before acknowledgement may redeliver the event; publishers and consumers
    /// should use the stable event ID for idempotency.
    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), OutboxDispatcherError> {
        loop {
            if *shutdown.borrow_and_update() {
                return Ok(());
            }

            let report = self.dispatch_once().await?;
            if report.claimed() > 0 {
                continue;
            }

            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow_and_update() {
                        return Ok(());
                    }
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OutboxDispatcherConfig, OutboxDispatcherConfigError};
    use std::time::Duration;

    #[test]
    fn retry_delay_grows_exponentially_and_is_capped() {
        let config = OutboxDispatcherConfig::new(
            16,
            Duration::from_millis(10),
            Duration::from_secs(5),
            Duration::from_millis(100),
            Duration::from_millis(350),
        )
        .expect("valid dispatcher config");

        assert_eq!(config.retry_delay(1), Duration::from_millis(100));
        assert_eq!(config.retry_delay(2), Duration::from_millis(200));
        assert_eq!(config.retry_delay(3), Duration::from_millis(350));
        assert_eq!(config.retry_delay(u32::MAX), Duration::from_millis(350));
    }

    #[test]
    fn dispatcher_rejects_zero_batch_size() {
        assert_eq!(
            OutboxDispatcherConfig::new(
                0,
                Duration::from_millis(10),
                Duration::from_secs(5),
                Duration::from_millis(100),
                Duration::from_secs(1),
            )
            .unwrap_err(),
            OutboxDispatcherConfigError::InvalidBatchSize
        );
    }
}
