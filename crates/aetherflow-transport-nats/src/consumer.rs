//! Durable JetStream consumer loop backed by the PostgreSQL inbox.
//!
//! Inbox completion is committed before the JetStream delivery is acknowledged.
//! A redelivery after inbox completion is deduplicated by the stable event ID.
//! Handler side effects outside PostgreSQL still require their own idempotency
//! key or a transaction that includes inbox completion.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aetherflow_core::domain::{TenantId, WorkerId};
use aetherflow_storage_postgres::{
    InboxClaim, InboxClaimResult, PgInboxRepository, StorageError, WorkerRegistration,
};
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::{AckKind, Message as JetStreamMessage};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::watch;

/// Boxed handler future for one decoded event.
pub type EventHandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// A versioned event envelope written by the AetherFlow outbox publisher.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct JetStreamEvent {
    schema_version: u16,
    #[serde(default = "legacy_default_tenant")]
    tenant_id: String,
    event_id: String,
    aggregate_type: String,
    aggregate_id: String,
    aggregate_version: u64,
    event_type: String,
    payload: serde_json::Value,
}

impl JetStreamEvent {
    /// Returns the tenant that owns this event.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the envelope schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    /// Returns the stable outbox event identity.
    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    /// Returns the aggregate category.
    #[must_use]
    pub fn aggregate_type(&self) -> &str {
        &self.aggregate_type
    }

    /// Returns the aggregate identity.
    #[must_use]
    pub fn aggregate_id(&self) -> &str {
        &self.aggregate_id
    }

    /// Returns the aggregate version associated with this event.
    #[must_use]
    pub const fn aggregate_version(&self) -> u64 {
        self.aggregate_version
    }

    /// Returns the event type.
    #[must_use]
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    /// Returns the application payload.
    #[must_use]
    pub const fn payload(&self) -> &serde_json::Value {
        &self.payload
    }
}

/// Application handler invoked after a durable inbox claim is acquired.
pub trait JetStreamEventHandler: Send + Sync + 'static {
    /// Processes one event. Return an error to release the inbox claim and allow
    /// JetStream redelivery after its configured acknowledgement wait.
    fn handle<'a>(&'a self, event: &'a JetStreamEvent) -> EventHandlerFuture<'a>;
}

/// Snapshot of the consumer's in-process counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConsumerMetricsSnapshot {
    /// Successfully completed handler invocations.
    pub processed: usize,
    /// Broker redeliveries suppressed by the durable inbox.
    pub duplicates: usize,
    /// Deliveries deferred because another worker holds an active claim.
    pub busy: usize,
    /// Handler failures released back to the durable inbox.
    pub handler_failures: usize,
}

/// Shared counters for an active NATS consumer.
#[derive(Clone, Default)]
pub struct ConsumerMetrics {
    processed: Arc<AtomicUsize>,
    duplicates: Arc<AtomicUsize>,
    busy: Arc<AtomicUsize>,
    handler_failures: Arc<AtomicUsize>,
}

impl ConsumerMetrics {
    /// Returns a consistent-enough point-in-time snapshot of the counters.
    #[must_use]
    pub fn snapshot(&self) -> ConsumerMetricsSnapshot {
        ConsumerMetricsSnapshot {
            processed: self.processed.load(Ordering::Relaxed),
            duplicates: self.duplicates.load(Ordering::Relaxed),
            busy: self.busy.load(Ordering::Relaxed),
            handler_failures: self.handler_failures.load(Ordering::Relaxed),
        }
    }
}

/// Errors that stop the durable JetStream consumer loop.
#[derive(Debug)]
pub enum NatsConsumerError {
    /// A JetStream operation failed.
    Broker(String),
    /// A durable inbox operation failed.
    Storage(StorageError),
    /// A delivered payload cannot be decoded as the supported event envelope.
    InvalidEnvelope(String),
    /// The event envelope uses a schema version this consumer cannot process.
    UnsupportedSchemaVersion(u16),
    /// The inbox lease is zero or cannot be represented in PostgreSQL.
    InvalidLeaseDuration,
    /// A tenant-owned event was delivered to a consumer without a registered tenant grant.
    TenantRegistrationRequired,
}

impl std::fmt::Display for NatsConsumerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Broker(error) => write!(formatter, "JetStream consumer error: {error}"),
            Self::Storage(error) => write!(formatter, "consumer inbox error: {error}"),
            Self::InvalidEnvelope(error) => {
                write!(formatter, "invalid AetherFlow event envelope: {error}")
            }
            Self::UnsupportedSchemaVersion(version) => {
                write!(
                    formatter,
                    "unsupported AetherFlow event schema version: {version}"
                )
            }
            Self::InvalidLeaseDuration => {
                formatter.write_str("consumer inbox lease duration is not representable")
            }
            Self::TenantRegistrationRequired => {
                formatter.write_str("tenant-owned events require a registered worker with a tenant grant")
            }
        }
    }
}

impl std::error::Error for NatsConsumerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}

impl From<StorageError> for NatsConsumerError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

enum HandlerExecutionError {
    Handler(String),
    Storage(StorageError),
    Broker(String),
}

/// Pull-based JetStream consumer that coordinates each event through
/// `PgInboxRepository` before acknowledging the broker delivery.
pub struct NatsJetStreamConsumer<H: JetStreamEventHandler> {
    consumer: PullConsumer,
    inbox: PgInboxRepository,
    consumer_name: String,
    worker_id: WorkerId,
    registration: Option<WorkerRegistration>,
    lease_duration: Duration,
    handler: Arc<H>,
    metrics: ConsumerMetrics,
}

impl<H: JetStreamEventHandler> NatsJetStreamConsumer<H> {
    /// Creates a consumer for the reserved legacy-default inbox.
    ///
    /// This compatibility constructor cannot process tenant-owned events.
    pub fn new(
        consumer: PullConsumer,
        inbox: PgInboxRepository,
        consumer_name: impl Into<String>,
        worker_id: WorkerId,
        lease_duration: Duration,
        handler: Arc<H>,
    ) -> Result<Self, NatsConsumerError> {
        Self::build(
            consumer,
            inbox,
            consumer_name.into(),
            worker_id,
            None,
            lease_duration,
            handler,
        )
    }

    /// Creates a consumer whose registered worker grant gates tenant inbox access.
    pub fn new_registered(
        consumer: PullConsumer,
        inbox: PgInboxRepository,
        consumer_name: impl Into<String>,
        registration: WorkerRegistration,
        lease_duration: Duration,
        handler: Arc<H>,
    ) -> Result<Self, NatsConsumerError> {
        let worker_id = registration.worker_id().clone();
        Self::build(
            consumer,
            inbox,
            consumer_name.into(),
            worker_id,
            Some(registration),
            lease_duration,
            handler,
        )
    }

    fn build(
        consumer: PullConsumer,
        inbox: PgInboxRepository,
        consumer_name: String,
        worker_id: WorkerId,
        registration: Option<WorkerRegistration>,
        lease_duration: Duration,
        handler: Arc<H>,
    ) -> Result<Self, NatsConsumerError> {
        if lease_duration.is_zero() || lease_duration.as_millis() > i64::MAX as u128 {
            return Err(NatsConsumerError::InvalidLeaseDuration);
        }

        Ok(Self {
            consumer,
            inbox,
            consumer_name,
            worker_id,
            registration,
            lease_duration,
            handler,
            metrics: ConsumerMetrics::default(),
        })
    }

    /// Returns a handle for observing this consumer instance's counters.
    #[must_use]
    pub fn metrics(&self) -> ConsumerMetrics {
        self.metrics.clone()
    }

    /// Runs until shutdown is signaled or a broker/storage/protocol error occurs.
    ///
    /// The current delivery, if any, is allowed to finish before shutdown. An
    /// unprocessed delivery is never acknowledged merely because shutdown began.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), NatsConsumerError> {
        if *shutdown.borrow_and_update() {
            return Ok(());
        }

        let mut messages = self
            .consumer
            .messages()
            .await
            .map_err(|error| NatsConsumerError::Broker(error.to_string()))?;

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow_and_update() {
                        return Ok(());
                    }
                }
                delivery = messages.next() => {
                    match delivery {
                        Some(Ok(message)) => self.process_message(message).await?,
                        Some(Err(error)) => {
                            return Err(NatsConsumerError::Broker(error.to_string()));
                        }
                        None => return Ok(()),
                    }
                }
            }
        }
    }

    async fn process_message(&self, message: JetStreamMessage) -> Result<(), NatsConsumerError> {
        let event: JetStreamEvent = serde_json::from_slice(&message.payload)
            .map_err(|error| NatsConsumerError::InvalidEnvelope(error.to_string()))?;
        if event.schema_version != 1 {
            return Err(NatsConsumerError::UnsupportedSchemaVersion(
                event.schema_version,
            ));
        }

        let claim_result = match self.registration.as_ref() {
            Some(registration) => {
                let tenant_id = TenantId::parse(event.tenant_id.clone())
                    .map_err(|error| NatsConsumerError::InvalidEnvelope(error.to_string()))?;
                self.inbox
                    .try_claim_for_tenant(
                        &tenant_id,
                        registration,
                        &self.consumer_name,
                        &event.event_id,
                        self.lease_duration,
                    )
                    .await?
            }
            None if event.tenant_id == "legacy-default" => {
                self.inbox
                    .try_claim(
                        &self.consumer_name,
                        &event.event_id,
                        &self.worker_id,
                        self.lease_duration,
                    )
                    .await?
            }
            None => return Err(NatsConsumerError::TenantRegistrationRequired),
        };

        match claim_result {
            InboxClaimResult::AlreadyProcessed => {
                self.metrics.duplicates.fetch_add(1, Ordering::Relaxed);
                message
                    .double_ack()
                    .await
                    .map_err(|error| NatsConsumerError::Broker(error.to_string()))?;
            }
            InboxClaimResult::Busy => {
                self.metrics.busy.fetch_add(1, Ordering::Relaxed);
                // Drop without ack. JetStream will redeliver after ack_wait,
                // giving the current inbox owner time to finish or renew.
            }
            InboxClaimResult::Acquired(claim) => {
                match self
                    .execute_handler_with_renewal(&claim, &event, &message)
                    .await
                {
                    Ok(()) => {
                        self.inbox.acknowledge(&claim).await?;
                        // Confirm broker acknowledgement after the database
                        // inbox commit. If this fails, redelivery is suppressed
                        // by the already-processed inbox row.
                        message
                            .double_ack()
                            .await
                            .map_err(|error| NatsConsumerError::Broker(error.to_string()))?;
                        self.metrics.processed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(HandlerExecutionError::Handler(error)) => {
                        self.inbox
                            .release(&claim, Some(&bounded_error(&error)))
                            .await?;
                        self.metrics
                            .handler_failures
                            .fetch_add(1, Ordering::Relaxed);
                        // Do not ack the broker message. Its normal redelivery
                        // timer controls retry cadence.
                    }
                    Err(HandlerExecutionError::Storage(error)) => {
                        return Err(NatsConsumerError::Storage(error));
                    }
                    Err(HandlerExecutionError::Broker(error)) => {
                        return Err(NatsConsumerError::Broker(error));
                    }
                }
            }
        }

        Ok(())
    }

    async fn execute_handler_with_renewal(
        &self,
        claim: &InboxClaim,
        event: &JetStreamEvent,
        message: &JetStreamMessage,
    ) -> Result<(), HandlerExecutionError> {
        let handler_future = self.handler.handle(event);
        tokio::pin!(handler_future);

        let renewal_millis = (self.lease_duration.as_millis() / 3).max(1) as u64;
        let mut renewal = tokio::time::interval(Duration::from_millis(renewal_millis));
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        renewal.tick().await;

        loop {
            tokio::select! {
                result = &mut handler_future => {
                    return result.map_err(HandlerExecutionError::Handler);
                }
                _ = renewal.tick() => {
                    self.inbox
                        .renew(claim, self.lease_duration)
                        .await
                        .map_err(HandlerExecutionError::Storage)?;
                    message
                        .ack_with(AckKind::Progress)
                        .await
                        .map_err(|error| HandlerExecutionError::Broker(error.to_string()))?;
                }
            }
        }
    }
}

fn legacy_default_tenant() -> String {
    "legacy-default".to_owned()
}

fn bounded_error(message: &str) -> String {
    if message.len() <= 4096 {
        return message.to_owned();
    }

    let mut end = 4096;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_owned()
}
