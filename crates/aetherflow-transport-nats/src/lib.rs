//! NATS JetStream publishing adapters for AetherFlow's durable outbox.
//!
//! The publisher resolves only after JetStream confirms that the message was
//! stored. A stable `Nats-Msg-Id` header lets JetStream suppress duplicate
//! publications within the stream's configured duplicate window. Consumers
//! must still use the durable PostgreSQL inbox for longer-lived deduplication.

mod connection;
mod consumer;

pub use connection::{NatsConnectionConfig, NatsConnectionError};

pub use consumer::{
    ConsumerMetrics, ConsumerMetricsSnapshot, EventHandlerFuture, JetStreamEvent,
    JetStreamEventHandler, NatsConsumerError, NatsJetStreamConsumer,
};

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use aetherflow_storage_postgres::{
    OutboxClaim, OutboxPublishError, OutboxPublishFuture, OutboxPublisher,
};
use async_nats::jetstream::Context;
use serde::Serialize;

/// JetStream envelope format published by AetherFlow.
#[derive(Serialize)]
struct OutboxEnvelope<'a> {
    schema_version: u16,
    tenant_id: &'a str,
    event_id: String,
    aggregate_type: &'a str,
    aggregate_id: &'a str,
    aggregate_version: u64,
    event_type: &'a str,
    payload: &'a serde_json::Value,
}

/// Publishes AetherFlow outbox events to a configured JetStream subject.
///
/// The subject must be captured by an existing JetStream stream. Stream
/// provisioning, secret delivery, and server-side account/ACL policy remain
/// deployment responsibilities. Use `NatsConnectionConfig` to establish the
/// client connection with the configured credentials and TLS policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PublisherMetricsSnapshot {
    /// Publish futures that began executing.
    pub attempts_started: usize,
    /// Attempts confirmed as stored by JetStream.
    pub confirmed: usize,
    /// Attempts that returned an encoding, send, or acknowledgement error.
    pub failed: usize,
    /// Started futures dropped before a final result was observed; broker outcome may be unknown.
    pub interrupted: usize,
    /// Publish futures currently executing.
    pub in_flight: usize,
}

#[derive(Default)]
struct PublisherMetricsInner {
    attempts_started: AtomicUsize,
    confirmed: AtomicUsize,
    failed: AtomicUsize,
    interrupted: AtomicUsize,
    in_flight: AtomicUsize,
}

/// Process-local counters shared by clones of one JetStream publisher.
#[derive(Clone, Default)]
pub struct PublisherMetrics {
    inner: Arc<PublisherMetricsInner>,
}

impl PublisherMetrics {
    /// Returns a point-in-time snapshot of this publisher instance's counters.
    #[must_use]
    pub fn snapshot(&self) -> PublisherMetricsSnapshot {
        PublisherMetricsSnapshot {
            attempts_started: self.inner.attempts_started.load(Ordering::Relaxed),
            confirmed: self.inner.confirmed.load(Ordering::Relaxed),
            failed: self.inner.failed.load(Ordering::Relaxed),
            interrupted: self.inner.interrupted.load(Ordering::Relaxed),
            in_flight: self.inner.in_flight.load(Ordering::Relaxed),
        }
    }
}

struct PublishAttempt {
    metrics: PublisherMetrics,
    finished: bool,
}

impl PublishAttempt {
    fn start(metrics: PublisherMetrics) -> Self {
        metrics.inner.attempts_started.fetch_add(1, Ordering::Relaxed);
        metrics.inner.in_flight.fetch_add(1, Ordering::Relaxed);
        Self {
            metrics,
            finished: false,
        }
    }

    fn finish(mut self, succeeded: bool) {
        self.metrics.inner.in_flight.fetch_sub(1, Ordering::Relaxed);
        let counter = if succeeded {
            &self.metrics.inner.confirmed
        } else {
            &self.metrics.inner.failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.finished = true;
    }
}

impl Drop for PublishAttempt {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics.inner.in_flight.fetch_sub(1, Ordering::Relaxed);
            self.metrics.inner.interrupted.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Publishes AetherFlow outbox events to a configured JetStream subject.
///
/// The subject must be captured by an existing JetStream stream. Stream
/// provisioning, secret delivery, and server-side account/ACL policy remain
/// deployment responsibilities. Use `NatsConnectionConfig` to establish the
/// client connection with the configured credentials and TLS policy.
///
//// Metrics are process-local snapshots shared by publisher clones. They are
/// not a cross-replica aggregate or an HTTP/Prometheus endpoint.
#[derive(Clone)]
pub struct NatsJetStreamPublisher {
    context: Context,
    subject: String,
    metrics: PublisherMetrics,
}

impl NatsJetStreamPublisher {
    /// Creates a publisher for a subject already captured by a JetStream stream.
    #[must_use]
    pub fn new(context: Context, subject: impl Into<String>) -> Self {
        Self {
            context,
            subject: subject.into(),
            metrics: PublisherMetrics::default(),
        }
    }

    /// Returns the configured publication subject.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns a handle for observing this publisher instance's counters.
    #[must_use]
    pub fn metrics(&self) -> PublisherMetrics {
        self.metrics.clone()
    }
}

impl OutboxPublisher for NatsJetStreamPublisher {
    fn publish<'a>(&'a self, event: &'a OutboxClaim) -> OutboxPublishFuture<'a> {
        let context = self.context.clone();
        let subject = self.subject.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let attempt = PublishAttempt::start(metrics);
            let result: Result<(), OutboxPublishError> = async {
                let envelope = OutboxEnvelope {
                    schema_version: 1,
                    tenant_id: event.tenant_id(),
                    event_id: event.event_id().to_string(),
                    aggregate_type: event.aggregate_type(),
                    aggregate_id: event.aggregate_id(),
                    aggregate_version: event.aggregate_version(),
                    event_type: event.event_type(),
                    payload: event.payload(),
                };
                let payload = serde_json::to_vec(&envelope).map_err(|error| {
                    OutboxPublishError::new(format!("encode outbox event: {error}"))
                })?;

                let mut headers = async_nats::HeaderMap::new();
                headers.append("Nats-Msg-Id", event.event_id().to_string());
                headers.append("Aetherflow-Tenant-Id", event.tenant_id());
                headers.append("Aetherflow-Event-Type", event.event_type());

                // JetStream has a two-stage API: the first await sends the request;
                // the returned future resolves only once the server confirms storage.
                let acknowledgement = context
                    .publish_with_headers(subject, headers, payload.into())
                    .await
                    .map_err(|error| {
                        OutboxPublishError::new(format!("send JetStream publish: {error}"))
                    })?;
                acknowledgement.await.map_err(|error| {
                    OutboxPublishError::new(format!("JetStream publish acknowledgement: {error}"))
                })?;

                Ok(())
            }
            .await;
            attempt.finish(result.is_ok());
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::OutboxEnvelope;
    use serde_json::json;

    #[test]
    fn publisher_metrics_track_confirmed_failed_and_interrupted_attempts() {
        use super::{PublishAttempt, PublisherMetrics};

        let metrics = PublisherMetrics::default();
        let attempt = PublishAttempt::start(metrics.clone());
        assert_eq!(metrics.snapshot().attempts_started, 1);
        assert_eq!(metrics.snapshot().in_flight, 1);
        attempt.finish(true);

        let attempt = PublishAttempt::start(metrics.clone());
        attempt.finish(false);

        drop(PublishAttempt::start(metrics.clone()));
        assert_eq!(
            metrics.snapshot(),
            super::PublisherMetricsSnapshot {
                attempts_started: 3,
                confirmed: 1,
                failed: 1,
                interrupted: 1,
                in_flight: 0,
            }
        );
    }

    #[test]
    fn outbox_envelope_carries_stable_identity_and_schema_version() {
        let payload = json!({"task_id": "task-1"});
        let envelope = OutboxEnvelope {
            schema_version: 1,
            tenant_id: "legacy-default",
            event_id: "42".to_owned(),
            aggregate_type: "workflow_run",
            aggregate_id: "workflow-1",
            aggregate_version: 3,
            event_type: "workflow.started",
            payload: &payload,
        };

        let encoded = serde_json::to_value(envelope).expect("serialize envelope");
        assert_eq!(encoded["schema_version"], 1);
        assert_eq!(encoded["tenant_id"], "legacy-default");
        assert_eq!(encoded["event_id"], "42");
        assert_eq!(encoded["aggregate_version"], 3);
        assert_eq!(encoded["payload"]["task_id"], "task-1");
    }
}
