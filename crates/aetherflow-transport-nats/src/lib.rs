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
#[derive(Clone)]
pub struct NatsJetStreamPublisher {
    context: Context,
    subject: String,
}

impl NatsJetStreamPublisher {
    /// Creates a publisher for a subject already captured by a JetStream stream.
    #[must_use]
    pub fn new(context: Context, subject: impl Into<String>) -> Self {
        Self {
            context,
            subject: subject.into(),
        }
    }

    /// Returns the configured publication subject.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }
}

impl OutboxPublisher for NatsJetStreamPublisher {
    fn publish<'a>(&'a self, event: &'a OutboxClaim) -> OutboxPublishFuture<'a> {
        let context = self.context.clone();
        let subject = self.subject.clone();
        Box::pin(async move {
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::OutboxEnvelope;
    use serde_json::json;

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
