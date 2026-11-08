//! Exercises the shared NATS client connector against a real TLS listener.
//!
//! The test requires a disposable NATS JetStream server with TLS enabled and
//! a CA that signs a certificate valid for 127.0.0.1. CI creates that fixture.

use std::env;

use aetherflow_transport_nats::{NatsConnectionConfig, NatsConnectionError};
use async_nats::{
    ServerAddr,
    jetstream::{self, stream::Config as StreamConfig},
};

const STREAM_NAME: &str = "AFLOW_TLS_CLIENT_TEST";
const SUBJECT: &str = "aetherflow.security.tls.probe";

fn secure_server_address() -> Result<ServerAddr, Box<dyn std::error::Error>> {
    let url = env::var("AETHERFLOW_SECURE_NATS_URL")?;
    Ok(url.parse()?)
}

#[tokio::test]
async fn secure_connector_verifies_ca_and_server_name_before_jetstream_use()
-> Result<(), Box<dyn std::error::Error>> {
    let config = NatsConnectionConfig::from_env()?;
    assert!(
        config.tls_required(),
        "the secure test fixture must require TLS"
    );

    let server = secure_server_address()?;
    assert!(server.tls_required(), "secure fixture URL must use tls://");

    let client = config.connect(vec![server]).await?;
    let jetstream = jetstream::new(client);

    jetstream
        .create_stream(StreamConfig {
            name: STREAM_NAME.to_owned(),
            subjects: vec![SUBJECT.to_owned()],
            ..Default::default()
        })
        .await?;
    let acknowledgement = jetstream
        .publish(SUBJECT.to_owned(), "verified-tls-client".into())
        .await?
        .await?;
    assert_eq!(acknowledgement.stream, STREAM_NAME);

    // The fixture certificate contains an IP SAN for 127.0.0.1 only. The
    // same trusted CA must not make a different DNS name valid.
    let wrong_host_url = env::var("AETHERFLOW_SECURE_NATS_WRONG_HOST_URL")?;
    let wrong_host = wrong_host_url.parse::<ServerAddr>()?;
    let result = config.connect(vec![wrong_host]).await;
    assert!(
        matches!(result, Err(NatsConnectionError::Connect(_))),
        "the connector must reject a trusted certificate presented for another hostname"
    );

    Ok(())
}
