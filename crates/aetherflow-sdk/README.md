# AetherFlow Rust SDK

The SDK is a typed asynchronous HTTP client for the v1 control-plane contract. It does not connect to PostgreSQL, run migrations, or start workers.

## Authentication scopes

- `ControlPlaneClient::anonymous` can call the public health endpoint.
- Operator clients use the platform Bearer token and can list active workers.
- Tenant clients use a tenant-bound Bearer credential and can submit/read workflows and read/cancel their own tasks.

Tokens are supplied at construction, stored only in the client, excluded from Debug output, and never placed in URLs. Redirects are disabled. Remote plain HTTP is rejected; HTTPS uses the platform trust configuration. Local HTTP is permitted only for loopback development.

## Example

~~~rust
use aetherflow_sdk::{
    ControlPlaneClient, RetryPolicy, WorkflowRunSubmission, WorkflowTaskInput,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("AETHERFLOW_API_TOKEN")?;
    let client = ControlPlaneClient::operator("http://127.0.0.1:8080", token)?;

    let submission = WorkflowRunSubmission::new(
        "image-pipeline",
        "image-run-0001",
        vec![
            WorkflowTaskInput::new("decode"),
            WorkflowTaskInput::new("resize")
                .with_dependencies(vec!["decode".to_owned()]),
        ],
        RetryPolicy::new(3, 100, 2_000)?,
    );

    let receipt = client.submit_workflow_run(&submission).await?;
    println!("created workflow {}", receipt.workflow_run_id);

    let current = client.get_workflow_run(&receipt.workflow_run_id).await?;
    println!("workflow status: {:?}", current.status);
    Ok(())
}
~~~

Use `ControlPlaneClient::anonymous` for process liveness or `ControlPlaneClient::tenant` with `AETHERFLOW_TENANT_TOKEN` for tenant-owned routes. Task input is limited to 1 MiB after decoding and HTTP bodies are bounded to 2 MiB. Cancellation requires the version from the latest task snapshot to preserve optimistic concurrency.
