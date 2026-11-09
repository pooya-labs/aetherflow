use std::error::Error;
use std::fs;
use std::io::{self, Read};
use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;

use clap::{Parser, Subcommand};
use reqwest::{Client, Method, Url};
use serde_json::{Value, json};

const DEFAULT_URL: &str = "http://127.0.0.1:8080";
const MAX_HTTP_BODY_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(
    name = "aetherflow",
    version,
    about = "Inspect and operate AetherFlow through its versioned control-plane API",
    long_about = "AetherFlow CLI talks to the HTTP control plane. It never connects directly to PostgreSQL or starts workers."
)]
struct Cli {
    /// Control-plane base URL. Can also be set with AETHERFLOW_URL.
    #[arg(long, env = "AETHERFLOW_URL", default_value = DEFAULT_URL, global = true)]
    url: String,

    /// Use the authenticated tenant API and AETHERFLOW_TENANT_TOKEN.
    #[arg(long, global = true)]
    tenant: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Check public process liveness (does not check PostgreSQL).
    Health,
    /// List active worker registrations (operator-only).
    Workers,
    /// Inspect or submit workflow runs.
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    /// Inspect or cancel task runs.
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    /// Inspect a workflow run and its task summaries.
    Inspect { workflow_run_id: String },
    /// Submit a workflow-run JSON request from a file, or '-' for stdin.
    Submit {
        #[arg(short, long, value_name = "FILE")]
        file: std::path::PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum TaskCommand {
    /// Inspect task state and payload-size metadata.
    Inspect { task_run_id: String },
    /// Cancel a task using its current optimistic-concurrency version.
    Cancel {
        task_run_id: String,
        #[arg(long, value_name = "VERSION")]
        expected_version: u64,
    },
}

struct ApiClient {
    http: Client,
    base_url: Url,
    token: Option<String>,
    tenant: bool,
}

impl ApiClient {
    fn new(base_url: &str, token: Option<String>, tenant: bool) -> Result<Self, Box<dyn Error>> {
        let base_url = normalize_base_url(base_url)?;
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            http,
            base_url,
            token,
            tenant,
        })
    }

    fn endpoint(&self, route: &str) -> Result<Url, Box<dyn Error>> {
        let prefix = if self.tenant {
            "api/v1/tenant/"
        } else {
            "api/v1/"
        };
        Ok(self.base_url.join(&format!("{prefix}{route}"))?)
    }

    async fn request(
        &self,
        method: Method,
        route: &str,
        body: Option<Value>,
        authenticated: bool,
    ) -> Result<Value, Box<dyn Error>> {
        let url = if route == "health" {
            self.base_url.join("health")?
        } else {
            self.endpoint(route)?
        };
        let mut request = self.http.request(method, url);
        if authenticated {
            let token = self.token.as_deref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    if self.tenant {
                        "AETHERFLOW_TENANT_TOKEN is required for tenant commands"
                    } else {
                        "AETHERFLOW_API_TOKEN is required for operator commands"
                    },
                )
            })?;
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }

        let mut response = request.send().await?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_HTTP_BODY_BYTES as u64)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "control-plane response exceeds the 2 MiB HTTP body limit",
            )
            .into());
        }

        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            append_bounded_response_chunk(&mut bytes, &chunk)?;
        }
        if !status.is_success() {
            let parsed: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            let code = parsed.pointer("/error/code").and_then(Value::as_str);
            let message = parsed.pointer("/error/message").and_then(Value::as_str);
            let detail = match (code, message) {
                (Some(code), Some(message)) => format!("{code}: {message}"),
                _ => "the server returned an error response".to_owned(),
            };
            return Err(io::Error::other(format!("HTTP {status}: {detail}")).into());
        }

        if bytes.is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
}

fn append_bounded_response_chunk(response_body: &mut Vec<u8>, chunk: &[u8]) -> io::Result<()> {
    if response_body.len().saturating_add(chunk.len()) > MAX_HTTP_BODY_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "control-plane response exceeds the 2 MiB HTTP body limit",
        ));
    }
    response_body.extend_from_slice(chunk);
    Ok(())
}

fn normalize_base_url(raw: &str) -> Result<Url, Box<dyn Error>> {
    let mut value = raw.trim().to_owned();
    if value.is_empty() {
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "server URL cannot be empty").into(),
        );
    }
    if !value.ends_with('/') {
        value.push('/');
    }
    let url = Url::parse(&value)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "server URL must be an http(s) base URL without embedded credentials, query, or fragment",
        )
        .into());
    }

    // A bearer token must never be sent over cleartext HTTP to a remote host.
    // The server only permits plaintext HTTP on loopback, so the CLI mirrors
    // the same boundary before constructing an authenticated request.
    let host = url
        .host_str()
        .unwrap_or_default()
        .trim_start_matches('[')
        .trim_end_matches(']');
    let is_loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if url.scheme() == "http" && !is_loopback {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "remote control-plane URLs must use HTTPS; plain HTTP is allowed only on loopback",
        )
        .into());
    }

    Ok(url)
}

fn validate_identifier(value: &str) -> Result<(), Box<dyn Error>> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    if !valid {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "IDs must contain 1–128 ASCII letters, digits, underscores, or hyphens",
        )
        .into());
    }
    Ok(())
}

fn read_bounded(reader: impl Read) -> Result<String, Box<dyn Error>> {
    let mut limited = reader.take((MAX_HTTP_BODY_BYTES + 1) as u64);
    let mut raw = String::new();
    limited.read_to_string(&mut raw)?;
    if raw.len() > MAX_HTTP_BODY_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workflow request exceeds the 2 MiB HTTP body limit",
        )
        .into());
    }
    Ok(raw)
}

fn read_submission(path: &Path) -> Result<Value, Box<dyn Error>> {
    let raw = if path.as_os_str() == std::ffi::OsStr::new("-") {
        let stdin = io::stdin();
        read_bounded(stdin.lock())?
    } else {
        read_bounded(fs::File::open(path)?)?
    };
    let value: Value = serde_json::from_str(&raw)?;
    if !value.is_object() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workflow submission must be a JSON object",
        )
        .into());
    }
    Ok(value)
}

async fn run(cli: Cli) -> Result<(), Box<dyn Error>> {
    if cli.tenant && matches!(&cli.command, Command::Workers) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker inventory is an operator-only endpoint; remove --tenant",
        )
        .into());
    }

    let authenticated = !matches!(&cli.command, Command::Health);
    let token = if !authenticated {
        None
    } else if cli.tenant {
        Some(std::env::var("AETHERFLOW_TENANT_TOKEN").map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "AETHERFLOW_TENANT_TOKEN is required",
            )
        })?)
    } else {
        Some(std::env::var("AETHERFLOW_API_TOKEN").map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "AETHERFLOW_API_TOKEN is required",
            )
        })?)
    };

    let client = ApiClient::new(&cli.url, token, cli.tenant)?;
    let result = match cli.command {
        Command::Health => client.request(Method::GET, "health", None, false).await?,
        Command::Workers => client.request(Method::GET, "workers", None, true).await?,
        Command::Workflow { command } => match command {
            WorkflowCommand::Inspect { workflow_run_id } => {
                validate_identifier(&workflow_run_id)?;
                client
                    .request(
                        Method::GET,
                        &format!("workflow-runs/{workflow_run_id}"),
                        None,
                        true,
                    )
                    .await?
            }
            WorkflowCommand::Submit { file } => {
                let request = read_submission(&file)?;
                client
                    .request(Method::POST, "workflow-runs", Some(request), true)
                    .await?
            }
        },
        Command::Task { command } => match command {
            TaskCommand::Inspect { task_run_id } => {
                validate_identifier(&task_run_id)?;
                client
                    .request(Method::GET, &format!("task-runs/{task_run_id}"), None, true)
                    .await?
            }
            TaskCommand::Cancel {
                task_run_id,
                expected_version,
            } => {
                validate_identifier(&task_run_id)?;
                client
                    .request(
                        Method::POST,
                        &format!("task-runs/{task_run_id}/cancel"),
                        Some(json!({ "expected_version": expected_version })),
                        true,
                    )
                    .await?
            }
        },
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(error) = run(cli).await {
        eprintln!("aetherflow: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ApiClient, MAX_HTTP_BODY_BYTES, append_bounded_response_chunk, normalize_base_url,
        read_bounded, validate_identifier,
    };
    use std::io::Cursor;

    #[test]
    fn base_url_adds_trailing_slash_and_preserves_reverse_proxy_prefix() {
        let url = normalize_base_url("https://example.test/control").expect("valid URL");
        assert_eq!(url.as_str(), "https://example.test/control/");
    }

    #[test]
    fn base_url_rejects_credentials_queries_and_non_http_schemes() {
        assert!(normalize_base_url("https://user:secret@example.test").is_err());
        assert!(normalize_base_url("https://example.test?token=secret").is_err());
        assert!(normalize_base_url("file:///tmp/api").is_err());
    }

    #[test]
    fn plaintext_http_is_restricted_to_loopback_hosts() {
        assert!(normalize_base_url("http://127.0.0.1:8080").is_ok());
        assert!(normalize_base_url("http://127.42.0.9:8080").is_ok());
        assert!(normalize_base_url("http://localhost:8080").is_ok());
        assert!(normalize_base_url("http://[::1]:8080").is_ok());
        assert!(normalize_base_url("https://control.example.test").is_ok());
        assert!(normalize_base_url("http://control.example.test").is_err());
        assert!(normalize_base_url("http://192.168.1.10:8080").is_err());
        assert!(normalize_base_url("http://localhost.attacker.test").is_err());
    }

    #[test]
    fn bounded_reader_stops_at_the_http_body_limit() {
        let oversized = format!(r#"{{"input":"{}"}}"#, "x".repeat(MAX_HTTP_BODY_BYTES + 32));
        assert!(read_bounded(Cursor::new(oversized.into_bytes())).is_err());
        assert_eq!(
            read_bounded(Cursor::new(b"{\"ok\":true}".to_vec())).unwrap(),
            "{\"ok\":true}"
        );
    }

    #[test]
    fn response_body_reader_accepts_the_limit_and_rejects_overflow() {
        let mut response = vec![b'x'; MAX_HTTP_BODY_BYTES - 4];
        append_bounded_response_chunk(&mut response, b"1234").expect("limit is inclusive");
        assert_eq!(response.len(), MAX_HTTP_BODY_BYTES);

        assert!(append_bounded_response_chunk(&mut response, b"5").is_err());
        assert_eq!(response.len(), MAX_HTTP_BODY_BYTES);
    }

    #[test]
    fn identifiers_match_the_server_contract() {
        assert!(validate_identifier("run_01-abc").is_ok());
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("../admin").is_err());
        assert!(validate_identifier("a".repeat(129).as_str()).is_err());
        assert!(validate_identifier("سلام").is_err());
    }

    #[test]
    fn tenant_and_operator_routes_are_separate() {
        let operator = ApiClient::new("http://127.0.0.1:8080", None, false).unwrap();
        let tenant = ApiClient::new("http://127.0.0.1:8080", None, true).unwrap();
        assert_eq!(
            operator.endpoint("workflow-runs").unwrap().as_str(),
            "http://127.0.0.1:8080/api/v1/workflow-runs"
        );
        assert_eq!(
            tenant.endpoint("workflow-runs").unwrap().as_str(),
            "http://127.0.0.1:8080/api/v1/tenant/workflow-runs"
        );
    }
}
