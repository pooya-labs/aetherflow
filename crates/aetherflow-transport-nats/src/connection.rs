//! Environment-backed NATS client security configuration.
//!
//! Local Compose uses plaintext NATS on loopback. External deployments should
//! provide a credentials file and require TLS, with an explicit CA or client
//! certificate when their NATS deployment needs private trust or mTLS.

use std::env;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;

use async_nats::{Client, ConnectOptions, ServerAddr};

/// Supported environment variables for NATS client connections.
const TLS_REQUIRED: &str = "AETHERFLOW_NATS_TLS_REQUIRED";
const CREDENTIALS_FILE: &str = "AETHERFLOW_NATS_CREDENTIALS_FILE";
const CA_FILE: &str = "AETHERFLOW_NATS_CA_FILE";
const CLIENT_CERT_FILE: &str = "AETHERFLOW_NATS_CLIENT_CERT_FILE";
const CLIENT_KEY_FILE: &str = "AETHERFLOW_NATS_CLIENT_KEY_FILE";

/// Validated security options shared by AetherFlow's NATS processes.
///
/// The default is intentionally suitable only for the loopback-only local
/// Compose cluster. Credentials, custom CA roots, or a client certificate
/// automatically require TLS unless the configuration is invalidly
/// overridden to plaintext.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NatsConnectionConfig {
    tls_required: bool,
    credentials_file: Option<PathBuf>,
    ca_file: Option<PathBuf>,
    client_certificate_file: Option<PathBuf>,
    client_key_file: Option<PathBuf>,
}

/// Invalid NATS client connection configuration or connection failure.
#[derive(Debug)]
pub enum NatsConnectionError {
    /// A boolean environment variable did not contain `true` or `false`.
    InvalidBoolean {
        /// Environment variable name.
        name: &'static str,
    },
    /// Only one of the client certificate and key paths was supplied.
    IncompleteClientCertificate,
    /// Credentials or TLS files were configured while TLS was disabled.
    TlsRequired {
        /// Setting which requires TLS.
        setting: &'static str,
    },
    /// The credentials file could not be read or parsed.
    CredentialsFile(std::io::Error),
    /// The client could not connect to any configured NATS server.
    Connect(async_nats::ConnectError),
}

impl fmt::Display for NatsConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBoolean { name } => {
                write!(formatter, "{name} must be either true or false")
            }
            Self::IncompleteClientCertificate => formatter.write_str(
                "AETHERFLOW_NATS_CLIENT_CERT_FILE and AETHERFLOW_NATS_CLIENT_KEY_FILE must be set together",
            ),
            Self::TlsRequired { setting } => {
                write!(formatter, "{setting} requires AETHERFLOW_NATS_TLS_REQUIRED=true")
            }
            Self::CredentialsFile(_) => {
                formatter.write_str("could not load the configured NATS credentials file")
            }
            Self::Connect(_) => formatter.write_str("could not connect to the configured NATS servers"),
        }
    }
}

impl Error for NatsConnectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CredentialsFile(error) => Some(error),
            Self::Connect(error) => Some(error),
            _ => None,
        }
    }
}

impl NatsConnectionConfig {
    /// Reads and validates NATS transport settings from the process environment.
    ///
    /// Supported variables:
    ///
    /// - `AETHERFLOW_NATS_TLS_REQUIRED`: optional `true`/`false`; defaults to
    ///   true whenever credentials or TLS files are configured.
    /// - `AETHERFLOW_NATS_CREDENTIALS_FILE`: NATS `.creds` file (JWT + user
    ///   NKey seed). The file is loaded when a connection is created.
    /// - `AETHERFLOW_NATS_CA_FILE`: PEM CA certificate bundle for server trust.
    /// - `AETHERFLOW_NATS_CLIENT_CERT_FILE` and
    ///   `AETHERFLOW_NATS_CLIENT_KEY_FILE`: optional client mTLS identity pair.
    ///
    /// With no settings, plaintext remains enabled for the loopback-only local
    /// Compose environment. Do not use that default for an externally reachable
    /// deployment.
    pub fn from_env() -> Result<Self, NatsConnectionError> {
        let credentials_file = env_path(CREDENTIALS_FILE);
        let ca_file = env_path(CA_FILE);
        let client_certificate_file = env_path(CLIENT_CERT_FILE);
        let client_key_file = env_path(CLIENT_KEY_FILE);
        let explicit_tls = env_bool(TLS_REQUIRED)?;

        let security_material_configured = credentials_file.is_some()
            || ca_file.is_some()
            || client_certificate_file.is_some()
            || client_key_file.is_some();

        let config = Self {
            tls_required: explicit_tls.unwrap_or(security_material_configured),
            credentials_file,
            ca_file,
            client_certificate_file,
            client_key_file,
        };
        config.validate()?;
        Ok(config)
    }

    /// Returns whether this configuration requires TLS for the NATS connection.
    #[must_use]
    pub const fn tls_required(&self) -> bool {
        self.tls_required
    }

    /// Connects to the supplied NATS seed addresses with the validated settings.
    ///
    /// Credentials are loaded once when this method is called. To rotate a
    /// credentials file, create a new connection from a freshly loaded
    /// configuration and drain the old client; this helper does not watch files
    /// or silently reload secrets in the background.
    pub async fn connect(&self, servers: Vec<ServerAddr>) -> Result<Client, NatsConnectionError> {
        self.validate()?;

        let mut options = ConnectOptions::new().require_tls(self.tls_required);

        if let Some(path) = &self.credentials_file {
            options = options
                .credentials_file(path)
                .await
                .map_err(NatsConnectionError::CredentialsFile)?;
        }
        if let Some(path) = &self.ca_file {
            options = options.add_root_certificates(path.clone());
        }
        if let (Some(certificate), Some(key)) =
            (&self.client_certificate_file, &self.client_key_file)
        {
            options = options.add_client_certificate(certificate.clone(), key.clone());
        }

        options
            .connect(servers)
            .await
            .map_err(NatsConnectionError::Connect)
    }

    fn validate(&self) -> Result<(), NatsConnectionError> {
        if self.client_certificate_file.is_some() != self.client_key_file.is_some() {
            return Err(NatsConnectionError::IncompleteClientCertificate);
        }

        for (setting, is_set) in [
            (CREDENTIALS_FILE, self.credentials_file.is_some()),
            (CA_FILE, self.ca_file.is_some()),
            (CLIENT_CERT_FILE, self.client_certificate_file.is_some()),
            (CLIENT_KEY_FILE, self.client_key_file.is_some()),
        ] {
            if is_set && !self.tls_required {
                return Err(NatsConnectionError::TlsRequired { setting });
            }
        }

        Ok(())
    }
}

fn env_path(name: &'static str) -> Option<PathBuf> {
    env::var_os(name).and_then(|value| {
        if value.is_empty() {
            None
        } else {
            Some(PathBuf::from(value))
        }
    })
}

fn env_bool(name: &'static str) -> Result<Option<bool>, NatsConnectionError> {
    match env::var(name) {
        Ok(value) => parse_bool(name, &value).map(Some),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(NatsConnectionError::InvalidBoolean { name }),
    }
}

fn parse_bool(name: &'static str, value: &str) -> Result<bool, NatsConnectionError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(NatsConnectionError::InvalidBoolean { name }),
    }
}

#[cfg(test)]
mod tests {
    use super::{NatsConnectionConfig, NatsConnectionError};
    use std::path::PathBuf;

    #[test]
    fn tls_environment_boolean_accepts_only_explicit_true_or_false() {
        assert!(super::parse_bool("AETHERFLOW_NATS_TLS_REQUIRED", "true").expect("true"));
        assert!(!super::parse_bool("AETHERFLOW_NATS_TLS_REQUIRED", " false ").expect("false"));
        assert!(matches!(
            super::parse_bool("AETHERFLOW_NATS_TLS_REQUIRED", "yes"),
            Err(NatsConnectionError::InvalidBoolean {
                name: "AETHERFLOW_NATS_TLS_REQUIRED"
            })
        ));
    }

    #[test]
    fn local_default_keeps_loopback_compose_compatible() {
        let config = NatsConnectionConfig::default();

        assert!(!config.tls_required());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn credentials_require_tls() {
        let config = NatsConnectionConfig {
            credentials_file: Some(PathBuf::from("/run/secrets/nats.creds")),
            ..NatsConnectionConfig::default()
        };

        assert!(matches!(
            config.validate(),
            Err(NatsConnectionError::TlsRequired {
                setting: "AETHERFLOW_NATS_CREDENTIALS_FILE"
            })
        ));
    }

    #[test]
    fn custom_ca_requires_tls() {
        let config = NatsConnectionConfig {
            ca_file: Some(PathBuf::from("/run/secrets/nats-ca.pem")),
            ..NatsConnectionConfig::default()
        };

        assert!(matches!(
            config.validate(),
            Err(NatsConnectionError::TlsRequired {
                setting: "AETHERFLOW_NATS_CA_FILE"
            })
        ));
    }

    #[test]
    fn client_mtls_requires_both_certificate_and_key() {
        let config = NatsConnectionConfig {
            tls_required: true,
            client_certificate_file: Some(PathBuf::from("/run/secrets/client.pem")),
            ..NatsConnectionConfig::default()
        };

        assert!(matches!(
            config.validate(),
            Err(NatsConnectionError::IncompleteClientCertificate)
        ));
    }

    #[test]
    fn complete_mtls_configuration_requires_tls_and_is_accepted() {
        let config = NatsConnectionConfig {
            tls_required: true,
            client_certificate_file: Some(PathBuf::from("/run/secrets/client.pem")),
            client_key_file: Some(PathBuf::from("/run/secrets/client-key.pem")),
            ..NatsConnectionConfig::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn tls_without_custom_credentials_or_mtls_is_valid() {
        let config = NatsConnectionConfig {
            tls_required: true,
            ..NatsConnectionConfig::default()
        };

        assert!(config.validate().is_ok());
    }
}
