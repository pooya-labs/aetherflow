//! Foundational, infrastructure-independent primitives for AetherFlow.
//!
//! The core crate must remain free of network, database, and runtime adapters.

/// The product version compiled into this crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// A stable, validated identifier for an entity in the AetherFlow domain.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EntityId(String);

/// An error returned when an entity identifier is invalid.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidEntityId {
    reason: &'static str,
}

impl InvalidEntityId {
    /// Returns the validation failure reason.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        self.reason
    }
}

impl std::fmt::Display for InvalidEntityId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.reason)
    }
}

impl std::error::Error for InvalidEntityId {}

impl EntityId {
    /// Creates an identifier from a non-empty, bounded ASCII token.
    ///
    /// Allowed characters are ASCII letters, digits, hyphens, and underscores.
    /// This deliberately does not generate IDs; generation policy belongs to
    /// the application layer and will be specified in a later milestone.
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidEntityId> {
        let value = value.into();

        if value.is_empty() {
            return Err(InvalidEntityId {
                reason: "entity identifier must not be empty",
            });
        }

        if value.len() > 128 {
            return Err(InvalidEntityId {
                reason: "entity identifier must be at most 128 bytes",
            });
        }

        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(InvalidEntityId {
                reason: "entity identifier contains unsupported characters",
            });
        }

        Ok(Self(value))
    }

    /// Returns the identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::EntityId;

    #[test]
    fn accepts_ascii_identifier() {
        let id = EntityId::parse("run_01-alpha").expect("valid identifier");
        assert_eq!(id.as_str(), "run_01-alpha");
    }

    #[test]
    fn rejects_empty_identifier() {
        assert!(EntityId::parse("").is_err());
    }

    #[test]
    fn rejects_identifier_longer_than_limit() {
        assert!(EntityId::parse("a".repeat(129)).is_err());
    }

    #[test]
    fn rejects_whitespace_and_path_separators() {
        assert!(EntityId::parse("run / 01").is_err());
        assert!(EntityId::parse("../run").is_err());
    }
}
