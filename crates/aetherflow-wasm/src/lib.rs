//! Versioned plugin ABI and default-deny capability contracts.
//!
//! This crate intentionally does not expose Wasmtime or WASI types. The ABI
//! stays independent from the engine so plugins and host policy can evolve
//! without leaking engine implementation details into the rest of AetherFlow.

use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

use aetherflow_core::EntityId;

/// Current host-supported plugin ABI version.
pub const PLUGIN_ABI_V1: PluginAbiVersion = PluginAbiVersion { major: 1, minor: 0 };

/// Maximum input or output payload accepted by the v1 ABI (1 MiB).
pub const MAX_PLUGIN_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Maximum original component artifact accepted by the host (16 MiB).
pub const MAX_PLUGIN_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;

/// Maximum guest-provided log message accepted by the v1 host interface (4 KiB).
pub const MAX_LOG_MESSAGE_BYTES: usize = 4 * 1024;

/// Maximum host log calls permitted during one plugin invocation.
pub const MAX_PLUGIN_LOG_MESSAGES: u32 = 256;

/// Maximum linear memory permitted for one plugin invocation (64 MiB).
pub const MAX_PLUGIN_MEMORY_BYTES: usize = 64 * 1024 * 1024;

/// Maximum core WebAssembly instances permitted in one plugin Store.
pub const MAX_PLUGIN_CORE_INSTANCES: usize = 16;

/// Maximum fuel budget permitted for one plugin invocation.
pub const MAX_PLUGIN_FUEL: u64 = 100_000_000;

/// Maximum guest-to-host component call transfer budget (16 KiB per call).
pub const MAX_PLUGIN_HOSTCALL_FUEL: usize = 16 * 1024;

/// Maximum simultaneously-live component handles in one Store.
pub const MAX_PLUGIN_COMPONENT_HANDLES: usize = 10_000;

/// Maximum wall-clock execution budget for one plugin invocation.
pub const MAX_PLUGIN_EXECUTION_TIMEOUT: Duration = Duration::from_secs(10);

/// A semantic version for the host/guest plugin contract.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PluginAbiVersion {
    /// Breaking interface generation.
    pub major: u16,
    /// Backwards-compatible additions within a major generation.
    pub minor: u16,
}

impl PluginAbiVersion {
    /// Returns whether this host can execute a plugin requiring `required`.
    ///
    /// A host accepts an older or equal minor version of the same major ABI.
    #[must_use]
    pub const fn supports(self, required: Self) -> bool {
        self.major == required.major && self.minor >= required.minor
    }
}

/// A named host capability that a plugin may request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Capability {
    /// Write bounded diagnostic messages through the host logger.
    LogWrite,
}

impl Capability {
    /// Returns the stable manifest/WIT capability identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LogWrite => "log.write",
        }
    }
}

/// Explicit allow-list of capabilities granted to one plugin invocation.
///
/// The empty set is the default. No capability is ambient or inherited.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CapabilitySet(BTreeSet<Capability>);

impl CapabilitySet {
    /// Creates a capability set from an explicit iterator.
    #[must_use]
    pub fn from_capabilities(capabilities: impl IntoIterator<Item = Capability>) -> Self {
        Self(capabilities.into_iter().collect())
    }

    /// Grants one capability to this set.
    pub fn grant(&mut self, capability: Capability) {
        self.0.insert(capability);
    }

    /// Returns whether the capability was explicitly granted.
    #[must_use]
    pub fn allows(&self, capability: Capability) -> bool {
        self.0.contains(&capability)
    }

    /// Returns the number of explicitly granted capabilities.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no capabilities were granted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates over granted capabilities in stable identifier order.
    pub fn iter(&self) -> impl Iterator<Item = Capability> + '_ {
        self.0.iter().copied()
    }
}

/// Host-provided execution metadata exposed to a plugin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginExecutionContext {
    /// Stable workflow run identifier.
    workflow_run_id: EntityId,
    /// Stable task run identifier.
    task_run_id: EntityId,
    /// One-based durable attempt number.
    attempt: u32,
    /// Optional Unix timestamp deadline in milliseconds.
    deadline_unix_ms: Option<u64>,
}

impl PluginExecutionContext {
    /// Creates a context, rejecting attempt zero.
    pub fn new(
        workflow_run_id: EntityId,
        task_run_id: EntityId,
        attempt: u32,
        deadline_unix_ms: Option<u64>,
    ) -> Result<Self, ContextError> {
        if attempt == 0 {
            return Err(ContextError::ZeroAttempt);
        }

        Ok(Self {
            workflow_run_id,
            task_run_id,
            attempt,
            deadline_unix_ms,
        })
    }

    /// Returns the workflow execution identifier.
    #[must_use]
    pub fn workflow_run_id(&self) -> &EntityId {
        &self.workflow_run_id
    }

    /// Returns the task execution identifier.
    #[must_use]
    pub fn task_run_id(&self) -> &EntityId {
        &self.task_run_id
    }

    /// Returns the one-based durable attempt number.
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Returns the optional Unix timestamp deadline in milliseconds.
    #[must_use]
    pub const fn deadline_unix_ms(&self) -> Option<u64> {
        self.deadline_unix_ms
    }
}

/// A malformed host-provided plugin context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextError {
    /// Durable attempts are one-based.
    ZeroAttempt,
}

impl fmt::Display for ContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroAttempt => formatter.write_str("plugin attempt must be one-based"),
        }
    }
}

impl std::error::Error for ContextError {}

/// Resource budgets enforced by the host for a single plugin invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionLimits {
    /// Maximum guest input payload size.
    pub max_input_bytes: usize,
    /// Maximum guest output payload size.
    pub max_output_bytes: usize,
    /// Maximum linear memory size.
    pub max_memory_bytes: usize,
    /// Maximum number of core WebAssembly instances in one Store.
    pub max_core_instances: usize,
    /// Maximum number of table elements per table.
    pub max_table_elements: usize,
    /// Wasmtime instruction fuel budget.
    pub fuel: u64,
    /// Maximum bytes a guest may transfer to the host in one component call.
    pub hostcall_fuel: usize,
    /// Maximum simultaneously-live component-model handles.
    pub max_component_handles: usize,
    /// Wall-clock deadline; the engine must actively interrupt execution.
    pub wall_clock_timeout: Duration,
    /// Maximum UTF-8 log message size.
    pub max_log_message_bytes: usize,
    /// Maximum number of host log calls during one invocation.
    pub max_log_messages: u32,
}

impl Default for ExecutionLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: MAX_PLUGIN_PAYLOAD_BYTES,
            max_output_bytes: MAX_PLUGIN_PAYLOAD_BYTES,
            max_memory_bytes: 16 * 1024 * 1024,
            max_core_instances: 4,
            max_table_elements: 1_024,
            fuel: 10_000_000,
            hostcall_fuel: 4 * 1024,
            max_component_handles: 1_024,
            wall_clock_timeout: Duration::from_secs(2),
            max_log_message_bytes: MAX_LOG_MESSAGE_BYTES,
            max_log_messages: 64,
        }
    }
}

impl ExecutionLimits {
    /// Validates that a caller-selected budget stays within v1 hard ceilings.
    pub fn validate(self) -> Result<(), LimitConfigurationError> {
        if self.max_input_bytes == 0 || self.max_input_bytes > MAX_PLUGIN_PAYLOAD_BYTES {
            return Err(LimitConfigurationError::InputLimitOutOfRange);
        }
        if self.max_output_bytes == 0 || self.max_output_bytes > MAX_PLUGIN_PAYLOAD_BYTES {
            return Err(LimitConfigurationError::OutputLimitOutOfRange);
        }
        if self.max_memory_bytes == 0 || self.max_memory_bytes > MAX_PLUGIN_MEMORY_BYTES {
            return Err(LimitConfigurationError::MemoryLimitOutOfRange);
        }
        if self.max_core_instances == 0
            || self.max_core_instances > MAX_PLUGIN_CORE_INSTANCES
        {
            return Err(LimitConfigurationError::CoreInstancesOutOfRange);
        }
        if self.max_table_elements == 0 || self.max_table_elements > 10_000 {
            return Err(LimitConfigurationError::TableLimitOutOfRange);
        }
        if self.fuel == 0 || self.fuel > MAX_PLUGIN_FUEL {
            return Err(LimitConfigurationError::FuelOutOfRange);
        }
        if self.hostcall_fuel == 0 || self.hostcall_fuel > MAX_PLUGIN_HOSTCALL_FUEL {
            return Err(LimitConfigurationError::HostcallFuelOutOfRange);
        }
        if self.max_component_handles == 0
            || self.max_component_handles > MAX_PLUGIN_COMPONENT_HANDLES
        {
            return Err(LimitConfigurationError::ComponentHandlesOutOfRange);
        }
        if self.wall_clock_timeout.is_zero()
            || self.wall_clock_timeout > MAX_PLUGIN_EXECUTION_TIMEOUT
        {
            return Err(LimitConfigurationError::TimeoutOutOfRange);
        }
        if self.max_log_message_bytes == 0 || self.max_log_message_bytes > MAX_LOG_MESSAGE_BYTES {
            return Err(LimitConfigurationError::LogLimitOutOfRange);
        }
        if self.max_log_messages == 0 || self.max_log_messages > MAX_PLUGIN_LOG_MESSAGES {
            return Err(LimitConfigurationError::LogRateLimitOutOfRange);
        }

        Ok(())
    }

    /// Validates guest input before crossing the component boundary.
    pub fn validate_input(self, input: &[u8]) -> Result<(), PayloadTooLarge> {
        validate_payload(input, self.max_input_bytes, PayloadDirection::Input)
    }

    /// Validates guest output immediately after it returns to the host.
    pub fn validate_output(self, output: &[u8]) -> Result<(), PayloadTooLarge> {
        validate_payload(output, self.max_output_bytes, PayloadDirection::Output)
    }
}

fn validate_payload(
    payload: &[u8],
    maximum: usize,
    direction: PayloadDirection,
) -> Result<(), PayloadTooLarge> {
    if payload.len() > maximum {
        return Err(PayloadTooLarge {
            direction,
            actual: payload.len(),
            maximum,
        });
    }

    Ok(())
}

/// The direction of a payload crossing the plugin boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadDirection {
    /// Host-to-guest input.
    Input,
    /// Guest-to-host output.
    Output,
}

/// A plugin payload exceeded its configured byte limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PayloadTooLarge {
    /// Which side of the ABI boundary exceeded its limit.
    pub direction: PayloadDirection,
    /// Actual payload size in bytes.
    pub actual: usize,
    /// Maximum permitted payload size in bytes.
    pub maximum: usize,
}

impl fmt::Display for PayloadTooLarge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "plugin {:?} payload is {} bytes; maximum is {} bytes",
            self.direction, self.actual, self.maximum
        )
    }
}

impl std::error::Error for PayloadTooLarge {}

/// A caller-supplied resource budget exceeded the v1 hard ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LimitConfigurationError {
    /// Input limit must be between 1 byte and 1 MiB.
    InputLimitOutOfRange,
    /// Output limit must be between 1 byte and 1 MiB.
    OutputLimitOutOfRange,
    /// Memory limit must be between 1 byte and 64 MiB.
    MemoryLimitOutOfRange,
    /// Core instance count must be between 1 and 16.
    CoreInstancesOutOfRange,
    /// Table limit must be between 1 and 10,000 elements.
    TableLimitOutOfRange,
    /// Fuel must be between 1 and 100,000,000 units.
    FuelOutOfRange,
    /// Host-call transfer fuel must be between 1 byte and 16 KiB.
    HostcallFuelOutOfRange,
    /// Component handle count must be between 1 and 10,000.
    ComponentHandlesOutOfRange,
    /// Wall-clock timeout must be greater than zero and at most 10 seconds.
    TimeoutOutOfRange,
    /// Log message limit must be between 1 byte and 4 KiB.
    LogLimitOutOfRange,
    /// Log call count must be between 1 and 256 per invocation.
    LogRateLimitOutOfRange,
}

impl fmt::Display for LimitConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InputLimitOutOfRange => "plugin input limit is outside v1 bounds",
            Self::OutputLimitOutOfRange => "plugin output limit is outside v1 bounds",
            Self::MemoryLimitOutOfRange => "plugin memory limit is outside v1 bounds",
            Self::CoreInstancesOutOfRange => "plugin core instance limit is outside v1 bounds",
            Self::TableLimitOutOfRange => "plugin table limit is outside v1 bounds",
            Self::FuelOutOfRange => "plugin fuel budget is outside v1 bounds",
            Self::HostcallFuelOutOfRange => "plugin host-call fuel is outside v1 bounds",
            Self::ComponentHandlesOutOfRange => {
                "plugin component handle limit is outside v1 bounds"
            }
            Self::TimeoutOutOfRange => "plugin wall-clock timeout is outside v1 bounds",
            Self::LogLimitOutOfRange => "plugin log message limit is outside v1 bounds",
            Self::LogRateLimitOutOfRange => "plugin log call count is outside v1 bounds",
        })
    }
}

impl std::error::Error for LimitConfigurationError {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        Capability, CapabilitySet, ExecutionLimits, LimitConfigurationError,
        MAX_PLUGIN_PAYLOAD_BYTES, PLUGIN_ABI_V1, PluginAbiVersion,
    };

    #[test]
    fn plugin_capabilities_are_default_deny() {
        let mut capabilities = CapabilitySet::default();
        assert!(capabilities.is_empty());
        assert!(!capabilities.allows(Capability::LogWrite));

        capabilities.grant(Capability::LogWrite);
        assert!(capabilities.allows(Capability::LogWrite));
        assert_eq!(
            capabilities.iter().collect::<Vec<_>>(),
            vec![Capability::LogWrite]
        );
    }

    #[test]
    fn host_accepts_only_same_major_and_supported_minor_abi() {
        assert!(PLUGIN_ABI_V1.supports(PluginAbiVersion { major: 1, minor: 0 }));
        assert!(!PLUGIN_ABI_V1.supports(PluginAbiVersion { major: 1, minor: 1 }));
        assert!(!PLUGIN_ABI_V1.supports(PluginAbiVersion { major: 2, minor: 0 }));
        assert!(
            PluginAbiVersion { major: 1, minor: 2 }
                .supports(PluginAbiVersion { major: 1, minor: 1 })
        );
    }

    #[test]
    fn plugin_context_requires_a_one_based_attempt() {
        let workflow_run_id =
            aetherflow_core::EntityId::parse("workflow-run-1").expect("valid workflow run id");
        let task_run_id =
            aetherflow_core::EntityId::parse("task-run-1").expect("valid task run id");
        assert!(
            super::PluginExecutionContext::new(
                workflow_run_id.clone(),
                task_run_id.clone(),
                0,
                None,
            )
            .is_err()
        );
        assert!(
            super::PluginExecutionContext::new(
                workflow_run_id,
                task_run_id,
                1,
                Some(1_800_000_000_000),
            )
            .is_ok()
        );
    }

    #[test]
    fn default_execution_limits_are_valid_and_bounded() {
        let limits = ExecutionLimits::default();
        assert_eq!(limits.max_input_bytes, MAX_PLUGIN_PAYLOAD_BYTES);
        assert_eq!(limits.max_output_bytes, MAX_PLUGIN_PAYLOAD_BYTES);
        assert_eq!(limits.max_core_instances, 4);
        assert!(limits.validate().is_ok());
    }

    #[test]
    fn rejects_input_and_output_above_one_mib() {
        let limits = ExecutionLimits::default();
        let oversized = vec![0; MAX_PLUGIN_PAYLOAD_BYTES + 1];
        assert!(limits.validate_input(&oversized).is_err());
        assert!(limits.validate_output(&oversized).is_err());
    }

    #[test]
    fn rejects_unbounded_memory_fuel_and_time_budgets() {
        let limits = ExecutionLimits {
            max_memory_bytes: super::MAX_PLUGIN_MEMORY_BYTES + 1,
            ..ExecutionLimits::default()
        };
        assert_eq!(
            limits.validate(),
            Err(LimitConfigurationError::MemoryLimitOutOfRange)
        );

        let limits = ExecutionLimits {
            max_core_instances: super::MAX_PLUGIN_CORE_INSTANCES + 1,
            ..ExecutionLimits::default()
        };
        assert_eq!(
            limits.validate(),
            Err(LimitConfigurationError::CoreInstancesOutOfRange)
        );

        let limits = ExecutionLimits {
            fuel: super::MAX_PLUGIN_FUEL + 1,
            ..ExecutionLimits::default()
        };
        assert_eq!(
            limits.validate(),
            Err(LimitConfigurationError::FuelOutOfRange)
        );

        let limits = ExecutionLimits {
            hostcall_fuel: super::MAX_PLUGIN_HOSTCALL_FUEL + 1,
            ..ExecutionLimits::default()
        };
        assert_eq!(
            limits.validate(),
            Err(LimitConfigurationError::HostcallFuelOutOfRange)
        );

        let limits = ExecutionLimits {
            max_component_handles: super::MAX_PLUGIN_COMPONENT_HANDLES + 1,
            ..ExecutionLimits::default()
        };
        assert_eq!(
            limits.validate(),
            Err(LimitConfigurationError::ComponentHandlesOutOfRange)
        );

        let limits = ExecutionLimits {
            max_log_messages: super::MAX_PLUGIN_LOG_MESSAGES + 1,
            ..ExecutionLimits::default()
        };
        assert_eq!(
            limits.validate(),
            Err(LimitConfigurationError::LogRateLimitOutOfRange)
        );

        let limits = ExecutionLimits {
            wall_clock_timeout: super::MAX_PLUGIN_EXECUTION_TIMEOUT + Duration::from_millis(1),
            ..ExecutionLimits::default()
        };
        assert_eq!(
            limits.validate(),
            Err(LimitConfigurationError::TimeoutOutOfRange)
        );
    }
}

mod runtime;

pub use runtime::{CompiledPlugin, PluginRuntimeError, WasmPluginRuntime};
