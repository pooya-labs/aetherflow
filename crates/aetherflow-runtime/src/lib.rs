//! Single-node runtime building blocks and testable repository adapters.
//!
//! This crate depends on the domain core, never the other way around.

pub mod clock;
pub mod repository;
pub mod scheduler;
pub mod workflow;
