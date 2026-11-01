//! Injectable time boundaries for runtime scheduling.

use std::time::Instant;

/// A monotonic source of elapsed milliseconds for in-process scheduling.
///
/// Values are relative to an arbitrary process-local origin. They must not be
/// persisted or compared across process restarts.
pub trait Clock: Send + Sync {
    /// Returns elapsed monotonic milliseconds from this clock's origin.
    fn now_ms(&self) -> u64;
}

/// A production monotonic clock backed by Instant.
#[derive(Debug)]
pub struct MonotonicClock {
    origin: Instant,
}

impl MonotonicClock {
    /// Creates a new clock whose first reading is close to zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MonotonicClock {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::{Clock, MonotonicClock};

    #[test]
    fn monotonic_clock_never_moves_backwards() {
        let clock = MonotonicClock::new();
        let first = clock.now_ms();
        let second = clock.now_ms();

        assert!(second >= first);
    }
}
