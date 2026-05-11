//! Pluggable strategy interface.

use crate::events::{Event, OrderIntent};
use crate::state::GlobalState;
use time::OffsetDateTime;

/// Read-only context passed to strategies.
pub struct StrategyContext<'a> {
    /// Current engine time (event time or replay clock).
    pub now: OffsetDateTime,
    /// Read-only state.
    pub state: &'a GlobalState,
}

/// User strategy hook.
pub trait Strategy: Send {
    /// React to one normalized event; return order intents (may be empty).
    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &Event) -> Vec<OrderIntent>;
}
