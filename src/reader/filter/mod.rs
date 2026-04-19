// C++ filter system uses Filter* base class with 20+ virtual subclasses.
// Filters are composed at runtime into trees (AndFilter holds Filter* left/right).
// This is one of the few places we use dyn Trait instead of enum — the
// filter set is open (users should be able to add custom filters) and
// filters are composed dynamically via Box<dyn Filter>.

use crate::statistic::Statistic;
use crate::value::TsValue;

pub mod logical;
pub mod time;
pub mod value;

/// Predicate evaluated at three levels during a scan: chunk statistic,
/// page statistic, and individual rows. Implementations must return
/// `true` whenever the input *could* contain a satisfying row; false
/// means "safe to prune."
pub trait Filter: Send + Sync + std::fmt::Debug {
    /// Can any row covered by this statistic possibly satisfy this filter?
    fn satisfy_statistic(&self, stat: &Statistic) -> bool;

    /// Tight time-range check for filters that only care about time.
    /// Default: no pruning.
    fn satisfy_time_range(&self, _start: i64, _end: i64) -> bool {
        true
    }

    /// Row-level check. `value` is `None` for a null slot in an aligned chunk.
    fn satisfy(&self, time: i64, value: Option<&TsValue>) -> bool;
}
