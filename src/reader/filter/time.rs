// C++ time filters (filter/time/) are six small classes; each overrides
// satisfy and the statistic-prune hooks. We mirror the set 1-for-1.
// Hand-written Filter impls per variant because each needs a tight
// satisfy_time_range with a different direction relative to the bound.

use crate::reader::filter::Filter;
use crate::statistic::Statistic;
use crate::value::TsValue;

#[derive(Debug, Clone)] pub struct TimeGt    { pub bound: i64 }
#[derive(Debug, Clone)] pub struct TimeGtEq  { pub bound: i64 }
#[derive(Debug, Clone)] pub struct TimeLt    { pub bound: i64 }
#[derive(Debug, Clone)] pub struct TimeLtEq  { pub bound: i64 }
#[derive(Debug, Clone)] pub struct TimeEq    { pub bound: i64 }
#[derive(Debug, Clone)] pub struct TimeBetween { pub low: i64, pub high: i64, pub inclusive: bool }

impl TimeGt    { pub fn new(bound: i64) -> Self { Self { bound } } }
impl TimeGtEq  { pub fn new(bound: i64) -> Self { Self { bound } } }
impl TimeLt    { pub fn new(bound: i64) -> Self { Self { bound } } }
impl TimeLtEq  { pub fn new(bound: i64) -> Self { Self { bound } } }
impl TimeEq    { pub fn new(bound: i64) -> Self { Self { bound } } }
impl TimeBetween {
    pub fn new(low: i64, high: i64, inclusive: bool) -> Self { Self { low, high, inclusive } }
}

impl Filter for TimeGt {
    fn satisfy_statistic(&self, stat: &Statistic) -> bool {
        if stat.count() == 0 { return false; }
        self.satisfy_time_range(stat.start_time(), stat.end_time())
    }
    fn satisfy_time_range(&self, _start: i64, end: i64) -> bool { end > self.bound }
    fn satisfy(&self, time: i64, _v: Option<&TsValue>) -> bool { time > self.bound }
}

impl Filter for TimeGtEq {
    fn satisfy_statistic(&self, stat: &Statistic) -> bool {
        if stat.count() == 0 { return false; }
        self.satisfy_time_range(stat.start_time(), stat.end_time())
    }
    fn satisfy_time_range(&self, _start: i64, end: i64) -> bool { end >= self.bound }
    fn satisfy(&self, time: i64, _v: Option<&TsValue>) -> bool { time >= self.bound }
}

impl Filter for TimeLt {
    fn satisfy_statistic(&self, stat: &Statistic) -> bool {
        if stat.count() == 0 { return false; }
        self.satisfy_time_range(stat.start_time(), stat.end_time())
    }
    fn satisfy_time_range(&self, start: i64, _end: i64) -> bool { start < self.bound }
    fn satisfy(&self, time: i64, _v: Option<&TsValue>) -> bool { time < self.bound }
}

impl Filter for TimeLtEq {
    fn satisfy_statistic(&self, stat: &Statistic) -> bool {
        if stat.count() == 0 { return false; }
        self.satisfy_time_range(stat.start_time(), stat.end_time())
    }
    fn satisfy_time_range(&self, start: i64, _end: i64) -> bool { start <= self.bound }
    fn satisfy(&self, time: i64, _v: Option<&TsValue>) -> bool { time <= self.bound }
}

impl Filter for TimeEq {
    fn satisfy_statistic(&self, stat: &Statistic) -> bool {
        if stat.count() == 0 { return false; }
        self.satisfy_time_range(stat.start_time(), stat.end_time())
    }
    fn satisfy_time_range(&self, start: i64, end: i64) -> bool {
        start <= self.bound && self.bound <= end
    }
    fn satisfy(&self, time: i64, _v: Option<&TsValue>) -> bool { time == self.bound }
}

impl Filter for TimeBetween {
    fn satisfy_statistic(&self, stat: &Statistic) -> bool {
        if stat.count() == 0 { return false; }
        self.satisfy_time_range(stat.start_time(), stat.end_time())
    }
    fn satisfy_time_range(&self, start: i64, end: i64) -> bool {
        if self.inclusive { !(end < self.low || start > self.high) }
        else              { !(end <= self.low || start >= self.high) }
    }
    fn satisfy(&self, time: i64, _v: Option<&TsValue>) -> bool {
        if self.inclusive { time >= self.low && time <= self.high }
        else              { time >  self.low && time <  self.high }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statistic::Statistic;
    use crate::types::TSDataType;

    /// Build an Int64 statistic by feeding `count` points. First point
    /// uses `start` as the timestamp, remaining points use `end`. Values
    /// are irrelevant for time-only filters; pass 0. Works only for
    /// `count >= 1`.
    fn stat_i64(count: u64, start: i64, end: i64) -> Statistic {
        assert!(count >= 1);
        let mut s = Statistic::new(TSDataType::Int64);
        s.update_i64(start, 0);
        let remaining = count.saturating_sub(1);
        for _ in 0..remaining {
            s.update_i64(end, 0);
        }
        s
    }

    #[test]
    fn time_gt_row_level() {
        let f = TimeGt::new(10);
        assert!(!f.satisfy(10, None));
        assert!( f.satisfy(11, None));
    }

    #[test]
    fn time_gteq_row_level() {
        let f = TimeGtEq::new(10);
        assert!( f.satisfy(10, None));
        assert!(!f.satisfy(9,  None));
    }

    #[test]
    fn time_lt_lteq_row_level() {
        assert!( TimeLt::new(10).satisfy(9, None));
        assert!(!TimeLt::new(10).satisfy(10, None));
        assert!( TimeLtEq::new(10).satisfy(10, None));
    }

    #[test]
    fn time_eq_row_level() {
        assert!( TimeEq::new(7).satisfy(7, None));
        assert!(!TimeEq::new(7).satisfy(8, None));
    }

    #[test]
    fn time_between_inclusive() {
        let f = TimeBetween::new(10, 20, true);
        assert!(f.satisfy(10, None));
        assert!(f.satisfy(20, None));
        assert!(!f.satisfy(9, None));
        assert!(!f.satisfy(21, None));
    }

    #[test]
    fn time_between_exclusive() {
        let f = TimeBetween::new(10, 20, false);
        assert!(!f.satisfy(10, None));
        assert!(!f.satisfy(20, None));
        assert!( f.satisfy(15, None));
    }

    #[test]
    fn statistic_pruning_empty_rejects() {
        let s = Statistic::new(TSDataType::Int64);
        assert!(!TimeGt::new(0).satisfy_statistic(&s));
    }

    #[test]
    fn statistic_pruning_time_gt_skips_below_bound() {
        let s = stat_i64(2, 0, 9);
        assert!(!TimeGt::new(10).satisfy_statistic(&s));
        let s = stat_i64(2, 0, 11);
        assert!( TimeGt::new(10).satisfy_statistic(&s));
    }

    #[test]
    fn statistic_pruning_time_between_accepts_overlap() {
        let s = stat_i64(3, 5, 25);
        assert!(TimeBetween::new(10, 20, true).satisfy_statistic(&s));
        let s = stat_i64(3, 30, 40);
        assert!(!TimeBetween::new(10, 20, true).satisfy_statistic(&s));
    }

    #[test]
    fn statistic_pruning_time_gteq_boundary() {
        assert!(TimeGtEq::new(10).satisfy_time_range(0, 10));
        assert!(!TimeGtEq::new(10).satisfy_time_range(0, 9));
    }

    #[test]
    fn statistic_pruning_time_lt_boundary() {
        assert!(!TimeLt::new(10).satisfy_time_range(10, 20));
        assert!(TimeLt::new(10).satisfy_time_range(9, 20));
    }

    #[test]
    fn statistic_pruning_time_lteq_boundary() {
        assert!(TimeLtEq::new(10).satisfy_time_range(10, 20));
        assert!(!TimeLtEq::new(10).satisfy_time_range(11, 20));
    }

    #[test]
    fn statistic_pruning_time_eq_overlap_and_boundary() {
        assert!(TimeEq::new(7).satisfy_time_range(5, 10));
        assert!(TimeEq::new(5).satisfy_time_range(5, 10));
        assert!(TimeEq::new(10).satisfy_time_range(5, 10));
        assert!(!TimeEq::new(4).satisfy_time_range(5, 10));
        assert!(!TimeEq::new(11).satisfy_time_range(5, 10));
    }
}
