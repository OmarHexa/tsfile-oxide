// C++ And/Or/Not filters hold Filter* children. Rust equivalent:
// Box<dyn Filter>. Statistic-level composition: And tightens (all must
// accept), Or loosens (any accepts), Not is conservative — cannot prove
// a range-based min/max rejects the negated child, so always accept.

use crate::reader::filter::Filter;
use crate::statistic::Statistic;
use crate::value::TsValue;

#[derive(Debug)]
pub struct And { pub left: Box<dyn Filter>, pub right: Box<dyn Filter> }
#[derive(Debug)]
pub struct Or  { pub left: Box<dyn Filter>, pub right: Box<dyn Filter> }
#[derive(Debug)]
pub struct Not { pub inner: Box<dyn Filter> }

impl And { pub fn new(l: Box<dyn Filter>, r: Box<dyn Filter>) -> Self { Self { left: l, right: r } } }
impl Or  { pub fn new(l: Box<dyn Filter>, r: Box<dyn Filter>) -> Self { Self { left: l, right: r } } }
impl Not { pub fn new(inner: Box<dyn Filter>) -> Self { Self { inner } } }

impl Filter for And {
    fn satisfy_statistic(&self, stat: &Statistic) -> bool {
        self.left.satisfy_statistic(stat) && self.right.satisfy_statistic(stat)
    }
    fn satisfy_time_range(&self, s: i64, e: i64) -> bool {
        self.left.satisfy_time_range(s, e) && self.right.satisfy_time_range(s, e)
    }
    fn satisfy(&self, t: i64, v: Option<&TsValue>) -> bool {
        self.left.satisfy(t, v) && self.right.satisfy(t, v)
    }
}

impl Filter for Or {
    fn satisfy_statistic(&self, stat: &Statistic) -> bool {
        self.left.satisfy_statistic(stat) || self.right.satisfy_statistic(stat)
    }
    fn satisfy_time_range(&self, s: i64, e: i64) -> bool {
        self.left.satisfy_time_range(s, e) || self.right.satisfy_time_range(s, e)
    }
    fn satisfy(&self, t: i64, v: Option<&TsValue>) -> bool {
        self.left.satisfy(t, v) || self.right.satisfy(t, v)
    }
}

impl Filter for Not {
    // Conservative: cannot prove a statistic rejects a negated filter,
    // so Not::satisfy_statistic and satisfy_time_range always return
    // true. Row-level is the only level where Not prunes.
    fn satisfy_statistic(&self, _stat: &Statistic) -> bool { true }
    fn satisfy_time_range(&self, _s: i64, _e: i64) -> bool { true }
    fn satisfy(&self, t: i64, v: Option<&TsValue>) -> bool {
        !self.inner.satisfy(t, v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::filter::time::{TimeGt, TimeLt};

    #[test]
    fn and_truth_table_rows() {
        let a = And::new(Box::new(TimeGt::new(10)), Box::new(TimeLt::new(20)));
        assert!(!a.satisfy(10, None));
        assert!( a.satisfy(15, None));
        assert!(!a.satisfy(20, None));
    }

    #[test]
    fn or_truth_table_rows() {
        let o = Or::new(Box::new(TimeLt::new(10)), Box::new(TimeGt::new(20)));
        assert!( o.satisfy(5, None));
        assert!(!o.satisfy(15, None));
        assert!( o.satisfy(25, None));
    }

    #[test]
    fn not_negates_rows() {
        let n = Not::new(Box::new(TimeGt::new(10)));
        assert!(!n.satisfy(11, None));
        assert!( n.satisfy(10, None));
    }

    #[test]
    fn not_statistic_is_conservative() {
        use crate::statistic::Statistic;
        use crate::types::TSDataType;
        let s = Statistic::new(TSDataType::Int64);
        let n = Not::new(Box::new(TimeGt::new(0)));
        assert!(n.satisfy_statistic(&s));
        assert!(n.satisfy_time_range(0, 0));
    }

    #[test]
    fn and_statistic_tightens() {
        use crate::statistic::Statistic;
        use crate::types::TSDataType;
        // Build a statistic with time range [0, 9]. TimeGt(5) accepts,
        // TimeGt(50) rejects; AND should reject whenever either rejects.
        let mut s = Statistic::new(TSDataType::Int64);
        s.update_i64(0, 0);
        s.update_i64(9, 0);
        let a_accept = And::new(Box::new(TimeGt::new(5)), Box::new(TimeLt::new(100)));
        let a_reject = And::new(Box::new(TimeGt::new(5)), Box::new(TimeGt::new(50)));
        assert!(a_accept.satisfy_statistic(&s));
        assert!(!a_reject.satisfy_statistic(&s));
    }

    #[test]
    fn or_statistic_loosens() {
        use crate::statistic::Statistic;
        use crate::types::TSDataType;
        let mut s = Statistic::new(TSDataType::Int64);
        s.update_i64(0, 0);
        s.update_i64(9, 0);
        // TimeGt(50) rejects, TimeLt(100) accepts; OR accepts whenever either accepts.
        let o = Or::new(Box::new(TimeGt::new(50)), Box::new(TimeLt::new(100)));
        assert!(o.satisfy_statistic(&s));
    }
}
