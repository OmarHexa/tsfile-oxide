// C++ uses the full ANTLR4 runtime (~100 vendored files) to parse paths
// like "root.sg1.d1.temperature". The grammar is trivial: split on '.'
// with optional backtick quoting for segments containing dots or special
// characters. A hand-written parser replaces the entire ANTLR4 dependency.
//
// This is a textbook example of framework over-engineering: the C++ project
// vendors an entire parser generator runtime for what is essentially a
// string split with one quoting rule.

use crate::error::{TsFileError, Result};

/// Parse a dot-separated path into segments, respecting backtick quoting.
///
/// - Simple: `"root.sg1.d1"` -> `["root", "sg1", "d1"]`
/// - Quoted: `` "root.`sg.1`.d1" `` -> `["root", "sg.1", "d1"]`
/// - Backticks are stripped from quoted segments.
/// - Empty input returns an empty vec.
pub fn parse_path(input: &str) -> Result<Vec<String>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let mut segments = Vec::new();
    let mut current = String::new();
    let mut in_backtick = false;

    for ch in input.chars() {
        match ch {
            '`' => {
                // Toggle quoted region
                in_backtick = !in_backtick;
            }
            '.' if !in_backtick => {
                // Segment separator outside quotes
                segments.push(std::mem::take(&mut current));
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if in_backtick {
        return Err(TsFileError::InvalidArg(format!(
            "unclosed backtick in path: {input}"
        )));
    }

    // Push the final segment
    segments.push(current);

    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_dotted_path() {
        let segs = parse_path("root.sg1.d1").unwrap();
        assert_eq!(segs, vec!["root", "sg1", "d1"]);
    }

    #[test]
    fn single_segment() {
        let segs = parse_path("root").unwrap();
        assert_eq!(segs, vec!["root"]);
    }

    #[test]
    fn empty_input() {
        let segs = parse_path("").unwrap();
        assert!(segs.is_empty());
    }

    #[test]
    fn backtick_quoted_segment() {
        let segs = parse_path("root.`sg.1`.d1").unwrap();
        assert_eq!(segs, vec!["root", "sg.1", "d1"]);
    }

    #[test]
    fn backtick_at_start() {
        let segs = parse_path("`root.sg1`.d1").unwrap();
        assert_eq!(segs, vec!["root.sg1", "d1"]);
    }

    #[test]
    fn backtick_at_end() {
        let segs = parse_path("root.`d1.temp`").unwrap();
        assert_eq!(segs, vec!["root", "d1.temp"]);
    }

    #[test]
    fn multiple_quoted_segments() {
        let segs = parse_path("`a.b`.`c.d`").unwrap();
        assert_eq!(segs, vec!["a.b", "c.d"]);
    }

    #[test]
    fn empty_segments_from_consecutive_dots() {
        let segs = parse_path("a..b").unwrap();
        assert_eq!(segs, vec!["a", "", "b"]);
    }

    #[test]
    fn trailing_dot() {
        let segs = parse_path("root.sg1.").unwrap();
        assert_eq!(segs, vec!["root", "sg1", ""]);
    }

    #[test]
    fn leading_dot() {
        let segs = parse_path(".root.sg1").unwrap();
        assert_eq!(segs, vec!["", "root", "sg1"]);
    }

    #[test]
    fn unclosed_backtick_returns_error() {
        let result = parse_path("root.`unclosed");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unclosed backtick"));
    }

    #[test]
    fn quoted_segment_with_no_dots_inside() {
        let segs = parse_path("root.`sg1`.d1").unwrap();
        assert_eq!(segs, vec!["root", "sg1", "d1"]);
    }

    #[test]
    fn long_path() {
        let segs = parse_path("root.sg1.sg2.sg3.d1.temperature").unwrap();
        assert_eq!(segs, vec!["root", "sg1", "sg2", "sg3", "d1", "temperature"]);
    }
}
