// C++ defines an IDeviceID interface with a single implementation
// (StringArrayDeviceID). The virtual hierarchy exists for extensibility
// that was never used. In Rust we skip the trait indirection and use a
// concrete struct directly — if a second variant is ever needed, it can
// be refactored into an enum at that point (YAGNI).
//
// C++ stores segments as vector<string*> with manual lifecycle management.
// In Rust, Vec<String> owns its data and Drop handles cleanup — no
// explicit delete calls, no risk of use-after-free.

use crate::error::Result;
use crate::path::parse_path;
use crate::serialize;
use std::fmt;

/// A device identifier consisting of dot-separated path segments.
///
/// In the tree model, a device like `root.sg1.d1` has segments
/// `["root", "sg1", "d1"]`. In the table model, the first segment
/// is the table name.
///
/// Ordering is lexicographic over segments, matching C++ operator< on
/// StringArrayDeviceID — this is critical because the on-disk index
/// stores devices in sorted order.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeviceId {
    pub segments: Vec<String>,
}

// Lexicographic ordering over segments, matching C++ StringArrayDeviceID
// comparison which compares segment-by-segment. This ordering determines
// the device layout in the on-disk index tree.
impl PartialOrd for DeviceId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeviceId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.segments.cmp(&other.segments)
    }
}

impl DeviceId {
    /// Create a DeviceId from a pre-split list of segments.
    pub fn new(segments: Vec<String>) -> Self {
        Self { segments }
    }

    /// Parse a dot-separated path string into a DeviceId.
    ///
    /// Uses the hand-written path parser (replaces C++ ANTLR4 runtime).
    ///

    pub fn parse(path: &str) -> Result<Self> {
        let segments = parse_path(path)?;
        Ok(Self { segments })
    }

    /// Returns the table name, which is the first segment.
    /// In the table data model, devices are grouped by table name.
    pub fn table_name(&self) -> Option<&str> {
        self.segments.first().map(|s| s.as_str())
    }

    /// Returns the number of path segments.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Serialize this DeviceId: varint segment count + each segment length-prefixed.
    pub fn serialize(&self, writer: &mut impl std::io::Write) -> Result<()> {
        serialize::write_var_u32(writer, self.segments.len() as u32)?;
        for seg in &self.segments {
            serialize::write_string(writer, seg)?;
        }
        Ok(())
    }

    /// Deserialize a DeviceId from a reader.
    pub fn deserialize(reader: &mut impl std::io::Read) -> Result<Self> {
        let count = serialize::read_var_u32(reader)? as usize;
        let mut segments = Vec::with_capacity(count);
        for _ in 0..count {
            segments.push(serialize::read_string(reader)?);
        }
        Ok(Self { segments })
    }
}

// TODO: c++ implementation split_device_id_string function missing,
// count the total segments and group the path
// "root.a.b.c" -> {"root.a.b", "c"}
// "root.a.b.c.d" -> {"root.a.b", "c", "d"}
// "root.a" -> {"root", "a"}
// "root.a.b" -> {"root.a", "b"}
// "root" -> {"root"}

/// Display as the dot-joined path, matching the canonical string form.
impl fmt::Display for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for seg in &self.segments {
            if !first {
                write!(f, ".")?;
            }
            // Quote segments that contain dots
            if seg.contains('.') {
                write!(f, "`{seg}`")?;
            } else {
                write!(f, "{seg}")?;
            }
            first = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn parse_simple_path() {
        let id = DeviceId::parse("root.sg1.d1").unwrap();
        assert_eq!(id.segments, vec!["root", "sg1", "d1"]);
    }

    #[test]
    fn parse_single_segment() {
        let id = DeviceId::parse("root").unwrap();
        assert_eq!(id.segments, vec!["root"]);
    }

    #[test]
    fn parse_quoted_segment() {
        let id = DeviceId::parse("root.`sg.1`.d1").unwrap();
        assert_eq!(id.segments, vec!["root", "sg.1", "d1"]);
    }

    #[test]
    fn table_name_is_first_segment() {
        let id = DeviceId::parse("root.sg1.d1").unwrap();
        assert_eq!(id.table_name(), Some("root"));
    }

    #[test]
    fn table_name_empty_device() {
        let id = DeviceId::new(vec![]);
        assert_eq!(id.table_name(), None);
    }

    #[test]
    fn segment_count() {
        let id = DeviceId::parse("root.sg1.d1").unwrap();
        assert_eq!(id.segment_count(), 3);
    }

    #[test]
    fn display_simple() {
        let id = DeviceId::parse("root.sg1.d1").unwrap();
        assert_eq!(id.to_string(), "root.sg1.d1");
    }

    #[test]
    fn display_quotes_dotted_segments() {
        let id = DeviceId::new(vec!["root".into(), "sg.1".into(), "d1".into()]);
        assert_eq!(id.to_string(), "root.`sg.1`.d1");
    }

    #[test]
    fn ordering_lexicographic() {
        let a = DeviceId::parse("root.sg1.d1").unwrap();
        let b = DeviceId::parse("root.sg1.d2").unwrap();
        let c = DeviceId::parse("root.sg2.d1").unwrap();
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn ordering_shorter_prefix_comes_first() {
        let short = DeviceId::parse("root.sg1").unwrap();
        let long = DeviceId::parse("root.sg1.d1").unwrap();
        assert!(short < long);
    }

    #[test]
    fn equality() {
        let a = DeviceId::parse("root.sg1.d1").unwrap();
        let b = DeviceId::parse("root.sg1.d1").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn hash_consistent_with_equality() {
        use std::collections::HashSet;
        let a = DeviceId::parse("root.sg1.d1").unwrap();
        let b = DeviceId::parse("root.sg1.d1").unwrap();
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }

    #[test]
    fn serialize_deserialize_round_trip() {
        let original = DeviceId::parse("root.sg1.d1").unwrap();
        let mut buf = Vec::new();
        original.serialize(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = DeviceId::deserialize(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn serialize_deserialize_empty() {
        let original = DeviceId::new(vec![]);
        let mut buf = Vec::new();
        original.serialize(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = DeviceId::deserialize(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn serialize_deserialize_with_quoted_segments() {
        let original = DeviceId::new(vec!["root".into(), "sg.1".into(), "d1".into()]);
        let mut buf = Vec::new();
        original.serialize(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = DeviceId::deserialize(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }
}
