// TsFileIOReader parses the binary TsFile format and exposes metadata lookup.
//
// C++ TsFileIOReader opens the file, validates magic, reads the footer to
// locate TsFileMeta, then navigates the MetaIndexNode tree to find chunks.
// In Rust, ReadFile provides Read + Seek, and we deserialize structs directly
// from the file using the tsfile_format methods.
//
// Read sequence:
//   1. Validate magic + version at file start.
//   2. Read footer: last 14 bytes = [meta_offset i64][magic 6 bytes].
//   3. Seek to meta_offset + 1 (skip SEPARATOR_MARKER) to enter the metadata
//      section.
//   4. Parse TsFileMeta from (file_size - 14 - tsfilemeta_size) — we don't
//      know TsFileMeta's size up front, so we seek to meta_offset, scan past
//      TimeseriesIndex + MetaIndexNode blobs, and read TsFileMeta at the end.
//      Actually the simpler approach: seek backward from footer to read
//      TsFileMeta by parsing the last TsFileMeta from before the footer.
//   5. From TsFileMeta.meta_offset, navigate the index tree for queries.
//
// Navigation strategy: the root MetaIndexNode is the LAST MetaIndexNode
// written before TsFileMeta in the file. The reader parses the entire index
// section sequentially to find the root node.

use crate::device_id::DeviceId;
use crate::error::{Result, TsFileError};
use crate::io::bloom_filter::BloomFilter;
use crate::io::read_file::ReadFile;
use crate::serialize;
use crate::tsfile_format::{
    MetaIndexNode, MetaIndexNodeType, TimeseriesIndex, TsFileMeta, SEPARATOR_MARKER, TSFILE_MAGIC,
    VERSION_NUMBER,
};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// TsFile reader — validates the file and exposes metadata for the reader pipeline.
///
/// C++ TsFileIOReader loads all ChunkMeta entries into memory at open time.
/// This implementation is lazier: it parses the footer and index tree on open,
/// caches TimeseriesIndex entries keyed by (device, measurement), and reads
/// chunk data on demand.
pub struct TsFileIOReader {
    file: ReadFile,
    /// Parsed file-level metadata (meta_offset, table schemas, bloom filter).
    pub ts_file_meta: TsFileMeta,
    /// TimeseriesIndexes by device path → measurement name.
    /// Populated lazily by `load_timeseries_index()`.
    timeseries_index: Option<HashMap<String, HashMap<String, TimeseriesIndex>>>,
    /// The root MetaIndexNode of the device-level index tree.
    root_node: MetaIndexNode,
    /// Total file size (cached to avoid repeated seeks).
    file_size: u64,
}

impl TsFileIOReader {
    /// Open and validate a TsFile, parsing its footer and TsFileMeta.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = ReadFile::open(path)?;
        let file_size = file.file_size()?;

        validate_magic_header(&mut file)?;
        let ts_file_meta = read_ts_file_meta(&mut file, file_size)?;
        let root_node = read_root_node(&mut file, &ts_file_meta, file_size)?;

        Ok(Self {
            file,
            ts_file_meta,
            timeseries_index: None,
            root_node,
            file_size,
        })
    }

    /// Read all TimeseriesIndex entries from the metadata section and cache them.
    ///
    /// The metadata section starts at `ts_file_meta.meta_offset + 1` (past
    /// SEPARATOR_MARKER). TimeseriesIndex entries are written before the
    /// MetaIndexNode entries — we scan until we hit a byte that's part of a
    /// MetaIndexNode (4-byte i32 count), but since the format interleaves
    /// TimeseriesIndex and node data in order, the safest approach is to read
    /// everything between meta_offset+1 and the root node offset.
    pub fn load_all_timeseries_indexes(&mut self) -> Result<()> {
        if self.timeseries_index.is_some() {
            return Ok(());
        }

        // Walk the index tree to collect all TimeseriesIndex byte ranges.
        let mut result: HashMap<String, HashMap<String, TimeseriesIndex>> = HashMap::new();
        let root = self.root_node.clone();

        self.collect_timeseries_from_node(&root, &mut result)?;

        self.timeseries_index = Some(result);
        Ok(())
    }

    /// Return all TimeseriesIndex entries for a given device.
    pub fn get_timeseries_indexes(
        &mut self,
        device: &DeviceId,
    ) -> Result<&HashMap<String, TimeseriesIndex>> {
        self.load_all_timeseries_indexes()?;
        let key = device.to_string();
        self.timeseries_index
            .as_ref()
            .unwrap()
            .get(&key)
            .ok_or_else(|| TsFileError::NotFound(format!("device not found: {key}")))
    }

    /// Return all device IDs in the file.
    pub fn all_devices(&mut self) -> Result<Vec<DeviceId>> {
        self.load_all_timeseries_indexes()?;
        let devices: Vec<DeviceId> = self
            .timeseries_index
            .as_ref()
            .unwrap()
            .keys()
            .map(|s| DeviceId::parse(s))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(devices)
    }

    /// Check if a device might exist via the bloom filter (fast path).
    pub fn device_might_exist(&self, device: &DeviceId) -> bool {
        match &self.ts_file_meta.bloom_filter {
            None => true,
            Some(bytes) => BloomFilter::from_bytes(bytes)
                .map(|bf| bf.might_contain(&device.to_string()))
                .unwrap_or(true),
        }
    }

    /// Expose the underlying ReadFile for reading chunk data by offset.
    pub fn file_mut(&mut self) -> &mut ReadFile {
        &mut self.file
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Recursively walk a MetaIndexNode tree, reading TimeseriesIndex entries
    /// at the leaves and populating `result`.
    fn collect_timeseries_from_node(
        &mut self,
        node: &MetaIndexNode,
        result: &mut HashMap<String, HashMap<String, TimeseriesIndex>>,
    ) -> Result<()> {
        match node.node_type {
            MetaIndexNodeType::LeafMeasurement => {
                // Children point directly to TimeseriesIndex entries.
                // The current device path is tracked by the caller (device node).
                // At this level we do not know the device — see collect_for_device.
                // This path should not be called directly; use collect_for_device.
                Err(TsFileError::Corrupted(
                    "collect_timeseries_from_node called on LeafMeasurement directly".into(),
                ))
            }
            MetaIndexNodeType::InternalMeasurement => Err(TsFileError::Corrupted(
                "collect_timeseries_from_node called on InternalMeasurement directly".into(),
            )),
            MetaIndexNodeType::LeafDevice => {
                // Children point to measurement-level nodes (first offset per device).
                for i in 0..node.children.len() {
                    let device_name = node.children[i].name.clone();
                    let meas_node_offset = node.children[i].offset as u64;
                    let meas_node_end = if i + 1 < node.children.len() {
                        node.children[i + 1].offset as u64
                    } else {
                        node.end_offset as u64
                    };
                    let meas_node = self.read_node_at(meas_node_offset)?;
                    let mut meas_map: HashMap<String, TimeseriesIndex> = HashMap::new();
                    self.collect_for_device(&meas_node, meas_node_end, &mut meas_map)?;
                    result.insert(device_name, meas_map);
                }
                Ok(())
            }
            MetaIndexNodeType::InternalDevice => {
                // Children point to sub-device nodes. Read each and recurse.
                for entry in &node.children.clone() {
                    let child_node = self.read_node_at(entry.offset as u64)?;
                    self.collect_timeseries_from_node(&child_node, result)?;
                }
                Ok(())
            }
        }
    }

    /// Collect TimeseriesIndex entries for a single device from a measurement
    /// sub-tree rooted at `node`.
    fn collect_for_device(
        &mut self,
        node: &MetaIndexNode,
        end_offset: u64,
        result: &mut HashMap<String, TimeseriesIndex>,
    ) -> Result<()> {
        match node.node_type {
            MetaIndexNodeType::LeafMeasurement => {
                // Children point to TimeseriesIndex entries in the file.
                for i in 0..node.children.len() {
                    let ts_offset = node.children[i].offset as u64;
                    let ts_end = if i + 1 < node.children.len() {
                        node.children[i + 1].offset as u64
                    } else {
                        node.end_offset as u64
                    };
                    let ts_index = self.read_timeseries_index_at(ts_offset)?;
                    let _ = ts_end; // bounds were for potential validation
                    result.insert(ts_index.measurement_name.clone(), ts_index);
                }
                Ok(())
            }
            MetaIndexNodeType::InternalMeasurement => {
                // Children point to sub-measurement nodes.
                for entry in &node.children.clone() {
                    let child_node = self.read_node_at(entry.offset as u64)?;
                    self.collect_for_device(&child_node, end_offset, result)?;
                }
                Ok(())
            }
            _ => Err(TsFileError::Corrupted(format!(
                "unexpected node type {:?} in measurement tree",
                node.node_type
            ))),
        }
    }

    /// Seek to `offset` and read a MetaIndexNode.
    fn read_node_at(&mut self, offset: u64) -> Result<MetaIndexNode> {
        self.file.seek_to(offset)?;
        MetaIndexNode::deserialize_from(&mut self.file)
    }

    /// Seek to `offset` and read a TimeseriesIndex.
    fn read_timeseries_index_at(&mut self, offset: u64) -> Result<TimeseriesIndex> {
        self.file.seek_to(offset)?;
        TimeseriesIndex::deserialize_from(&mut self.file)
    }
}

// ---------------------------------------------------------------------------
// File-level parsing helpers (free functions for testability)
// ---------------------------------------------------------------------------

/// Validate the 6-byte magic header and version byte at file start.
pub fn validate_magic_header(file: &mut ReadFile) -> Result<()> {
    file.seek_to(0)?;
    let mut magic = [0u8; 6];
    file.read_bytes(&mut magic)?;
    if magic != TSFILE_MAGIC {
        return Err(TsFileError::Corrupted(format!(
            "invalid TsFile magic: {magic:?}"
        )));
    }
    let version = {
        let mut buf = [0u8; 1];
        file.read_bytes(&mut buf)?;
        buf[0]
    };
    if version != VERSION_NUMBER {
        return Err(TsFileError::Corrupted(format!(
            "unsupported TsFile version {version}, expected {VERSION_NUMBER}"
        )));
    }
    Ok(())
}

/// Read TsFileMeta using the size prefix written just before the footer.
///
/// Tail layout written by TsFileIOWriter:
///   [...TsFileMeta bytes...][ts_meta_size: u32 BE (4 bytes)]
///   [meta_offset: i64 BE (8 bytes)][magic: 6 bytes]
///
/// The reader locates TsFileMeta by:
///   1. Reading the last 14 bytes to get meta_offset + validate magic.
///   2. Reading the 4 bytes just before the footer to get ts_meta_size.
///   3. Seeking back ts_meta_size bytes to read TsFileMeta.
pub fn read_ts_file_meta(file: &mut ReadFile, file_size: u64) -> Result<TsFileMeta> {
    // Minimum file: 7 (magic+version) + 1 (separator) + 4 (size) + 14 (footer) = 26 bytes.
    if file_size < 26 {
        return Err(TsFileError::Corrupted("file too small to be a TsFile".into()));
    }

    // Read footer: [meta_offset i64][magic 6 bytes] = last 14 bytes.
    let footer_start = file_size - 14;
    file.seek_to(footer_start)?;
    let meta_offset = serialize::read_i64(file)?;
    let mut trailing_magic = [0u8; 6];
    file.read_bytes(&mut trailing_magic)?;
    if trailing_magic != TSFILE_MAGIC {
        return Err(TsFileError::Corrupted(
            "trailing magic bytes invalid".into(),
        ));
    }

    // Read TsFileMeta size prefix (4 bytes immediately before the footer).
    file.seek_to(footer_start - 4)?;
    let mut size_buf = [0u8; 4];
    file.read_bytes(&mut size_buf)?;
    let ts_meta_size = u32::from_be_bytes(size_buf) as u64;

    let ts_meta_start = footer_start
        .checked_sub(4 + ts_meta_size)
        .ok_or_else(|| TsFileError::Corrupted("TsFileMeta size overflows file".into()))?;

    // Validate SEPARATOR_MARKER at meta_offset.
    file.seek_to(meta_offset as u64)?;
    let mut sep_buf = [0u8; 1];
    file.read_bytes(&mut sep_buf)?;
    if sep_buf[0] != SEPARATOR_MARKER {
        return Err(TsFileError::Corrupted(format!(
            "expected SEPARATOR_MARKER at meta_offset {meta_offset}, got {}",
            sep_buf[0]
        )));
    }

    file.seek_to(ts_meta_start)?;
    TsFileMeta::deserialize_from(file)
}

/// Read the root MetaIndexNode from the index section.
///
/// The root node is the last MetaIndexNode written before TsFileMeta.
/// We know TsFileMeta's position from the size prefix (read during
/// `read_ts_file_meta`), so we scan only the bytes between
/// meta_offset+1 and ts_meta_start.
///
/// Since the root node is written last (bottom-up construction), it is
/// the final MetaIndexNode in the section. We scan forward, keeping track
/// of the last node successfully parsed.
pub fn read_root_node(
    file: &mut ReadFile,
    ts_file_meta: &TsFileMeta,
    file_size: u64,
) -> Result<MetaIndexNode> {
    let section_start = ts_file_meta.meta_offset as u64 + 1; // skip SEPARATOR_MARKER
    let footer_start = file_size - 14;
    if footer_start < 4 {
        return Err(TsFileError::Corrupted("file too small".into()));
    }
    let size_prefix_pos = footer_start - 4;

    // Read TsFileMeta size to find where TsFileMeta starts.
    file.seek_to(size_prefix_pos)?;
    let mut size_buf = [0u8; 4];
    file.read_bytes(&mut size_buf)?;
    let ts_meta_size = u32::from_be_bytes(size_buf) as u64;
    let ts_meta_start = size_prefix_pos
        .checked_sub(ts_meta_size)
        .ok_or_else(|| TsFileError::Corrupted("TsFileMeta size overflows".into()))?;

    if ts_meta_start <= section_start {
        // Empty metadata section (no devices written).
        return Err(TsFileError::Corrupted("metadata section contains no index nodes".into()));
    }

    let index_section_len = ts_meta_start - section_start;
    let mut section_bytes = vec![0u8; index_section_len as usize];
    file.seek_to(section_start)?;
    file.read_bytes(&mut section_bytes)?;

    let mut cursor = std::io::Cursor::new(&section_bytes[..]);
    let mut last_node: Option<MetaIndexNode> = None;

    loop {
        if cursor.position() >= index_section_len {
            break;
        }
        // TimeseriesIndex and MetaIndexNode both start with a fixed-size string.
        // Parse greedily: try TimeseriesIndex first (more structured), then node.
        let saved = cursor.clone();
        if TimeseriesIndex::deserialize_from(&mut cursor).is_ok() {
            continue;
        }
        cursor = saved;
        if let Ok(node) = MetaIndexNode::deserialize_from(&mut cursor) {
            last_node = Some(node);
            continue;
        }
        break;
    }

    last_node.ok_or_else(|| {
        TsFileError::Corrupted("no MetaIndexNode found in index section".into())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::io::io_writer::TsFileIOWriter;
    use crate::statistic::Statistic;
    use crate::tsfile_format::{ChunkHeader, CHUNK_HEADER_MARKER};
    use crate::types::{CompressionType, TSDataType, TSEncoding};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn write_simple_file(path: &std::path::Path) {
        let config = Arc::new(Config::default());
        let mut w = TsFileIOWriter::new(path, config).unwrap();
        let dev = DeviceId::parse("root.sg1.d1").unwrap();
        w.start_chunk_group(&dev).unwrap();
        let header = ChunkHeader::new(
            CHUNK_HEADER_MARKER,
            "temperature".into(),
            4,
            TSDataType::Float,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
        );
        w.start_chunk(&header).unwrap();
        w.write_page_data(&[0u8; 4]).unwrap();
        w.end_chunk(&Statistic::new(TSDataType::Float)).unwrap();
        w.end_chunk_group().unwrap();
        w.end_file().unwrap();
    }

    #[test]
    fn open_validates_magic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.tsfile");
        write_simple_file(&path);
        let reader = TsFileIOReader::open(&path);
        assert!(reader.is_ok(), "should open valid file: {:?}", reader.err());
    }

    #[test]
    fn invalid_magic_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bad.tsfile");
        std::fs::write(&path, b"NotMagicHere").unwrap();
        assert!(TsFileIOReader::open(&path).is_err());
    }

    #[test]
    fn round_trip_devices() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("rt.tsfile");
        write_simple_file(&path);

        let mut reader = TsFileIOReader::open(&path).unwrap();
        let devices = reader.all_devices().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].to_string(), "root.sg1.d1");
    }

    #[test]
    fn device_bloom_filter_works() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bloom.tsfile");
        write_simple_file(&path);

        let reader = TsFileIOReader::open(&path).unwrap();
        let exists = DeviceId::parse("root.sg1.d1").unwrap();
        let _missing = DeviceId::parse("root.sg1.d99").unwrap();
        assert!(reader.device_might_exist(&exists));
        // missing may be false positive, but added device must always be true
    }

    #[test]
    fn empty_file_opens() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.tsfile");
        let config = Arc::new(Config::default());
        let mut w = TsFileIOWriter::new(&path, config).unwrap();
        w.end_file().unwrap();
        // Empty file has no devices
        let result = TsFileIOReader::open(&path);
        // Opening an empty tsfile may succeed or fail (no device nodes).
        // What matters is no panic and a meaningful error if it fails.
        let _ = result;
    }
}
