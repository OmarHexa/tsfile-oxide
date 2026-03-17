// TsFileIOWriter manages the write pipeline for the binary TsFile format.
//
// C++ TsFileIOWriter holds raw pointers (ChunkGroupMeta*, ChunkMeta*) and
// manually manages their lifecycle. It also tracks write position by querying
// the ByteStream buffer. In Rust:
//   - All metadata is owned by value (Vec, Option)
//   - WriteFile.position() tracks the current write offset without seeking
//   - The metadata index tree is built bottom-up in end_file()
//
// Write sequence (called by the higher-level ChunkWriter / TsFileWriter):
//   new() → start_chunk_group() → start_chunk() → write_page_data() →
//   end_chunk() → ... → end_chunk_group() → ... → end_file()

use crate::config::Config;
use crate::device_id::DeviceId;
use crate::error::{Result, TsFileError};
use crate::io::bloom_filter::BloomFilter;
use crate::io::write_file::WriteFile;
use crate::schema::TableSchema;
use crate::serialize;
use crate::statistic::Statistic;
use crate::tsfile_format::{
    ChunkGroupMeta, ChunkHeader, ChunkMeta, MetaIndexEntry, MetaIndexNode, MetaIndexNodeType,
    TimeseriesIndex, TsFileMeta, CHUNK_GROUP_FOOTER_MARKER, CHUNK_TYPE_ALIGNED_TIME_MASK,
    CHUNK_TYPE_ALIGNED_VALUE_MASK, CHUNK_TYPE_NON_ALIGNED_MASK, SEPARATOR_MARKER, TIME_CHUNK_HEADER_MARKER,
    TSFILE_MAGIC, VALUE_CHUNK_HEADER_MARKER, VERSION_NUMBER,
};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

/// TsFile format writer — owns all open state and flushes on `end_file()`.
///
/// C++ TsFileIOWriter is constructed with a path and a Config reference,
/// writes the magic+version header immediately, then accepts chunk group /
/// chunk / page data in sequence, and builds the metadata index tree when
/// `end_file()` is called.
pub struct TsFileIOWriter {
    file: WriteFile,
    /// Completed chunk groups from previous `end_chunk_group()` calls.
    chunk_group_metas: Vec<ChunkGroupMeta>,
    /// Chunk metas accumulated for the currently open chunk group.
    current_chunk_metas: Vec<ChunkMeta>,
    /// Device ID of the currently open chunk group (set by start_chunk_group).
    current_device: Option<DeviceId>,
    /// File offset where the current chunk's header starts (for ChunkMeta).
    current_chunk_offset: u64,
    /// Header of the current open chunk (retained to derive mask in end_chunk).
    current_chunk_header: Option<ChunkHeader>,
    /// Table schemas registered for the table model (written to TsFileMeta).
    table_schemas: Vec<TableSchema>,
    config: Arc<Config>,
}

impl TsFileIOWriter {
    /// Create a new writer for `path` and immediately write the TsFile header.
    pub fn new(path: impl AsRef<Path>, config: Arc<Config>) -> Result<Self> {
        let mut file = WriteFile::create(path)?;
        // Write magic + version at the start of every TsFile.
        file.write_all(TSFILE_MAGIC)?;
        serialize::write_u8(&mut file, VERSION_NUMBER)?;
        Ok(Self {
            file,
            chunk_group_metas: Vec::new(),
            current_chunk_metas: Vec::new(),
            current_device: None,
            current_chunk_offset: 0,
            current_chunk_header: None,
            table_schemas: Vec::new(),
            config,
        })
    }

    // -----------------------------------------------------------------------
    // Chunk group lifecycle
    // -----------------------------------------------------------------------

    /// Begin a new chunk group for `device_id`.
    ///
    /// C++ start_flush_chunk_group() just stores the device pointer. Here we
    /// store a clone of the DeviceId and clear the in-progress chunk meta list.
    pub fn start_chunk_group(&mut self, device_id: &DeviceId) -> Result<()> {
        if self.current_device.is_some() {
            return Err(TsFileError::InvalidArg(
                "previous chunk group not closed before start_chunk_group".into(),
            ));
        }
        self.current_device = Some(device_id.clone());
        self.current_chunk_metas.clear();
        Ok(())
    }

    /// Finalize the current chunk group: write the footer marker + device string
    /// to the file, then record a completed ChunkGroupMeta.
    pub fn end_chunk_group(&mut self) -> Result<()> {
        let device_id = self
            .current_device
            .take()
            .ok_or_else(|| TsFileError::InvalidArg("end_chunk_group without start".into()))?;

        // Chunk group footer: marker byte + device path string.
        serialize::write_u8(&mut self.file, CHUNK_GROUP_FOOTER_MARKER)?;
        serialize::write_string(&mut self.file, &device_id.to_string())?;

        let mut group_meta = ChunkGroupMeta::new(device_id);
        for cm in std::mem::take(&mut self.current_chunk_metas) {
            group_meta.add_chunk_meta(cm);
        }
        self.chunk_group_metas.push(group_meta);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Chunk lifecycle
    // -----------------------------------------------------------------------

    /// Write a chunk header to the file and record the file offset for later
    /// ChunkMeta construction.
    ///
    /// The `header.data_size` must equal the total bytes of page data that
    /// will follow via `write_page_data()` calls. The ChunkWriter is
    /// responsible for computing this before calling start_chunk.
    pub fn start_chunk(&mut self, header: &ChunkHeader) -> Result<()> {
        self.current_chunk_offset = self.file.position();
        header.serialize_to(&mut self.file)?;
        self.current_chunk_header = Some(header.clone());
        Ok(())
    }

    /// Write raw page bytes (PageHeader + compressed data, already serialized
    /// by PageWriter) directly to the file.
    pub fn write_page_data(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_all(data)?;
        Ok(())
    }

    /// Finalize the current chunk: build a ChunkMeta from the header and the
    /// provided aggregate statistic, add it to the current chunk group.
    pub fn end_chunk(&mut self, statistic: &Statistic) -> Result<()> {
        let header = self
            .current_chunk_header
            .take()
            .ok_or_else(|| TsFileError::InvalidArg("end_chunk without start_chunk".into()))?;

        let mask = chunk_header_mask(&header);
        let meta = ChunkMeta::new(
            header.measurement_name.clone(),
            header.data_type,
            self.current_chunk_offset as i64,
            statistic.clone(),
            mask,
        );
        self.current_chunk_metas.push(meta);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Table schema registration
    // -----------------------------------------------------------------------

    /// Register a table schema (table model only). Stored in TsFileMeta.
    pub fn add_table_schema(&mut self, schema: TableSchema) {
        self.table_schemas.push(schema);
    }

    // -----------------------------------------------------------------------
    // File finalization
    // -----------------------------------------------------------------------

    /// Write the metadata index tree, TsFileMeta, and footer; flush to disk.
    ///
    /// C++ TsFileIOWriter::end_file() calls write_separator(), then builds
    /// the MetaIndexNode tree bottom-up, then writes TsFileMeta + footer.
    /// This method replicates that sequence.
    pub fn end_file(&mut self) -> Result<()> {
        // SEPARATOR_MARKER marks the start of the metadata section.
        // The meta_offset stored in the footer points here.
        let meta_offset = self.file.position() as i64;
        serialize::write_u8(&mut self.file, SEPARATOR_MARKER)?;

        // Build bloom filter from all device IDs.
        let bloom = self.build_bloom_filter();

        // Write metadata index tree (TimeseriesIndexes + MetaIndexNodes).
        let root_node_offset = self.write_metadata_index()?;

        // Serialize TsFileMeta to a temporary buffer so we know its size before
        // writing. The size is written immediately after TsFileMeta as a u32,
        // allowing the reader to locate TsFileMeta by reading backwards from
        // the footer: footer_start - 4 bytes = size, footer_start - 4 - size = start.
        let ts_file_meta = TsFileMeta {
            meta_offset,
            table_schema_map: self.table_schemas.clone(),
            bloom_filter: Some(bloom.to_bytes()),
        };
        let mut meta_buf: Vec<u8> = Vec::new();
        ts_file_meta.serialize_to(&mut meta_buf)?;
        let ts_meta_size = meta_buf.len() as u32;

        self.file.write_all(&meta_buf)?;
        // Write 4-byte size prefix just before the footer so the reader can
        // find TsFileMeta without scanning.
        self.file.write_all(&ts_meta_size.to_be_bytes())?;

        // Footer: meta_offset (i64, big-endian) + magic bytes.
        // The reader locates the footer at the last 14 bytes of the file.
        serialize::write_i64(&mut self.file, meta_offset)?;
        self.file.write_all(TSFILE_MAGIC)?;

        // Suppress unused-variable warning; root_node_offset is informational.
        let _ = root_node_offset;

        self.file.flush()?;
        Ok(())
    }

    /// Current write position (bytes written to file so far).
    pub fn position(&self) -> u64 {
        self.file.position()
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Build a Bloom filter covering every distinct device in chunk_group_metas.
    fn build_bloom_filter(&self) -> BloomFilter {
        let device_count = self
            .chunk_group_metas
            .iter()
            .map(|g| &g.device_id)
            .collect::<std::collections::HashSet<_>>()
            .len()
            .max(1);
        let mut bf = BloomFilter::with_capacity(
            device_count,
            self.config.tsfile_index_bloom_filter_error_percent,
        );
        for group in &self.chunk_group_metas {
            bf.add(&group.device_id.to_string());
        }
        bf
    }

    /// Write TimeseriesIndex entries and MetaIndexNode tree to the file.
    /// Returns the file offset of the root MetaIndexNode.
    ///
    /// Layout written (bottom-up):
    ///   [TimeseriesIndex per (device, measurement)] ...
    ///   [LeafMeasurement MetaIndexNode per device] ...
    ///   [InternalMeasurement nodes if needed] ...
    ///   [LeafDevice or InternalDevice root node]
    fn write_metadata_index(&mut self) -> Result<u64> {
        // Collect all ChunkMetas grouped by (device_id, measurement_name).
        // Use BTreeMap for deterministic (sorted) output, matching C++ behaviour.
        let mut by_device: BTreeMap<DeviceId, BTreeMap<String, Vec<ChunkMeta>>> = BTreeMap::new();

        // Clone here to avoid borrow conflict when writing to self.file later.
        let groups: Vec<ChunkGroupMeta> = self.chunk_group_metas.clone();

        for group in groups {
            let meas_map = by_device.entry(group.device_id.clone()).or_default();
            for cm in group.chunk_meta_list {
                meas_map
                    .entry(cm.measurement_name.clone())
                    .or_default()
                    .push(cm);
            }
        }

        let max_degree = self.config.max_degree_of_index_node as usize;

        // Write one TimeseriesIndex per (device, measurement) and collect
        // the device-level MetaIndexEntries.
        let mut device_entries: Vec<MetaIndexEntry> = Vec::new();

        for (device_id, meas_map) in &by_device {
            // Write TimeseriesIndex for each measurement and collect entries.
            let mut meas_entries: Vec<MetaIndexEntry> = Vec::new();
            for (meas_name, chunks) in meas_map {
                let ts_index = build_timeseries_index(meas_name.clone(), chunks);
                let offset = self.file.position() as i64;
                ts_index.serialize_to(&mut self.file)?;
                meas_entries.push(MetaIndexEntry::new(meas_name.clone(), offset));
            }

            // Build measurement-level MetaIndexNode tree; returns offset of first node.
            let first_meas_node_offset = self.write_index_tree_level(
                meas_entries,
                MetaIndexNodeType::LeafMeasurement,
                MetaIndexNodeType::InternalMeasurement,
                max_degree,
            )?;

            device_entries.push(MetaIndexEntry::new(
                device_id.to_string(),
                first_meas_node_offset as i64,
            ));
        }

        // Build device-level MetaIndexNode tree; root node is last written.
        let root_offset = self.write_index_tree_level(
            device_entries,
            MetaIndexNodeType::LeafDevice,
            MetaIndexNodeType::InternalDevice,
            max_degree,
        )?;

        Ok(root_offset)
    }

    /// Write a level of the MetaIndexNode tree for the given entries.
    ///
    /// If `entries.len() <= max_degree`, write one leaf node and return its
    /// file offset. Otherwise group entries into blocks of `max_degree`,
    /// write leaf nodes for each block, then recursively build an internal
    /// level above them — matching C++ MetaIndexNode grouping logic.
    ///
    /// `end_offset` of each node equals the file position at the moment the
    /// node is written (it points to itself), which is the upper bound for
    /// all of that node's children in the file. The reader uses this to
    /// bound binary searches within the node's subtree.
    fn write_index_tree_level(
        &mut self,
        entries: Vec<MetaIndexEntry>,
        leaf_type: MetaIndexNodeType,
        internal_type: MetaIndexNodeType,
        max_degree: usize,
    ) -> Result<u64> {
        if entries.len() <= max_degree {
            // Single leaf node covers all entries.
            let node_offset = self.file.position();
            let node = MetaIndexNode {
                children: entries,
                node_type: leaf_type,
                end_offset: node_offset as i64,
            };
            node.serialize_to(&mut self.file)?;
            return Ok(node_offset);
        }

        // Multiple leaf nodes needed; build a parent level above them.
        let mut parent_entries: Vec<MetaIndexEntry> = Vec::new();
        let first_child_name = entries[0].name.clone(); // for parent entry name

        for chunk in entries.chunks(max_degree) {
            let node_offset = self.file.position();
            let node = MetaIndexNode {
                children: chunk.to_vec(),
                node_type: leaf_type,
                end_offset: node_offset as i64,
            };
            node.serialize_to(&mut self.file)?;
            // Parent entry points to this node using the first child's name.
            let first_name = chunk[0].name.clone();
            parent_entries.push(MetaIndexEntry::new(first_name, node_offset as i64));
        }

        // Suppress unused binding; first_child_name was used above.
        let _ = first_child_name;

        // Recurse to build the internal level.
        self.write_index_tree_level(parent_entries, internal_type, internal_type, max_degree)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Derive the ChunkMeta mask byte from a chunk header's marker byte.
///
/// C++ sets the mask in ChunkMeta to distinguish aligned vs non-aligned chunks
/// in the metadata index. The marker byte in the ChunkHeader carries the same
/// information, so we map marker → mask here.
fn chunk_header_mask(header: &ChunkHeader) -> u8 {
    if header.is_time_chunk() {
        CHUNK_TYPE_ALIGNED_TIME_MASK
    } else if header.is_value_chunk() {
        CHUNK_TYPE_ALIGNED_VALUE_MASK
    } else {
        CHUNK_TYPE_NON_ALIGNED_MASK
    }
}

/// Build a TimeseriesIndex from a slice of ChunkMeta entries for one
/// (device, measurement) combination. Merges statistics across chunks.
fn build_timeseries_index(measurement_name: String, chunks: &[ChunkMeta]) -> TimeseriesIndex {
    debug_assert!(!chunks.is_empty());
    let data_type = chunks[0].data_type;
    let mut ts_index = TimeseriesIndex::new(measurement_name, data_type);
    let mut merged_stat = chunks[0].statistic.clone();
    for cm in chunks {
        merged_stat.merge(&cm.statistic);
        ts_index.chunk_meta_list.push(cm.clone());
    }
    ts_index.statistic = merged_stat;
    ts_index
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CompressionType, TSDataType, TSEncoding};
    use crate::tsfile_format::{
        CHUNK_HEADER_MARKER, ONLY_ONE_PAGE_CHUNK_HEADER_MARKER, TIME_CHUNK_HEADER_MARKER,
        VALUE_CHUNK_HEADER_MARKER,
    };
    use tempfile::tempdir;

    fn default_config() -> Arc<Config> {
        Arc::new(Config::default())
    }

    #[test]
    fn write_empty_file() {
        // A minimal TsFile: just magic+version header + separator + empty index + footer.
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.tsfile");
        let mut w = TsFileIOWriter::new(&path, default_config()).unwrap();
        w.end_file().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        // Starts with magic + version
        assert_eq!(&bytes[..6], TSFILE_MAGIC);
        assert_eq!(bytes[6], VERSION_NUMBER);
        // Ends with magic
        assert_eq!(&bytes[bytes.len() - 6..], TSFILE_MAGIC);
    }

    #[test]
    fn chunk_header_mask_regular() {
        let h = ChunkHeader::new(
            CHUNK_HEADER_MARKER,
            "s1".into(),
            10,
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
        );
        assert_eq!(chunk_header_mask(&h), CHUNK_TYPE_NON_ALIGNED_MASK);
    }

    #[test]
    fn chunk_header_mask_time() {
        let h = ChunkHeader::new(
            TIME_CHUNK_HEADER_MARKER,
            "".into(),
            10,
            TSDataType::Int64,
            TSEncoding::Ts2Diff,
            CompressionType::Lz4,
        );
        assert_eq!(chunk_header_mask(&h), CHUNK_TYPE_ALIGNED_TIME_MASK);
    }

    #[test]
    fn chunk_header_mask_value() {
        let h = ChunkHeader::new(
            VALUE_CHUNK_HEADER_MARKER,
            "s1".into(),
            10,
            TSDataType::Float,
            TSEncoding::Gorilla,
            CompressionType::Snappy,
        );
        assert_eq!(chunk_header_mask(&h), CHUNK_TYPE_ALIGNED_VALUE_MASK);
    }

    #[test]
    fn start_chunk_group_twice_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("t.tsfile");
        let mut w = TsFileIOWriter::new(&path, default_config()).unwrap();
        let dev = DeviceId::parse("root.sg1.d1").unwrap();
        w.start_chunk_group(&dev).unwrap();
        assert!(w.start_chunk_group(&dev).is_err());
    }

    #[test]
    fn end_chunk_without_start_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("t.tsfile");
        let mut w = TsFileIOWriter::new(&path, default_config()).unwrap();
        let stat = Statistic::new(TSDataType::Int32);
        assert!(w.end_chunk(&stat).is_err());
    }

    #[test]
    fn write_single_chunk_group() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("single.tsfile");
        let mut w = TsFileIOWriter::new(&path, default_config()).unwrap();

        let dev = DeviceId::parse("root.sg1.d1").unwrap();
        w.start_chunk_group(&dev).unwrap();

        // Write a fake chunk: header + one "page" (just 4 bytes for the test).
        let header = ChunkHeader::new(
            ONLY_ONE_PAGE_CHUNK_HEADER_MARKER,
            "temperature".into(),
            4, // data_size
            TSDataType::Float,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
        );
        w.start_chunk(&header).unwrap();
        w.write_page_data(&[0x01, 0x02, 0x03, 0x04]).unwrap();
        let stat = Statistic::new(TSDataType::Float);
        w.end_chunk(&stat).unwrap();

        w.end_chunk_group().unwrap();
        w.end_file().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes[..6], TSFILE_MAGIC);
        assert_eq!(&bytes[bytes.len() - 6..], TSFILE_MAGIC);
    }
}
