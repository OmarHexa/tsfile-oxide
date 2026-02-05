// On-disk format structures (tsfile_common.h): ChunkHeader, PageHeader,
// ChunkMeta, ChunkGroupMeta, MetaIndexNode, TsFileMeta. These must
// serialize to the exact same byte layout as C++ for cross-reader
// compatibility. Each struct implements serialize/deserialize using
// the varint encoding from serialize.rs.
//
// ===========================================================================
// TsFile V4 On-Disk Layout Detailed Specification
// ===========================================================================
//
// A TsFile is structured into three main sections: Data, Metadata, and Footer.
// The TsFile on-disk layout (V4):
//   [Magic "TsFile"] [Version u8]
//   [ChunkGroup 1: marker + ChunkHeader + Pages ...]
//   [ChunkGroup 2: ...]
//   [SEPARATOR marker]
//   [MetaIndex tree: leaf nodes ... internal nodes ... root node]
//   [TsFileMeta: table schemas, bloom filter, meta_offset]
//   [Magic "TsFile"]
//
// 1. DATA SECTION
// ---------------
// Contains one or more ChunkGroups. Each ChunkGroup belongs to a single Device.
// Each chunk group == device and each chunk == measurement (current, voltage etc).
//
// [Magic "TsFile"] (6 bytes)
// [Version u8]     (1 byte, currently 4)
//
// [ChunkGroup 1]
//   [Chunk 1]
//     [ChunkHeader]
//       - marker (u8): identifies type (e.g., 1=regular, 8=time, 6=value)
//       - measurement_id (string): name of the timeseries
//       - data_size (i32): bytes of page data to follow
//       - data_type (u8): TSDataType (Boolean, Int32, Int64, Float, Double, Text, Vector)
//       - encoding (u8): TSEncoding (Plain, Dictionary, RLE, Diff, TS_2DIFF, GORILLA, ZIGZAG)
//       - compression (u8): CompressionType (Uncompressed, Snappy, GZIP, LZ4, ZSTD)
//     [Page 1]
//       [PageHeader]
//         - uncompressed_size (i32)
//         - compressed_size (i32)
//         - statistics (Statistic): min, max, sum, first, last, count
//       [PageData] (compressed bytes)
//     [Page 2...]
//   [Chunk 2...]
//   [ChunkGroupFooter]
//     - marker (u8): 0 (CHUNK_GROUP_FOOTER_MARKER)
//     - device_id (string): the device name
//
// [ChunkGroup 2...]
//
// 2. METADATA SECTION
// -------------------
// Begins at the file position indicated by 'meta_offset' in the footer.
// It consists of a B-Tree like structure (MetaIndexNodes) and TsFileMeta.
//
// [SEPARATOR marker] (u8: 2)
//
// [Metadata Index Tree]
//   Each node is a MetaIndexNode. The tree is walked from the root (the last
//   node serialized in the file before TsFileMeta) down to leaves.
//
//   Node Types & Hierarchy:
//   - InternalDevice: Maps device name ranges to child InternalDevice or LeafDevice nodes.
//   - LeafDevice: Maps device name to InternalMeasurement or LeafMeasurement nodes.
//   - InternalMeasurement: Maps measurement name ranges to child measurement nodes.
//   - LeafMeasurement: Maps measurement names to TimeseriesIndex structures.
//
// [TimeseriesIndex]
//   For each timeseries, stores a list of ChunkMeta entries and the merged
//   aggregate statistics for the entire timeseries across all chunks.
//
// [TsFileMeta]
//   - table_schemas: List of table definitions (for table model).
//   - bloom_filter: Optional bitset for fast lookup of device existence.
//   - meta_offset (i64): File position where the Metadata Section starts (at SEPARATOR).
//
// 3. FOOTER
// ---------
// [meta_offset i64] (8 bytes) - points to Metadata Section start (the SEPARATOR_MARKER)
// [Magic "TsFile"] (6 bytes)
//
// 4. SERIALIZATION LAYOUTS
// ------------------------
//
// MetaIndexNode:
//   [num_children i32]
//   [MetaIndexEntry 1: name (string), offset (i64)]
//   [MetaIndexEntry 2: ...]
//   [node_type u8] (0:InternalDevice, 1:LeafDevice, 2:InternalMeasurement, 3:LeafMeasurement)
//   [end_offset i64]
//
// TimeseriesIndex:
//   [measurement_name string]
//   [data_type u8]
//   [chunk_meta_data_size i32]
//   [ChunkMeta 1: measurement (string), type (u8), offset (i64), stats (Statistic), mask (u8)]
//   [ChunkMeta 2: ...]
//   [statistics (Statistic)] (merged stats for all chunks)
//
// ChunkHeader:
//   [marker u8] [measurement string] [data_size i32] [type u8] [encoding u8] [comp u8]
//
// PageHeader:
//   [uncompressed_size i32] [compressed_size i32] [statistics (Statistic)]
//   (Statistics omitted for single-page chunks)
//
// ===========================================================================
// SAMPLE TSFILE LAYOUT (Pseudo-Data Example)
// ===========================================================================
//
// Imagine a file recording "root.sg1.d1" with measurements "s1" (Int32) and
// "s2" (Float). "s1" has 2 pages of data.
//
// Offset | Content                 | Description
// -------|-------------------------|------------------------------------------
// 0      | "TsFile"                | Magic Header
// 6      | 0x04                    | Version Number
// -------|-------------------------|------------------------------------------
// 7      | 0x01                    | CHUNK_HEADER_MARKER
// 8      | "s1"                    | Measurement ID
// 12     | 150                     | data_size (sum of page headers + data)
// 16     | 0x01                    | data_type: Int32 (1)
// 17     | 0x00                    | encoding: Plain (0)
// 18     | 0x01                    | compression: Snappy (1)
// -------|-------------------------|------------------------------------------
// 19     | 40, 32, [Stats...]      | Page 1 Header
// 51     | [32 bytes...]           | Page 1 Data (Compressed)
// -------|-------------------------|------------------------------------------
// 83     | 40, 35, [Stats...]      | Page 2 Header
// 115    | [35 bytes...]           | Page 2 Data
// -------|-------------------------|------------------------------------------
// 150    | 0x00                    | CHUNK_GROUP_FOOTER_MARKER
// 151    | "root.sg1.d1"           | Device ID
// -------|-------------------------|------------------------------------------
// 170    | 0x02                    | SEPARATOR_MARKER (Metadata Section Starts)
// -------|-------------------------|------------------------------------------
// 171    | [TimeseriesIndex "s1"]  | ChunkMetas list + Merged Stats for "s1"
// 300    | [TimeseriesIndex "s2"]  | (Similar for measurement s2)
// -------|-------------------------|------------------------------------------
// 430    | [MetaIndexNode]         | LeafMeasurement Node:
//        |   - children: ["s1" -> 171, "s2" -> 300]
//        |   - type: LeafMeasurement (3)
// -------|-------------------------|------------------------------------------
// 500    | [MetaIndexNode]         | LeafDevice Node (The Tree Root):
//        |   - children: ["root.sg1.d1" -> 430]
//        |   - type: LeafDevice (1)
// -------|-------------------------|------------------------------------------
// 550    | [TsFileMeta]            | table_schemas, bloom_filter, meta_offset=170
// -------|-------------------------|------------------------------------------
// 600    | 170                     | meta_offset (i64)
// 608    | "TsFile"                | Magic Footer
// 614    | EOF                     | Total File Size: 614 bytes
// ===========================================================================

use crate::device_id::DeviceId;
use crate::error::{Result, TsFileError};
use crate::schema::TableSchema;
use crate::serialize;
use crate::statistic::Statistic;
use crate::types::{CompressionType, TSDataType, TSEncoding};
use std::io::{Read, Write};

// ===========================================================================
// File format constants
// ===========================================================================

/// Magic bytes at the start and end of every TsFile.
pub const TSFILE_MAGIC: &[u8] = b"TsFile";

/// Format version number (V4). Must match C++ for cross-reader compatibility.
pub const VERSION_NUMBER: u8 = 4;

// ---------------------------------------------------------------------------
// Chunk type markers. These appear as the first byte before each chunk
// header on disk, allowing the reader to identify the chunk type during
// sequential scan. Values match the Java/C++ MetaMarkerType constants.
// ---------------------------------------------------------------------------

/// Regular (non-aligned) chunk with multiple pages.
pub const CHUNK_HEADER_MARKER: u8 = 1;

/// Regular chunk with exactly one page (statistics inline in chunk header).
pub const ONLY_ONE_PAGE_CHUNK_HEADER_MARKER: u8 = 5;

/// Aligned time chunk with multiple pages.
pub const TIME_CHUNK_HEADER_MARKER: u8 = 8;

/// Aligned time chunk with exactly one page.
pub const ONLY_ONE_PAGE_TIME_CHUNK_HEADER_MARKER: u8 = 9;

/// Aligned value chunk with multiple pages.
pub const VALUE_CHUNK_HEADER_MARKER: u8 = 6;

/// Aligned value chunk with exactly one page.
pub const ONLY_ONE_PAGE_VALUE_CHUNK_HEADER_MARKER: u8 = 7;

/// Marks the end of a chunk group (device boundary).
pub const CHUNK_GROUP_FOOTER_MARKER: u8 = 0;

/// Separates the data section from the metadata section.
pub const SEPARATOR_MARKER: u8 = 2;

/// Operation index range marker.
pub const OPERATION_INDEX_RANGE_MARKER: u8 = 4;

// ---------------------------------------------------------------------------
// ChunkMeta mask values — identify aligned vs non-aligned chunks in the
// metadata index. The mask byte is stored per-ChunkMeta entry.
// ---------------------------------------------------------------------------

/// Regular (non-aligned) chunk.
pub const CHUNK_TYPE_NON_ALIGNED_MASK: u8 = 0;

/// Aligned value chunk.
pub const CHUNK_TYPE_ALIGNED_VALUE_MASK: u8 = 1;

/// Aligned time chunk.
pub const CHUNK_TYPE_ALIGNED_TIME_MASK: u8 = 3;

// ===========================================================================
// MetaIndexNodeType
// ===========================================================================

/// Node type in the metadata B-tree-like index.
///
/// The index tree has two levels: device-level nodes route to the correct
/// device, and measurement-level nodes route to the correct timeseries
/// within a device. Each level can have INTERNAL (routing) or LEAF
/// (data-bearing) nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MetaIndexNodeType {
    InternalDevice = 0,
    LeafDevice = 1,
    InternalMeasurement = 2,
    LeafMeasurement = 3,
}

impl TryFrom<u8> for MetaIndexNodeType {
    type Error = TsFileError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::InternalDevice),
            1 => Ok(Self::LeafDevice),
            2 => Ok(Self::InternalMeasurement),
            3 => Ok(Self::LeafMeasurement),
            _ => Err(TsFileError::InvalidArg(format!(
                "unknown MetaIndexNodeType discriminant: {value}"
            ))),
        }
    }
}

// ===========================================================================
// ChunkHeader
// ===========================================================================

/// On-disk header preceding each chunk's page data.
///
/// C++ ChunkHeader stores a marker byte (chunk type), measurement name,
/// total data size, data type, encoding, and compression. The marker byte
/// distinguishes regular chunks from aligned time/value chunks, and
/// single-page from multi-page chunks.
///
/// Serialization format:
///   marker (u8) + measurement_name (string) + data_size (i32) +
///   data_type (u8) + encoding (u8) + compression (u8)
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkHeader {
    /// Chunk type marker (one of the *_MARKER constants above).
    pub marker: u8,
    pub measurement_name: String,
    /// Total byte size of all page data in this chunk.
    pub data_size: u32,
    pub data_type: TSDataType,
    pub encoding: TSEncoding,
    pub compression: CompressionType,
}

impl ChunkHeader {
    pub fn new(
        marker: u8,
        measurement_name: String,
        data_size: u32,
        data_type: TSDataType,
        encoding: TSEncoding,
        compression: CompressionType,
    ) -> Self {
        Self {
            marker,
            measurement_name,
            data_size,
            data_type,
            encoding,
            compression,
        }
    }

    /// Returns true if this chunk contains exactly one page (statistics
    /// are stored in the chunk header rather than per-page).
    pub fn is_single_page(&self) -> bool {
        matches!(
            self.marker,
            ONLY_ONE_PAGE_CHUNK_HEADER_MARKER
                | ONLY_ONE_PAGE_TIME_CHUNK_HEADER_MARKER
                | ONLY_ONE_PAGE_VALUE_CHUNK_HEADER_MARKER
        )
    }

    /// Returns true if this is an aligned time chunk.
    pub fn is_time_chunk(&self) -> bool {
        matches!(
            self.marker,
            TIME_CHUNK_HEADER_MARKER | ONLY_ONE_PAGE_TIME_CHUNK_HEADER_MARKER
        )
    }

    /// Returns true if this is an aligned value chunk.
    pub fn is_value_chunk(&self) -> bool {
        matches!(
            self.marker,
            VALUE_CHUNK_HEADER_MARKER | ONLY_ONE_PAGE_VALUE_CHUNK_HEADER_MARKER
        )
    }

    /// Returns true if this is a regular (non-aligned) chunk.
    pub fn is_regular_chunk(&self) -> bool {
        matches!(
            self.marker,
            CHUNK_HEADER_MARKER | ONLY_ONE_PAGE_CHUNK_HEADER_MARKER
        )
    }

    pub fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        serialize::write_u8(w, self.marker)?;
        serialize::write_string(w, &self.measurement_name)?;
        serialize::write_i32(w, self.data_size as i32)?;
        serialize::write_u8(w, self.data_type as u8)?;
        serialize::write_u8(w, self.encoding as u8)?;
        serialize::write_u8(w, self.compression as u8)?;
        Ok(())
    }

    /// Deserialize from reader. Reads the marker byte first.
    pub fn deserialize_from(r: &mut impl Read) -> Result<Self> {
        let marker = serialize::read_u8(r)?;
        Self::deserialize_with_marker(r, marker)
    }

    /// Deserialize from reader given an already-consumed marker byte.
    /// This is useful when the caller has already peeked at the marker
    /// to determine the chunk type (e.g., during sequential file scan).
    pub fn deserialize_with_marker(r: &mut impl Read, marker: u8) -> Result<Self> {
        let measurement_name = serialize::read_string(r)?;
        let data_size = serialize::read_i32(r)? as u32;
        let data_type = TSDataType::try_from(serialize::read_u8(r)?)?;
        let encoding = TSEncoding::try_from(serialize::read_u8(r)?)?;
        let compression = CompressionType::try_from(serialize::read_u8(r)?)?;
        Ok(Self {
            marker,
            measurement_name,
            data_size,
            data_type,
            encoding,
            compression,
        })
    }
}

// ===========================================================================
// PageHeader
// ===========================================================================

/// On-disk header for a single page within a chunk.
///
/// C++ PageHeader contains compressed/uncompressed sizes and optional
/// per-page statistics. For single-page chunks (marker = ONLY_ONE_PAGE_*),
/// statistics live in the ChunkMeta instead and the PageHeader omits them.
///
/// Serialization format:
///   uncompressed_size (i32) + compressed_size (i32) + [statistic]
#[derive(Debug, Clone, PartialEq)]
pub struct PageHeader {
    pub uncompressed_size: i32,
    pub compressed_size: i32,
    /// Per-page statistics. Present for multi-page chunks; absent for
    /// single-page chunks where the ChunkMeta holds the statistics.
    pub statistic: Option<Statistic>,
}

impl PageHeader {
    pub fn new(uncompressed_size: i32, compressed_size: i32, statistic: Option<Statistic>) -> Self {
        Self {
            uncompressed_size,
            compressed_size,
            statistic,
        }
    }

    pub fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        serialize::write_i32(w, self.uncompressed_size)?;
        serialize::write_i32(w, self.compressed_size)?;
        if let Some(ref stat) = self.statistic {
            stat.serialize_to(w)?;
        }
        Ok(())
    }

    /// Deserialize a page header. The caller specifies:
    /// - `data_type`: for statistic deserialization (type must be known from chunk header)
    /// - `has_statistic`: true for multi-page chunks, false for single-page
    pub fn deserialize_from(
        r: &mut impl Read,
        data_type: TSDataType,
        has_statistic: bool,
    ) -> Result<Self> {
        let uncompressed_size = serialize::read_i32(r)?;
        let compressed_size = serialize::read_i32(r)?;
        let statistic = if has_statistic {
            Some(Statistic::deserialize_from(r, data_type)?)
        } else {
            None
        };
        Ok(Self {
            uncompressed_size,
            compressed_size,
            statistic,
        })
    }
}

// ===========================================================================
// ChunkMeta
// ===========================================================================

/// Index entry for a single chunk in the metadata section.
///
/// C++ ChunkMeta stores the measurement name, data type, file offset to
/// the chunk header, aggregate statistics across all pages, and a mask
/// byte indicating aligned vs non-aligned chunk type.
///
/// Serialization format:
///   measurement_name (string) + data_type (u8) +
///   offset_of_chunk_header (i64) + statistic + mask (u8)
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkMeta {
    pub measurement_name: String,
    pub data_type: TSDataType,
    /// Byte offset of the ChunkHeader in the file.
    pub offset_of_chunk_header: i64,
    /// Aggregate statistics across all pages in this chunk.
    pub statistic: Statistic,
    /// Bitmask: 0 = non-aligned, 1 = aligned value, 3 = aligned time.
    pub mask: u8,
}

impl ChunkMeta {
    pub fn new(
        measurement_name: String,
        data_type: TSDataType,
        offset_of_chunk_header: i64,
        statistic: Statistic,
        mask: u8,
    ) -> Self {
        Self {
            measurement_name,
            data_type,
            offset_of_chunk_header,
            statistic,
            mask,
        }
    }

    /// Returns true if this chunk is part of an aligned timeseries.
    pub fn is_aligned(&self) -> bool {
        self.mask != CHUNK_TYPE_NON_ALIGNED_MASK
    }

    /// Returns true if this is the time column of an aligned timeseries.
    pub fn is_time_chunk(&self) -> bool {
        self.mask == CHUNK_TYPE_ALIGNED_TIME_MASK
    }

    /// Returns true if this is a value column of an aligned timeseries.
    pub fn is_value_chunk(&self) -> bool {
        self.mask == CHUNK_TYPE_ALIGNED_VALUE_MASK
    }

    pub fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        serialize::write_string(w, &self.measurement_name)?;
        serialize::write_u8(w, self.data_type as u8)?;
        serialize::write_i64(w, self.offset_of_chunk_header)?;
        self.statistic.serialize_to(w)?;
        serialize::write_u8(w, self.mask)?;
        Ok(())
    }

    pub fn deserialize_from(r: &mut impl Read) -> Result<Self> {
        let measurement_name = serialize::read_string(r)?;
        let data_type = TSDataType::try_from(serialize::read_u8(r)?)?;
        let offset_of_chunk_header = serialize::read_i64(r)?;
        let statistic = Statistic::deserialize_from(r, data_type)?;
        let mask = serialize::read_u8(r)?;
        Ok(Self {
            measurement_name,
            data_type,
            offset_of_chunk_header,
            statistic,
            mask,
        })
    }
}

// ===========================================================================
// ChunkGroupMeta
// ===========================================================================

/// Groups ChunkMeta entries by device. This is primarily an in-memory
/// structure built during writing and used to construct the on-disk index.
///
/// C++ ChunkGroupMeta holds a DeviceId pointer and a vector of ChunkMeta
/// pointers (raw, manually managed). In Rust we own everything by value.
#[derive(Debug, Clone)]
pub struct ChunkGroupMeta {
    pub device_id: DeviceId,
    pub chunk_meta_list: Vec<ChunkMeta>,
}

impl ChunkGroupMeta {
    pub fn new(device_id: DeviceId) -> Self {
        Self {
            device_id,
            chunk_meta_list: Vec::new(),
        }
    }

    pub fn add_chunk_meta(&mut self, chunk_meta: ChunkMeta) {
        self.chunk_meta_list.push(chunk_meta);
    }

    pub fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        self.device_id.serialize(w)?;
        serialize::write_i32(w, self.chunk_meta_list.len() as i32)?;
        for cm in &self.chunk_meta_list {
            cm.serialize_to(w)?;
        }
        Ok(())
    }

    pub fn deserialize_from(r: &mut impl Read) -> Result<Self> {
        let device_id = DeviceId::deserialize(r)?;
        let count = serialize::read_i32(r)? as usize;
        let mut chunk_meta_list = Vec::with_capacity(count);
        for _ in 0..count {
            chunk_meta_list.push(ChunkMeta::deserialize_from(r)?);
        }
        Ok(Self {
            device_id,
            chunk_meta_list,
        })
    }
}

// ===========================================================================
// TimeseriesIndex
// ===========================================================================

/// Maps a measurement to its chunk metadata entries and aggregate statistics.
/// Used during metadata index construction.
///
/// C++ TimeseriesIndex serializes the chunk metadata list as a sized blob
/// (i32 byte-length prefix + serialized ChunkMeta entries), followed by
/// the merged statistic. This allows the reader to skip the chunk metadata
/// blob if only the aggregate statistic is needed.
///
/// Serialization format:
///   measurement_name (string) + data_type (u8) +
///   chunk_meta_data_size (i32) + [chunk_metas...] + statistic
#[derive(Debug, Clone, PartialEq)]
pub struct TimeseriesIndex {
    pub measurement_name: String,
    pub data_type: TSDataType,
    pub chunk_meta_list: Vec<ChunkMeta>,
    /// Aggregate statistic merged across all chunks for this timeseries.
    pub statistic: Statistic,
}

impl TimeseriesIndex {
    pub fn new(measurement_name: String, data_type: TSDataType) -> Self {
        let statistic = Statistic::new(data_type);
        Self {
            measurement_name,
            data_type,
            chunk_meta_list: Vec::new(),
            statistic,
        }
    }

    pub fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        serialize::write_string(w, &self.measurement_name)?;
        serialize::write_u8(w, self.data_type as u8)?;

        // Serialize chunk metas into a temporary buffer to compute the
        // byte-length prefix that C++ readers expect.
        let mut chunk_meta_buf = Vec::new();
        for cm in &self.chunk_meta_list {
            cm.serialize_to(&mut chunk_meta_buf)?;
        }
        serialize::write_i32(w, chunk_meta_buf.len() as i32)?;
        w.write_all(&chunk_meta_buf)?;

        self.statistic.serialize_to(w)?;
        Ok(())
    }

    pub fn deserialize_from(r: &mut impl Read) -> Result<Self> {
        let measurement_name = serialize::read_string(r)?;
        let data_type = TSDataType::try_from(serialize::read_u8(r)?)?;

        // Read the chunk meta data blob, then parse ChunkMeta entries from it.
        let chunk_meta_data_size = serialize::read_i32(r)? as usize;
        let mut chunk_meta_data = vec![0u8; chunk_meta_data_size];
        r.read_exact(&mut chunk_meta_data)?;

        let mut chunk_meta_reader = std::io::Cursor::new(&chunk_meta_data);
        let mut chunk_meta_list = Vec::new();
        while (chunk_meta_reader.position() as usize) < chunk_meta_data_size {
            chunk_meta_list.push(ChunkMeta::deserialize_from(&mut chunk_meta_reader)?);
        }

        let statistic = Statistic::deserialize_from(r, data_type)?;
        Ok(Self {
            measurement_name,
            data_type,
            chunk_meta_list,
            statistic,
        })
    }
}

// ===========================================================================
// MetaIndexEntry
// ===========================================================================

/// A single entry in a MetaIndexNode: name + file offset.
///
/// For device-level nodes, name is the device path string.
/// For measurement-level nodes, name is the measurement name.
///
/// Serialization format: name (string) + offset (i64)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaIndexEntry {
    pub name: String,
    pub offset: i64,
}

impl MetaIndexEntry {
    pub fn new(name: String, offset: i64) -> Self {
        Self { name, offset }
    }

    pub fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        serialize::write_string(w, &self.name)?;
        serialize::write_i64(w, self.offset)?;
        Ok(())
    }

    pub fn deserialize_from(r: &mut impl Read) -> Result<Self> {
        let name = serialize::read_string(r)?;
        let offset = serialize::read_i64(r)?;
        Ok(Self { name, offset })
    }
}

// ===========================================================================
// MetaIndexNode
// ===========================================================================

/// B-tree-like index node in the metadata section.
///
/// The metadata index tree has two levels:
/// - Device level: routes lookups to the correct device (InternalDevice / LeafDevice)
/// - Measurement level: routes lookups to timeseries within a device
///   (InternalMeasurement / LeafMeasurement)
///
/// C++ MetaIndexNode holds a vector of MetaIndexEntry children, a node
/// type enum, and an end_offset marking the byte position where this
/// node's subtree ends in the file. This enables binary search and
/// efficient skipping during reads.
///
/// Serialization format:
///   num_children (i32) + [MetaIndexEntry...] + node_type (u8) + end_offset (i64)
#[derive(Debug, Clone, PartialEq)]
pub struct MetaIndexNode {
    pub children: Vec<MetaIndexEntry>,
    pub node_type: MetaIndexNodeType,
    /// File position where this node's subtree data ends.
    pub end_offset: i64,
}

impl MetaIndexNode {
    pub fn new(node_type: MetaIndexNodeType) -> Self {
        Self {
            children: Vec::new(),
            node_type,
            end_offset: 0,
        }
    }

    pub fn add_child(&mut self, entry: MetaIndexEntry) {
        self.children.push(entry);
    }

    pub fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        serialize::write_i32(w, self.children.len() as i32)?;
        for child in &self.children {
            child.serialize_to(w)?;
        }
        serialize::write_u8(w, self.node_type as u8)?;
        serialize::write_i64(w, self.end_offset)?;
        Ok(())
    }

    pub fn deserialize_from(r: &mut impl Read) -> Result<Self> {
        let count = serialize::read_i32(r)? as usize;
        let mut children = Vec::with_capacity(count);
        for _ in 0..count {
            children.push(MetaIndexEntry::deserialize_from(r)?);
        }
        let node_type = MetaIndexNodeType::try_from(serialize::read_u8(r)?)?;
        let end_offset = serialize::read_i64(r)?;
        Ok(Self {
            children,
            node_type,
            end_offset,
        })
    }
}

// ===========================================================================
// TsFileMeta
// ===========================================================================

/// File-level metadata written at the end of a TsFile (before trailing magic).
///
/// The reader locates this by reading the meta_offset from the fixed
/// position at (file_size - 6 bytes magic - 8 bytes i64 offset). The
/// meta_offset points to the start of the metadata section (index tree),
/// and TsFileMeta itself is written after the index tree.
///
/// C++ TsFileMeta contains table schemas, optional bloom filter data,
/// and the meta_offset. The index tree root (MetaIndexNode) is serialized
/// immediately before TsFileMeta in the file — it is NOT inside TsFileMeta
/// but is reached by seeking to meta_offset.
///
/// Serialization format:
///   num_table_schemas (i32) + [TableSchema...] +
///   bloom_filter_size (i32) + [bloom_filter_data] +
///   meta_offset (i64)
#[derive(Debug, Clone, PartialEq)]
pub struct TsFileMeta {
    /// Byte offset in the file where the metadata section begins.
    pub meta_offset: i64,
    /// Table schemas (table model). Empty for pure tree-model files.
    pub table_schema_map: Vec<TableSchema>,
    /// Optional bloom filter data for fast device/path existence checks.
    pub bloom_filter: Option<Vec<u8>>,
}

impl TsFileMeta {
    pub fn new() -> Self {
        Self {
            meta_offset: 0,
            table_schema_map: Vec::new(),
            bloom_filter: None,
        }
    }

    pub fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        // Table schemas
        serialize::write_i32(w, self.table_schema_map.len() as i32)?;
        for ts in &self.table_schema_map {
            ts.serialize_to(w)?;
        }

        // Bloom filter
        match &self.bloom_filter {
            Some(data) => {
                serialize::write_i32(w, data.len() as i32)?;
                w.write_all(data)?;
            }
            None => {
                serialize::write_i32(w, 0)?;
            }
        }

        // Meta offset (last field before trailing magic)
        serialize::write_i64(w, self.meta_offset)?;
        Ok(())
    }

    pub fn deserialize_from(r: &mut impl Read) -> Result<Self> {
        // Table schemas
        let num_schemas = serialize::read_i32(r)? as usize;
        let mut table_schema_map = Vec::with_capacity(num_schemas);
        for _ in 0..num_schemas {
            table_schema_map.push(TableSchema::deserialize_from(r)?);
        }

        // Bloom filter
        let bloom_size = serialize::read_i32(r)? as usize;
        let bloom_filter = if bloom_size > 0 {
            let mut buf = vec![0u8; bloom_size];
            r.read_exact(&mut buf)?;
            Some(buf)
        } else {
            None
        };

        // Meta offset
        let meta_offset = serialize::read_i64(r)?;

        Ok(Self {
            meta_offset,
            table_schema_map,
            bloom_filter,
        })
    }
}

impl Default for TsFileMeta {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnCategory, ColumnSchema};
    use std::io::Cursor;

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------

    #[test]
    fn magic_bytes() {
        assert_eq!(TSFILE_MAGIC, b"TsFile");
        assert_eq!(TSFILE_MAGIC.len(), 6);
    }

    #[test]
    fn version_number() {
        assert_eq!(VERSION_NUMBER, 4);
    }

    #[test]
    fn marker_constants() {
        // Verify marker values match the Java/C++ MetaMarkerType constants.
        assert_eq!(CHUNK_HEADER_MARKER, 1);
        assert_eq!(ONLY_ONE_PAGE_CHUNK_HEADER_MARKER, 5);
        assert_eq!(VALUE_CHUNK_HEADER_MARKER, 6);
        assert_eq!(ONLY_ONE_PAGE_VALUE_CHUNK_HEADER_MARKER, 7);
        assert_eq!(TIME_CHUNK_HEADER_MARKER, 8);
        assert_eq!(ONLY_ONE_PAGE_TIME_CHUNK_HEADER_MARKER, 9);
        assert_eq!(CHUNK_GROUP_FOOTER_MARKER, 0);
        assert_eq!(SEPARATOR_MARKER, 2);
        assert_eq!(OPERATION_INDEX_RANGE_MARKER, 4);
    }

    // -----------------------------------------------------------------------
    // MetaIndexNodeType
    // -----------------------------------------------------------------------

    #[test]
    fn meta_index_node_type_discriminants() {
        assert_eq!(MetaIndexNodeType::InternalDevice as u8, 0);
        assert_eq!(MetaIndexNodeType::LeafDevice as u8, 1);
        assert_eq!(MetaIndexNodeType::InternalMeasurement as u8, 2);
        assert_eq!(MetaIndexNodeType::LeafMeasurement as u8, 3);
    }

    #[test]
    fn meta_index_node_type_try_from_valid() {
        for v in 0..=3u8 {
            assert!(MetaIndexNodeType::try_from(v).is_ok());
        }
    }

    #[test]
    fn meta_index_node_type_try_from_invalid() {
        assert!(MetaIndexNodeType::try_from(4).is_err());
        assert!(MetaIndexNodeType::try_from(255).is_err());
    }

    // -----------------------------------------------------------------------
    // ChunkHeader
    // -----------------------------------------------------------------------

    fn make_regular_chunk_header() -> ChunkHeader {
        ChunkHeader::new(
            CHUNK_HEADER_MARKER,
            "temperature".to_string(),
            1024,
            TSDataType::Float,
            TSEncoding::Gorilla,
            CompressionType::Snappy,
        )
    }

    #[test]
    fn chunk_header_type_checks() {
        let regular = make_regular_chunk_header();
        assert!(regular.is_regular_chunk());
        assert!(!regular.is_time_chunk());
        assert!(!regular.is_value_chunk());
        assert!(!regular.is_single_page());

        let single = ChunkHeader::new(
            ONLY_ONE_PAGE_CHUNK_HEADER_MARKER,
            "s".into(),
            100,
            TSDataType::Int32,
            TSEncoding::Plain,
            CompressionType::Uncompressed,
        );
        assert!(single.is_single_page());
        assert!(single.is_regular_chunk());

        let time = ChunkHeader::new(
            TIME_CHUNK_HEADER_MARKER,
            "".into(),
            500,
            TSDataType::Int64,
            TSEncoding::Ts2Diff,
            CompressionType::Lz4,
        );
        assert!(time.is_time_chunk());
        assert!(!time.is_value_chunk());
        assert!(!time.is_regular_chunk());

        let value = ChunkHeader::new(
            VALUE_CHUNK_HEADER_MARKER,
            "v".into(),
            200,
            TSDataType::Double,
            TSEncoding::Gorilla,
            CompressionType::Gzip,
        );
        assert!(value.is_value_chunk());
        assert!(!value.is_time_chunk());
    }

    #[test]
    fn chunk_header_serialize_round_trip() {
        let headers = [
            make_regular_chunk_header(),
            ChunkHeader::new(
                ONLY_ONE_PAGE_CHUNK_HEADER_MARKER,
                "humidity".into(),
                512,
                TSDataType::Double,
                TSEncoding::Sprintz,
                CompressionType::Lz4,
            ),
            ChunkHeader::new(
                TIME_CHUNK_HEADER_MARKER,
                "".into(),
                256,
                TSDataType::Int64,
                TSEncoding::Ts2Diff,
                CompressionType::Lz4,
            ),
            ChunkHeader::new(
                VALUE_CHUNK_HEADER_MARKER,
                "status".into(),
                64,
                TSDataType::Boolean,
                TSEncoding::Plain,
                CompressionType::Uncompressed,
            ),
        ];

        for original in &headers {
            let mut buf = Vec::new();
            original.serialize_to(&mut buf).unwrap();

            let mut cursor = Cursor::new(&buf);
            let decoded = ChunkHeader::deserialize_from(&mut cursor).unwrap();
            assert_eq!(original, &decoded);
        }
    }

    #[test]
    fn chunk_header_deserialize_with_marker() {
        let original = make_regular_chunk_header();
        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        // Simulate: reader already consumed the marker byte
        let marker = buf[0];
        let mut cursor = Cursor::new(&buf[1..]);
        let decoded = ChunkHeader::deserialize_with_marker(&mut cursor, marker).unwrap();
        assert_eq!(original, decoded);
    }

    // -----------------------------------------------------------------------
    // PageHeader
    // -----------------------------------------------------------------------

    #[test]
    fn page_header_without_statistic_round_trip() {
        let original = PageHeader::new(1024, 768, None);

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();
        // 4 bytes uncompressed + 4 bytes compressed = 8 bytes
        assert_eq!(buf.len(), 8);

        let mut cursor = Cursor::new(&buf);
        let decoded = PageHeader::deserialize_from(&mut cursor, TSDataType::Int32, false).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn page_header_with_statistic_round_trip() {
        let mut stat = Statistic::new(TSDataType::Int32);
        stat.update_i32(100, 42);
        stat.update_i32(200, -10);

        let original = PageHeader::new(2048, 1500, Some(stat));

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = PageHeader::deserialize_from(&mut cursor, TSDataType::Int32, true).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn page_header_all_data_types_with_stats() {
        let test_cases: Vec<(TSDataType, Box<dyn Fn(&mut Statistic)>)> = vec![
            (
                TSDataType::Boolean,
                Box::new(|s| {
                    s.update_bool(1, true);
                    s.update_bool(2, false);
                }),
            ),
            (
                TSDataType::Int32,
                Box::new(|s| {
                    s.update_i32(1, 42);
                }),
            ),
            (
                TSDataType::Int64,
                Box::new(|s| {
                    s.update_i64(1, 999);
                }),
            ),
            (
                TSDataType::Float,
                Box::new(|s| {
                    s.update_f32(1, 3.14);
                }),
            ),
            (
                TSDataType::Double,
                Box::new(|s| {
                    s.update_f64(1, 2.718);
                }),
            ),
            (
                TSDataType::Text,
                Box::new(|s| {
                    s.update_text(1, b"hello");
                }),
            ),
        ];

        for (dt, updater) in &test_cases {
            let mut stat = Statistic::new(*dt);
            updater(&mut stat);
            let original = PageHeader::new(100, 80, Some(stat));

            let mut buf = Vec::new();
            original.serialize_to(&mut buf).unwrap();

            let mut cursor = Cursor::new(&buf);
            let decoded = PageHeader::deserialize_from(&mut cursor, *dt, true).unwrap();
            assert_eq!(original, decoded, "failed for data type {:?}", dt);
        }
    }

    // -----------------------------------------------------------------------
    // ChunkMeta
    // -----------------------------------------------------------------------

    fn make_chunk_meta(mask: u8) -> ChunkMeta {
        let mut stat = Statistic::new(TSDataType::Float);
        stat.update_f32(100, 25.5);
        stat.update_f32(200, 30.0);

        ChunkMeta::new(
            "temperature".to_string(),
            TSDataType::Float,
            4096,
            stat,
            mask,
        )
    }

    #[test]
    fn chunk_meta_alignment_checks() {
        let regular = make_chunk_meta(CHUNK_TYPE_NON_ALIGNED_MASK);
        assert!(!regular.is_aligned());
        assert!(!regular.is_time_chunk());
        assert!(!regular.is_value_chunk());

        let time = make_chunk_meta(CHUNK_TYPE_ALIGNED_TIME_MASK);
        assert!(time.is_aligned());
        assert!(time.is_time_chunk());
        assert!(!time.is_value_chunk());

        let value = make_chunk_meta(CHUNK_TYPE_ALIGNED_VALUE_MASK);
        assert!(value.is_aligned());
        assert!(!value.is_time_chunk());
        assert!(value.is_value_chunk());
    }

    #[test]
    fn chunk_meta_serialize_round_trip() {
        for mask in [
            CHUNK_TYPE_NON_ALIGNED_MASK,
            CHUNK_TYPE_ALIGNED_TIME_MASK,
            CHUNK_TYPE_ALIGNED_VALUE_MASK,
        ] {
            let original = make_chunk_meta(mask);

            let mut buf = Vec::new();
            original.serialize_to(&mut buf).unwrap();

            let mut cursor = Cursor::new(&buf);
            let decoded = ChunkMeta::deserialize_from(&mut cursor).unwrap();
            assert_eq!(original, decoded, "failed for mask={mask}");
        }
    }

    #[test]
    fn chunk_meta_all_data_types() {
        let types_and_updaters: Vec<(TSDataType, Box<dyn Fn(&mut Statistic)>)> = vec![
            (TSDataType::Boolean, Box::new(|s| s.update_bool(1, true))),
            (TSDataType::Int32, Box::new(|s| s.update_i32(1, 42))),
            (TSDataType::Int64, Box::new(|s| s.update_i64(1, 999))),
            (TSDataType::Float, Box::new(|s| s.update_f32(1, 3.14))),
            (TSDataType::Double, Box::new(|s| s.update_f64(1, 2.718))),
            (TSDataType::Text, Box::new(|s| s.update_text(1, b"val"))),
        ];

        for (dt, updater) in &types_and_updaters {
            let mut stat = Statistic::new(*dt);
            updater(&mut stat);

            let original = ChunkMeta::new(
                format!("col_{:?}", dt),
                *dt,
                12345,
                stat,
                CHUNK_TYPE_NON_ALIGNED_MASK,
            );

            let mut buf = Vec::new();
            original.serialize_to(&mut buf).unwrap();

            let mut cursor = Cursor::new(&buf);
            let decoded = ChunkMeta::deserialize_from(&mut cursor).unwrap();
            assert_eq!(original, decoded, "failed for {:?}", dt);
        }
    }

    // -----------------------------------------------------------------------
    // ChunkGroupMeta
    // -----------------------------------------------------------------------

    #[test]
    fn chunk_group_meta_serialize_round_trip() {
        let mut original = ChunkGroupMeta::new(DeviceId::parse("root.sg1.d1").unwrap());

        let mut stat1 = Statistic::new(TSDataType::Float);
        stat1.update_f32(100, 25.5);
        original.add_chunk_meta(ChunkMeta::new(
            "temperature".into(),
            TSDataType::Float,
            100,
            stat1,
            CHUNK_TYPE_NON_ALIGNED_MASK,
        ));

        let mut stat2 = Statistic::new(TSDataType::Int32);
        stat2.update_i32(100, 80);
        original.add_chunk_meta(ChunkMeta::new(
            "humidity".into(),
            TSDataType::Int32,
            500,
            stat2,
            CHUNK_TYPE_NON_ALIGNED_MASK,
        ));

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = ChunkGroupMeta::deserialize_from(&mut cursor).unwrap();

        assert_eq!(original.device_id, decoded.device_id);
        assert_eq!(
            original.chunk_meta_list.len(),
            decoded.chunk_meta_list.len()
        );
        assert_eq!(original.chunk_meta_list, decoded.chunk_meta_list);
    }

    #[test]
    fn chunk_group_meta_empty() {
        let original = ChunkGroupMeta::new(DeviceId::parse("root.d1").unwrap());

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = ChunkGroupMeta::deserialize_from(&mut cursor).unwrap();
        assert_eq!(original.device_id, decoded.device_id);
        assert!(decoded.chunk_meta_list.is_empty());
    }

    // -----------------------------------------------------------------------
    // TimeseriesIndex
    // -----------------------------------------------------------------------

    #[test]
    fn timeseries_index_serialize_round_trip() {
        let mut original = TimeseriesIndex::new("temperature".to_string(), TSDataType::Float);

        // Populate aggregate statistic
        original.statistic.update_f32(100, 25.5);
        original.statistic.update_f32(200, 30.0);

        // Add two chunk metas
        let mut stat1 = Statistic::new(TSDataType::Float);
        stat1.update_f32(100, 25.5);
        original.chunk_meta_list.push(ChunkMeta::new(
            "temperature".into(),
            TSDataType::Float,
            1000,
            stat1,
            CHUNK_TYPE_NON_ALIGNED_MASK,
        ));

        let mut stat2 = Statistic::new(TSDataType::Float);
        stat2.update_f32(200, 30.0);
        original.chunk_meta_list.push(ChunkMeta::new(
            "temperature".into(),
            TSDataType::Float,
            2000,
            stat2,
            CHUNK_TYPE_NON_ALIGNED_MASK,
        ));

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = TimeseriesIndex::deserialize_from(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn timeseries_index_empty_chunks() {
        let original = TimeseriesIndex::new("col".to_string(), TSDataType::Int64);

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = TimeseriesIndex::deserialize_from(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    // -----------------------------------------------------------------------
    // MetaIndexEntry
    // -----------------------------------------------------------------------

    #[test]
    fn meta_index_entry_serialize_round_trip() {
        let entries = [
            MetaIndexEntry::new("root.sg1.d1".to_string(), 4096),
            MetaIndexEntry::new("temperature".to_string(), 0),
            MetaIndexEntry::new("".to_string(), i64::MAX),
        ];

        for original in &entries {
            let mut buf = Vec::new();
            original.serialize_to(&mut buf).unwrap();

            let mut cursor = Cursor::new(&buf);
            let decoded = MetaIndexEntry::deserialize_from(&mut cursor).unwrap();
            assert_eq!(original, &decoded);
        }
    }

    // -----------------------------------------------------------------------
    // MetaIndexNode
    // -----------------------------------------------------------------------

    #[test]
    fn meta_index_node_serialize_round_trip_all_types() {
        let node_types = [
            MetaIndexNodeType::InternalDevice,
            MetaIndexNodeType::LeafDevice,
            MetaIndexNodeType::InternalMeasurement,
            MetaIndexNodeType::LeafMeasurement,
        ];

        for nt in node_types {
            let mut original = MetaIndexNode::new(nt);
            original.end_offset = 8192;
            original.add_child(MetaIndexEntry::new("entry1".into(), 1000));
            original.add_child(MetaIndexEntry::new("entry2".into(), 2000));

            let mut buf = Vec::new();
            original.serialize_to(&mut buf).unwrap();

            let mut cursor = Cursor::new(&buf);
            let decoded = MetaIndexNode::deserialize_from(&mut cursor).unwrap();
            assert_eq!(original, decoded, "failed for {:?}", nt);
        }
    }

    #[test]
    fn meta_index_node_empty_children() {
        let original = MetaIndexNode {
            children: vec![],
            node_type: MetaIndexNodeType::LeafDevice,
            end_offset: 0,
        };

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = MetaIndexNode::deserialize_from(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn meta_index_node_many_children() {
        let mut original = MetaIndexNode::new(MetaIndexNodeType::InternalDevice);
        original.end_offset = 99999;
        for i in 0..256 {
            original.add_child(MetaIndexEntry::new(format!("device_{i}"), i * 100));
        }

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = MetaIndexNode::deserialize_from(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    // -----------------------------------------------------------------------
    // TsFileMeta
    // -----------------------------------------------------------------------

    #[test]
    fn tsfile_meta_empty_round_trip() {
        let original = TsFileMeta::new();

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = TsFileMeta::deserialize_from(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn tsfile_meta_with_schemas_round_trip() {
        let mut original = TsFileMeta::new();
        original.meta_offset = 65536;
        original.table_schema_map.push(TableSchema::new(
            "weather".to_string(),
            vec![
                ColumnSchema::new("region".into(), TSDataType::String, ColumnCategory::Tag),
                ColumnSchema::new("temp".into(), TSDataType::Float, ColumnCategory::Field),
            ],
        ));
        original.table_schema_map.push(TableSchema::new(
            "sensors".to_string(),
            vec![
                ColumnSchema::new("id".into(), TSDataType::Int32, ColumnCategory::Tag),
                ColumnSchema::new("value".into(), TSDataType::Double, ColumnCategory::Field),
            ],
        ));

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = TsFileMeta::deserialize_from(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn tsfile_meta_with_bloom_filter_round_trip() {
        let mut original = TsFileMeta::new();
        original.meta_offset = 12345;
        original.bloom_filter = Some(vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF]);

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = TsFileMeta::deserialize_from(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn tsfile_meta_full_round_trip() {
        let mut original = TsFileMeta::new();
        original.meta_offset = 999999;
        original.table_schema_map.push(TableSchema::new(
            "t".to_string(),
            vec![ColumnSchema::new(
                "c".into(),
                TSDataType::Int32,
                ColumnCategory::Field,
            )],
        ));
        original.bloom_filter = Some(vec![1, 2, 3, 4, 5]);

        let mut buf = Vec::new();
        original.serialize_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = TsFileMeta::deserialize_from(&mut cursor).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn tsfile_meta_default() {
        let meta = TsFileMeta::default();
        assert_eq!(meta.meta_offset, 0);
        assert!(meta.table_schema_map.is_empty());
        assert!(meta.bloom_filter.is_none());
    }
}
