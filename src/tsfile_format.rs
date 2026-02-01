// On-disk format structures (tsfile_common.h): ChunkHeader, PageHeader,
// ChunkMeta, ChunkGroupMeta, MetaIndexNode, TsFileMeta. These must
// serialize to the exact same byte layout as C++ for cross-reader
// compatibility. Each struct implements serialize/deserialize using
// the varint encoding from serialize.rs.
