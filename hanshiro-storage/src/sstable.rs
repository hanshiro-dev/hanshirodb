//! # SSTable - Sorted String Table
//!
//! SSTables are immutable, sorted files that store data on disk.
//! They are the primary storage format for HanshiroDB.
//!
//! ## SSTable File Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    SSTable File Structure                    │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                              │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │                    Data Blocks                       │    │
//! │  │  ┌───────────────────────────────────────────────┐  │    │
//! │  │  │ Block 1 (Default: 4KB)                        │  │    │
//! │  │  │ ┌─────────────────────────────────────────┐  │  │    │
//! │  │  │ │ Entry 1: [key_len][key][value_len][val] │  │  │    │
//! │  │  │ │ Entry 2: [key_len][key][value_len][val] │  │  │    │
//! │  │  │ │ ...                                     │  │  │    │
//! │  │  │ │ Entry N: [key_len][key][value_len][val] │  │  │    │
//! │  │  │ └─────────────────────────────────────────┘  │  │    │
//! │  │  │ Block Footer: [compression][crc32]           │  │    │
//! │  │  └───────────────────────────────────────────────┘  │    │
//! │  │  Block 2...                                         │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! │                                                              │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │                    Index Block                       │    │
//! │  │  ┌───────────────────────────────────────────────┐  │    │
//! │  │  │ Index Entry 1: [last_key][offset][size]      │  │    │
//! │  │  │ Index Entry 2: [last_key][offset][size]      │  │    │
//! │  │  │ ...                                          │  │    │
//! │  │  └───────────────────────────────────────────────┘  │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! │                                                              │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │                   Bloom Filter                       │    │
//! │  │  [filter_data][num_probes][bits_per_key]            │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! │                                                              │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │                      Footer                          │    │
//! │  │  [index_offset][index_size][bloom_offset][bloom_size]│    │
//! │  │  [magic_number][version][checksum]                  │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// TODO: Add proper bloom filter support
// Placeholder bloom filter for now
struct BloomFilter;

impl BloomFilter {
    fn with_rate(_false_positive_rate: f64, _expected_items: usize) -> Self {
        Self
    }
    
    fn insert(&mut self, _key: &[u8]) {}
    
    fn contains(&self, _key: &[u8]) -> bool {
        true // Always check the actual data for now
    }
}
use bytes::{Buf, BufMut, Bytes, BytesMut};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use memmap2::{Mmap, MmapOptions};
use tracing::{debug, info, warn};

use hanshiro_core::{
    error::{Error, Result},
    Event, EventId,
};

const SSTABLE_MAGIC: &[u8; 8] = b"HANSHIRO";
const SSTABLE_VERSION: u32 = 1;
const DEFAULT_BLOCK_SIZE: usize = 4 * 1024; // 4KB
const FOOTER_SIZE: usize = 48;

/// Compression type for blocks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
    Snappy = 3,
}

impl TryFrom<u8> for CompressionType {
    type Error = Error;
    
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Lz4),
            2 => Ok(CompressionType::Zstd),
            3 => Ok(CompressionType::Snappy),
            _ => Err(Error::SSTable {
                message: format!("Invalid compression type: {}", value),
                source: None,
            }),
        }
    }
}

/// SSTable configuration
#[derive(Debug, Clone)]
pub struct SSTableConfig {
    pub block_size: usize,
    pub compression: CompressionType,
    pub bloom_bits_per_key: usize,
    pub index_interval: usize,
}

impl Default for SSTableConfig {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            compression: CompressionType::Zstd,
            bloom_bits_per_key: 10,
            index_interval: 128, // Index every 128th key
        }
    }
}

/// SSTable metadata
#[derive(Debug, Clone)]
pub struct SSTableInfo {
    pub path: PathBuf,
    pub file_size: u64,
    pub entry_count: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub creation_time: u64,
    pub level: u32,
}

/// SSTable writer
pub struct SSTableWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    config: SSTableConfig,
    current_block: BlockBuilder,
    index_builder: IndexBuilder,
    bloom_filter: BloomFilter,
    entry_count: u64,
    file_offset: u64,
    min_key: Option<Bytes>,
    max_key: Option<Bytes>,
}

impl SSTableWriter {
    /// Create new SSTable writer
    pub fn new(path: impl AsRef<Path>, config: SSTableConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
            
        let writer = BufWriter::new(file);
        let bloom_filter = BloomFilter::with_rate(0.01, 10000); // 1% false positive rate
        
        Ok(Self {
            path,
            writer,
            config: config.clone(),
            current_block: BlockBuilder::new(config.block_size),
            index_builder: IndexBuilder::new(),
            bloom_filter,
            entry_count: 0,
            file_offset: 0,
            min_key: None,
            max_key: None,
        })
    }
    
    /// Add key-value pair
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Update min/max keys
        if self.min_key.is_none() {
            self.min_key = Some(Bytes::copy_from_slice(key));
        }
        self.max_key = Some(Bytes::copy_from_slice(key));
        
        // Add to bloom filter
        self.bloom_filter.insert(key);
        
        // Add to current block
        if !self.current_block.add(key, value) {
            // Block is full, flush it
            self.flush_block()?;
            
            // Try again with new block
            if !self.current_block.add(key, value) {
                return Err(Error::SSTable {
                    message: "Entry too large for block".to_string(),
                    source: None,
                });
            }
        }
        
        self.entry_count += 1;
        
        // Add to index if needed
        if self.entry_count % self.config.index_interval as u64 == 0 {
            self.index_builder.set_pending_key(key);
        }
        
        Ok(())
    }
    
    /// Flush current block to disk
    fn flush_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }
        
        let block_data = self.current_block.finish();
        let compressed = self.compress_block(&block_data)?;
        
        // Write block
        let block_offset = self.file_offset;
        let block_size = compressed.len() + 5; // +5 for footer
        
        self.writer.write_all(&compressed)?;
        
        // Write block footer
        self.writer.write_u8(self.config.compression as u8)?;
        self.writer.write_u32::<LittleEndian>(crc32fast::hash(&compressed))?;
        
        self.file_offset += block_size as u64;
        
        // Add to index if we have a pending key
        if let Some(key) = self.current_block.last_key() {
            if self.index_builder.has_pending_key() {
                self.index_builder.add(&key, block_offset, block_size as u32)?;
            }
        }
        
        // Reset block
        self.current_block = BlockBuilder::new(self.config.block_size);
        
        Ok(())
    }
    
    /// Compress block data
    fn compress_block(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.config.compression {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Lz4 => {
                let compressed = lz4::block::compress(data, None, false)
                    .map_err(|e| Error::SSTable {
                        message: format!("LZ4 compression failed: {}", e),
                        source: None,
                    })?;
                Ok(compressed)
            }
            CompressionType::Zstd => {
                let compressed = zstd::encode_all(data, 3)
                    .map_err(|e| Error::SSTable {
                        message: format!("Zstd compression failed: {}", e),
                        source: None,
                    })?;
                Ok(compressed)
            }
            CompressionType::Snappy => {
                let compressed = snap::raw::Encoder::new().compress_vec(data)
                    .map_err(|e| Error::SSTable {
                        message: format!("Snappy compression failed: {}", e),
                        source: None,
                    })?;
                Ok(compressed)
            }
        }
    }
    
    /// Finish writing SSTable
    pub fn finish(mut self) -> Result<SSTableInfo> {
        // Flush any remaining data
        self.flush_block()?;
        
        // Write index block
        let index_offset = self.file_offset;
        let index_data = self.index_builder.finish();
        self.writer.write_all(&index_data)?;
        let index_size = index_data.len() as u32;
        self.file_offset += index_size as u64;
        
        // Write bloom filter
        let bloom_offset = self.file_offset;
        let bloom_data = self.serialize_bloom_filter()?;
        self.writer.write_all(&bloom_data)?;
        let bloom_size = bloom_data.len() as u32;
        self.file_offset += bloom_size as u64;
        
        // Write footer
        self.writer.write_u64::<LittleEndian>(index_offset)?;
        self.writer.write_u32::<LittleEndian>(index_size)?;
        self.writer.write_u64::<LittleEndian>(bloom_offset)?;
        self.writer.write_u32::<LittleEndian>(bloom_size)?;
        self.writer.write_all(SSTABLE_MAGIC)?;
        self.writer.write_u32::<LittleEndian>(SSTABLE_VERSION)?;
        
        // Calculate and write file checksum
        self.writer.flush()?;
        let file_size = self.file_offset + FOOTER_SIZE as u64;
        let checksum = 0u32; // TODO: Calculate actual checksum
        self.writer.write_u32::<LittleEndian>(checksum)?;
        
        self.writer.flush()?;
        
        info!(
            "Finished writing SSTable: {} entries, {} bytes",
            self.entry_count, file_size
        );
        
        Ok(SSTableInfo {
            path: self.path,
            file_size,
            entry_count: self.entry_count,
            min_key: self.min_key.unwrap_or_default(),
            max_key: self.max_key.unwrap_or_default(),
            creation_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            level: 0,
        })
    }
    
    /// Serialize bloom filter
    fn serialize_bloom_filter(&self) -> Result<Vec<u8>> {
        // For now, just write a placeholder
        // TODO: Implement proper bloom filter serialization
        Ok(vec![0u8; 1024])
    }
}

/// SSTable reader
pub struct SSTableReader {
    path: PathBuf,
    mmap: Mmap,
    info: SSTableInfo,
    index: SSTableIndex,
    bloom_filter: Option<BloomFilter>,
    config: SSTableConfig,
}

impl SSTableReader {
    /// Open SSTable for reading
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let file_size = file.metadata()?.len();
        
        // Memory-map the file
        let mmap = unsafe {
            MmapOptions::new()
                .map(&file)
                .map_err(|e| Error::Io {
                    message: "Failed to mmap SSTable".to_string(),
                    source: e,
                })?
        };
        
        // Read footer
        if file_size < FOOTER_SIZE as u64 {
            return Err(Error::SSTable {
                message: "SSTable file too small".to_string(),
                source: None,
            });
        }
        
        let footer_offset = file_size - FOOTER_SIZE as u64;
        let mut cursor = io::Cursor::new(&mmap[footer_offset as usize..]);
        
        let index_offset = cursor.read_u64::<LittleEndian>()?;
        let index_size = cursor.read_u32::<LittleEndian>()?;
        let bloom_offset = cursor.read_u64::<LittleEndian>()?;
        let bloom_size = cursor.read_u32::<LittleEndian>()?;
        
        let mut magic = [0u8; 8];
        cursor.read_exact(&mut magic)?;
        if &magic != SSTABLE_MAGIC {
            return Err(Error::SSTable {
                message: "Invalid SSTable magic number".to_string(),
                source: None,
            });
        }
        
        let version = cursor.read_u32::<LittleEndian>()?;
        if version != SSTABLE_VERSION {
            return Err(Error::SSTable {
                message: format!("Unsupported SSTable version: {}", version),
                source: None,
            });
        }
        
        let _checksum = cursor.read_u32::<LittleEndian>()?;
        
        // Load index
        let index_data = &mmap[index_offset as usize..(index_offset + index_size as u64) as usize];
        let index = SSTableIndex::load(index_data)?;
        
        // Load bloom filter
        let bloom_filter = if bloom_size > 0 {
            // TODO: Deserialize bloom filter
            None
        } else {
            None
        };
        
        // Create info
        let info = SSTableInfo {
            path: path.clone(),
            file_size,
            entry_count: 0, // TODO: Store in footer
            min_key: Bytes::new(), // TODO: Store in footer
            max_key: Bytes::new(), // TODO: Store in footer
            creation_time: 0, // TODO: Store in footer
            level: 0,
        };
        
        Ok(Self {
            path,
            mmap,
            info,
            index,
            bloom_filter,
            config: SSTableConfig::default(), // TODO: Store config in file
        })
    }
    
    /// Get value by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // Check bloom filter first
        if let Some(ref bloom) = self.bloom_filter {
            if !bloom.contains(key) {
                return Ok(None);
            }
        }
        
        // Find block that might contain the key
        let block_info = match self.index.find_block(key) {
            Some(info) => info,
            None => return Ok(None),
        };
        
        // Read and decompress block
        let block_data = self.read_block(block_info.offset, block_info.size)?;
        
        // Search within block
        self.search_block(&block_data, key)
    }
    
    /// Read and decompress a block
    fn read_block(&self, offset: u64, size: u32) -> Result<Vec<u8>> {
        let block_end = offset + size as u64 - 5; // -5 for footer
        let block_data = &self.mmap[offset as usize..block_end as usize];
        
        // Read footer
        let compression = CompressionType::try_from(self.mmap[block_end as usize])?;
        let crc = (&self.mmap[(block_end + 1) as usize..(block_end + 5) as usize])
            .read_u32::<LittleEndian>()?;
        
        // Verify CRC
        if crc32fast::hash(block_data) != crc {
            return Err(Error::SSTable {
                message: "Block CRC mismatch".to_string(),
                source: None,
            });
        }
        
        // Decompress if needed
        match compression {
            CompressionType::None => Ok(block_data.to_vec()),
            CompressionType::Lz4 => {
                lz4::block::decompress(block_data, None)
                    .map_err(|e| Error::SSTable {
                        message: format!("LZ4 decompression failed: {}", e),
                        source: None,
                    })
            }
            CompressionType::Zstd => {
                zstd::decode_all(block_data)
                    .map_err(|e| Error::SSTable {
                        message: format!("Zstd decompression failed: {}", e),
                        source: None,
                    })
            }
            CompressionType::Snappy => {
                snap::raw::Decoder::new().decompress_vec(block_data)
                    .map_err(|e| Error::SSTable {
                        message: format!("Snappy decompression failed: {}", e),
                        source: None,
                    })
            }
        }
    }
    
    /// Search for key within a block
    fn search_block(&self, block_data: &[u8], target_key: &[u8]) -> Result<Option<Bytes>> {
        let mut cursor = io::Cursor::new(block_data);
        
        while cursor.position() < block_data.len() as u64 {
            // Read key length
            let key_len = match cursor.read_u32::<LittleEndian>() {
                Ok(len) => len as usize,
                Err(_) => break, // End of block
            };
            
            // Read key
            let mut key = vec![0u8; key_len];
            cursor.read_exact(&mut key)?;
            
            // Read value length
            let value_len = cursor.read_u32::<LittleEndian>()? as usize;
            
            // Read or skip value
            if key.as_slice() == target_key {
                let mut value = vec![0u8; value_len];
                cursor.read_exact(&mut value)?;
                return Ok(Some(Bytes::from(value)));
            } else if key.as_slice() > target_key {
                // Keys are sorted, so we've passed it
                return Ok(None);
            } else {
                // Skip value
                cursor.seek(SeekFrom::Current(value_len as i64))?;
            }
        }
        
        Ok(None)
    }
    
    /// Create iterator over all entries
    pub fn iter(&self) -> SSTableIterator {
        SSTableIterator::new(self)
    }
}

/// Block builder
struct BlockBuilder {
    buffer: BytesMut,
    last_key: Option<Vec<u8>>,
    max_size: usize,
}

impl BlockBuilder {
    fn new(max_size: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(max_size),
            last_key: None,
            max_size,
        }
    }
    
    fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let entry_size = 4 + key.len() + 4 + value.len();
        
        if self.buffer.len() + entry_size > self.max_size && !self.buffer.is_empty() {
            return false; // Block is full
        }
        
        // Write key length and key
        self.buffer.put_u32_le(key.len() as u32);
        self.buffer.put_slice(key);
        
        // Write value length and value
        self.buffer.put_u32_le(value.len() as u32);
        self.buffer.put_slice(value);
        
        self.last_key = Some(key.to_vec());
        true
    }
    
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
    
    fn last_key(&self) -> Option<Vec<u8>> {
        self.last_key.clone()
    }
    
    fn finish(&self) -> Vec<u8> {
        self.buffer.to_vec()
    }
}

/// Index builder
struct IndexBuilder {
    entries: Vec<IndexEntry>,
    pending_key: Option<Vec<u8>>,
}

impl IndexBuilder {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            pending_key: None,
        }
    }
    
    fn set_pending_key(&mut self, key: &[u8]) {
        self.pending_key = Some(key.to_vec());
    }
    
    fn has_pending_key(&self) -> bool {
        self.pending_key.is_some()
    }
    
    fn add(&mut self, key: &[u8], offset: u64, size: u32) -> Result<()> {
        if self.pending_key.is_some() {
            self.entries.push(IndexEntry {
                key: key.to_vec(),
                offset,
                size,
            });
            self.pending_key = None;
        }
        Ok(())
    }
    
    fn finish(&self) -> Vec<u8> {
        let mut buffer = BytesMut::new();
        
        // Write number of entries
        buffer.put_u32_le(self.entries.len() as u32);
        
        // Write each entry
        for entry in &self.entries {
            buffer.put_u32_le(entry.key.len() as u32);
            buffer.put_slice(&entry.key);
            buffer.put_u64_le(entry.offset);
            buffer.put_u32_le(entry.size);
        }
        
        buffer.to_vec()
    }
}

/// Index entry
#[derive(Debug, Clone)]
struct IndexEntry {
    key: Vec<u8>,
    offset: u64,
    size: u32,
}

/// SSTable index
struct SSTableIndex {
    entries: Vec<IndexEntry>,
}

impl SSTableIndex {
    fn load(data: &[u8]) -> Result<Self> {
        let mut cursor = io::Cursor::new(data);
        let entry_count = cursor.read_u32::<LittleEndian>()? as usize;
        
        let mut entries = Vec::with_capacity(entry_count);
        
        for _ in 0..entry_count {
            let key_len = cursor.read_u32::<LittleEndian>()? as usize;
            let mut key = vec![0u8; key_len];
            cursor.read_exact(&mut key)?;
            
            let offset = cursor.read_u64::<LittleEndian>()?;
            let size = cursor.read_u32::<LittleEndian>()?;
            
            entries.push(IndexEntry { key, offset, size });
        }
        
        Ok(Self { entries })
    }
    
    fn find_block(&self, key: &[u8]) -> Option<&IndexEntry> {
        // Binary search for the first block whose last key >= target key
        match self.entries.binary_search_by(|entry| entry.key.as_slice().cmp(key)) {
            Ok(idx) => Some(&self.entries[idx]),
            Err(idx) => {
                if idx < self.entries.len() {
                    Some(&self.entries[idx])
                } else {
                    self.entries.last()
                }
            }
        }
    }
}

/// SSTable iterator
pub struct SSTableIterator<'a> {
    reader: &'a SSTableReader,
    index_pos: usize,
    block_data: Option<Vec<u8>>,
    block_cursor: io::Cursor<Vec<u8>>,
}

impl<'a> SSTableIterator<'a> {
    fn new(reader: &'a SSTableReader) -> Self {
        Self {
            reader,
            index_pos: 0,
            block_data: None,
            block_cursor: io::Cursor::new(Vec::new()),
        }
    }
    
    fn load_next_block(&mut self) -> Result<bool> {
        if self.index_pos >= self.reader.index.entries.len() {
            return Ok(false);
        }
        
        let entry = &self.reader.index.entries[self.index_pos];
        let block_data = self.reader.read_block(entry.offset, entry.size)?;
        
        self.block_cursor = io::Cursor::new(block_data);
        self.index_pos += 1;
        
        Ok(true)
    }
}

impl<'a> Iterator for SSTableIterator<'a> {
    type Item = Result<(Bytes, Bytes)>;
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to read from current block
            if self.block_cursor.position() < self.block_cursor.get_ref().len() as u64 {
                // Read key
                let key_len = match self.block_cursor.read_u32::<LittleEndian>() {
                    Ok(len) => len as usize,
                    Err(e) => return Some(Err(e.into())),
                };
                
                let mut key = vec![0u8; key_len];
                if let Err(e) = self.block_cursor.read_exact(&mut key) {
                    return Some(Err(e.into()));
                }
                
                // Read value
                let value_len = match self.block_cursor.read_u32::<LittleEndian>() {
                    Ok(len) => len as usize,
                    Err(e) => return Some(Err(e.into())),
                };
                
                let mut value = vec![0u8; value_len];
                if let Err(e) = self.block_cursor.read_exact(&mut value) {
                    return Some(Err(e.into()));
                }
                
                return Some(Ok((Bytes::from(key), Bytes::from(value))));
            }
            
            // Load next block
            match self.load_next_block() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_sstable_write_read() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        // Write SSTable
        {
            let mut writer = SSTableWriter::new(path, SSTableConfig::default()).unwrap();
            
            for i in 0..100 {
                let key = format!("key_{:04}", i);
                let value = format!("value_{}", i);
                writer.add(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            writer.finish().unwrap();
        }
        
        // Read SSTable
        {
            let reader = SSTableReader::open(path).unwrap();
            
            // Test get
            let value = reader.get(b"key_0050").unwrap().unwrap();
            assert_eq!(&value[..], b"value_50");
            
            // Test missing key
            assert!(reader.get(b"key_9999").unwrap().is_none());
            
            // Test iteration
            let mut count = 0;
            for result in reader.iter() {
                let (key, value) = result.unwrap();
                count += 1;
                
                // Verify first few entries
                if count <= 3 {
                    let expected_key = format!("key_{:04}", count - 1);
                    let expected_value = format!("value_{}", count - 1);
                    assert_eq!(&key[..], expected_key.as_bytes());
                    assert_eq!(&value[..], expected_value.as_bytes());
                }
            }
            assert_eq!(count, 100);
        }
    }
    
    #[test]
    fn test_sstable_compression() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let config = SSTableConfig {
            compression: CompressionType::Zstd,
            ..Default::default()
        };
        
        // Write compressed SSTable
        {
            let mut writer = SSTableWriter::new(path, config).unwrap();
            
            for i in 0..1000 {
                let key = format!("key_{:06}", i);
                let value = format!("This is a test value for key {} with some repetitive text to compress well", i);
                writer.add(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            let info = writer.finish().unwrap();
            
            // Compressed file should be smaller than uncompressed
            // (though this is just a rough check)
            assert!(info.file_size < 100_000);
        }
        
        // Read and verify
        {
            let reader = SSTableReader::open(path).unwrap();
            let value = reader.get(b"key_000500").unwrap().unwrap();
            assert!(String::from_utf8_lossy(&value).contains("key 500"));
        }
    }
}