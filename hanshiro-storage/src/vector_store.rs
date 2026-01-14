//! # Memory-Mapped Vector Storage
//!
//! Separate storage for vectors, optimized for SIMD operations.
//!
//! ## Layout
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Vector File (.vec)                        │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Header (64 bytes, aligned):                                │
//! │    magic: [u8; 8] = "HNSHVEC\0"                             │
//! │    version: u32                                              │
//! │    dimension: u32                                            │
//! │    count: u64                                                │
//! │    entry_size: u32 (16 + dim*4, aligned to 64)              │
//! │    padding: [u8; 36]                                         │
//! │                                                              │
//! │  Entries (aligned to 64 bytes for SIMD):                    │
//! │    [id_hi: u64][id_lo: u64][f32; dimension][padding]        │
//! │    [id_hi: u64][id_lo: u64][f32; dimension][padding]        │
//! │    ...                                                       │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};
use parking_lot::RwLock;

use hanshiro_core::error::{Error, Result};
use hanshiro_core::EventId;

const MAGIC: &[u8; 8] = b"HNSHVEC\0";
const VERSION: u32 = 1;
const HEADER_SIZE: usize = 64;
const ALIGNMENT: usize = 64; // Cache line / AVX-512 alignment

/// Header for vector file
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct VectorFileHeader {
    magic: [u8; 8],
    version: u32,
    dimension: u32,
    count: u64,
    entry_size: u32,
    _padding: [u8; 36],
}

impl VectorFileHeader {
    fn new(dimension: u32) -> Self {
        let entry_size = Self::compute_entry_size(dimension);
        Self {
            magic: *MAGIC,
            version: VERSION,
            dimension,
            count: 0,
            entry_size,
            _padding: [0u8; 36],
        }
    }

    fn compute_entry_size(dimension: u32) -> u32 {
        // 16 bytes for EventId + dimension * 4 bytes for floats
        let raw_size = 16 + dimension * 4;
        // Align to 64 bytes
        ((raw_size + ALIGNMENT as u32 - 1) / ALIGNMENT as u32) * ALIGNMENT as u32
    }

    fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..12].copy_from_slice(&self.version.to_le_bytes());
        buf[12..16].copy_from_slice(&self.dimension.to_le_bytes());
        buf[16..24].copy_from_slice(&self.count.to_le_bytes());
        buf[24..28].copy_from_slice(&self.entry_size.to_le_bytes());
        buf
    }

    fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_SIZE {
            return Err(Error::Internal { message: "Header too small".into() });
        }
        let magic: [u8; 8] = buf[0..8].try_into().unwrap();
        if &magic != MAGIC {
            return Err(Error::Internal { message: "Invalid vector file magic".into() });
        }
        Ok(Self {
            magic,
            version: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            dimension: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            count: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            entry_size: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            _padding: [0u8; 36],
        })
    }
}

/// Memory-mapped vector storage
pub struct VectorStore {
    path: PathBuf,
    dimension: usize,
    entry_size: usize,
    mmap: RwLock<Option<MmapMut>>,
    file: RwLock<File>,
    count: AtomicU64,
    /// Index: EventId -> offset in file
    index: RwLock<HashMap<EventId, u64>>,
    capacity: AtomicU64,
}

impl VectorStore {
    /// Create or open a vector store
    pub fn open(path: impl AsRef<Path>, dimension: usize) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let exists = path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|e| Error::Io { message: format!("Failed to open vector file: {}", e), source: e })?;

        let (header, index, mmap) = if exists && file.metadata().map(|m| m.len()).unwrap_or(0) >= HEADER_SIZE as u64 {
            // Load existing
            let mut buf = [0u8; HEADER_SIZE];
            (&file).read_exact(&mut buf).ok();
            let header = VectorFileHeader::from_bytes(&buf)?;
            
            if header.dimension as usize != dimension {
                return Err(Error::Internal { 
                    message: format!("Dimension mismatch: file has {}, requested {}", header.dimension, dimension) 
                });
            }

            let index = Self::build_index(&file, &header)?;
            let mmap = if header.count > 0 {
                Some(unsafe { MmapOptions::new().map_mut(&file) }
                    .map_err(|e| Error::Internal { message: format!("mmap failed: {}", e) })?)
            } else {
                None
            };

            (header, index, mmap)
        } else {
            // Create new
            let header = VectorFileHeader::new(dimension as u32);
            (&file).write_all(&header.to_bytes())
                .map_err(|e| Error::Io { message: "Failed to write header".into(), source: e })?;
            file.sync_all().ok();
            
            (header, HashMap::new(), None)
        };

        let capacity = file.metadata().map(|m| m.len()).unwrap_or(HEADER_SIZE as u64);

        Ok(Self {
            path,
            dimension,
            entry_size: header.entry_size as usize,
            mmap: RwLock::new(mmap),
            file: RwLock::new(file),
            count: AtomicU64::new(header.count),
            index: RwLock::new(index),
            capacity: AtomicU64::new(capacity),
        })
    }

    /// Build index from existing file
    fn build_index(file: &File, header: &VectorFileHeader) -> Result<HashMap<EventId, u64>> {
        let mut index = HashMap::with_capacity(header.count as usize);
        let entry_size = header.entry_size as usize;

        if header.count == 0 {
            return Ok(index);
        }

        let mmap = unsafe { MmapOptions::new().map(file) }
            .map_err(|e| Error::Internal { message: format!("mmap failed: {}", e) })?;

        for i in 0..header.count {
            let offset = HEADER_SIZE + (i as usize * entry_size);
            if offset + 16 > mmap.len() {
                break;
            }
            let hi = u64::from_le_bytes(mmap[offset..offset+8].try_into().unwrap());
            let lo = u64::from_le_bytes(mmap[offset+8..offset+16].try_into().unwrap());
            index.insert(EventId { hi, lo }, i);
        }

        Ok(index)
    }

    /// Insert a vector for an event
    pub fn insert(&self, id: EventId, vector: &[f32]) -> Result<()> {
        if vector.len() != self.dimension {
            return Err(Error::Internal {
                message: format!("Dimension mismatch: expected {}, got {}", self.dimension, vector.len()),
            });
        }

        // Check if already exists
        if self.index.read().contains_key(&id) {
            return Ok(()); // Idempotent
        }

        let idx = self.count.fetch_add(1, Ordering::SeqCst);
        let offset = HEADER_SIZE + (idx as usize * self.entry_size);
        let required_size = offset + self.entry_size;

        // Grow file if needed
        self.ensure_capacity(required_size as u64)?;

        // Write entry
        {
            let mut mmap = self.mmap.write();
            let mmap = mmap.as_mut().ok_or_else(|| Error::Internal { message: "No mmap".into() })?;

            // Write EventId
            mmap[offset..offset+8].copy_from_slice(&id.hi.to_le_bytes());
            mmap[offset+8..offset+16].copy_from_slice(&id.lo.to_le_bytes());

            // Write vector (aligned)
            let vec_offset = offset + 16;
            for (i, &val) in vector.iter().enumerate() {
                let pos = vec_offset + i * 4;
                mmap[pos..pos+4].copy_from_slice(&val.to_le_bytes());
            }
        }

        // Update index
        self.index.write().insert(id, idx);

        // Update header count periodically (every 1000 inserts)
        if idx % 1000 == 0 {
            self.flush_header()?;
        }

        Ok(())
    }

    /// Ensure file has at least `size` bytes
    fn ensure_capacity(&self, size: u64) -> Result<()> {
        let current = self.capacity.load(Ordering::Relaxed);
        if size <= current {
            return Ok(());
        }

        // Grow by 2x or to required size, whichever is larger
        let new_size = std::cmp::max(size, current * 2).max(HEADER_SIZE as u64 + self.entry_size as u64 * 1024);

        {
            let file = self.file.write();
            file.set_len(new_size)
                .map_err(|e| Error::Io { message: "Failed to grow file".into(), source: e })?;
        }

        // Remap
        {
            let file = self.file.read();
            let new_mmap = unsafe { MmapOptions::new().map_mut(&*file) }
                .map_err(|e| Error::Internal { message: format!("mmap failed: {}", e) })?;
            *self.mmap.write() = Some(new_mmap);
        }

        self.capacity.store(new_size, Ordering::Relaxed);
        Ok(())
    }

    /// Get vector by EventId
    pub fn get(&self, id: &EventId) -> Option<Vec<f32>> {
        let idx = *self.index.read().get(id)?;
        self.get_by_index(idx as usize)
    }

    /// Get vector by index (for iteration)
    pub fn get_by_index(&self, idx: usize) -> Option<Vec<f32>> {
        let mmap = self.mmap.read();
        let mmap = mmap.as_ref()?;

        let offset = HEADER_SIZE + idx * self.entry_size;
        if offset + 16 + self.dimension * 4 > mmap.len() {
            return None;
        }

        let vec_offset = offset + 16;
        let mut vector = Vec::with_capacity(self.dimension);
        for i in 0..self.dimension {
            let pos = vec_offset + i * 4;
            let val = f32::from_le_bytes(mmap[pos..pos+4].try_into().ok()?);
            vector.push(val);
        }

        Some(vector)
    }

    /// Get EventId by index
    pub fn get_id_by_index(&self, idx: usize) -> Option<EventId> {
        let mmap = self.mmap.read();
        let mmap = mmap.as_ref()?;

        let offset = HEADER_SIZE + idx * self.entry_size;
        if offset + 16 > mmap.len() {
            return None;
        }

        let hi = u64::from_le_bytes(mmap[offset..offset+8].try_into().ok()?);
        let lo = u64::from_le_bytes(mmap[offset+8..offset+16].try_into().ok()?);
        Some(EventId { hi, lo })
    }

    /// Get raw pointer to vector data (for SIMD operations)
    /// SAFETY: Caller must ensure index is valid and not access beyond dimension
    #[inline]
    pub unsafe fn get_vector_ptr(&self, idx: usize) -> Option<*const f32> {
        let mmap = self.mmap.read();
        let mmap = mmap.as_ref()?;

        let offset = HEADER_SIZE + idx * self.entry_size + 16;
        if offset + self.dimension * 4 > mmap.len() {
            return None;
        }

        Some(mmap.as_ptr().add(offset) as *const f32)
    }

    /// Number of vectors stored
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Vector dimension
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Find top-k similar vectors using brute force (SIMD-friendly)
    pub fn find_similar(&self, query: &[f32], k: usize) -> Vec<(EventId, f32)> {
        if query.len() != self.dimension || self.is_empty() {
            return Vec::new();
        }

        let count = self.len();
        let mut results: Vec<(EventId, f32)> = Vec::with_capacity(count);

        let mmap = self.mmap.read();
        if let Some(mmap) = mmap.as_ref() {
            for idx in 0..count {
                let offset = HEADER_SIZE + idx * self.entry_size;
                
                // Read ID
                let hi = u64::from_le_bytes(mmap[offset..offset+8].try_into().unwrap());
                let lo = u64::from_le_bytes(mmap[offset+8..offset+16].try_into().unwrap());
                let id = EventId { hi, lo };

                // Compute cosine similarity
                let vec_offset = offset + 16;
                let similarity = self.cosine_similarity_raw(&mmap[vec_offset..], query);
                
                results.push((id, similarity));
            }
        }

        // Sort by similarity descending
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k);
        results
    }

    /// Compute cosine similarity from raw bytes
    #[inline]
    fn cosine_similarity_raw(&self, vec_bytes: &[u8], query: &[f32]) -> f32 {
        let mut dot = 0.0f32;
        let mut norm_a = 0.0f32;
        let mut norm_b = 0.0f32;

        for i in 0..self.dimension {
            let pos = i * 4;
            let a = f32::from_le_bytes(vec_bytes[pos..pos+4].try_into().unwrap());
            let b = query[i];
            dot += a * b;
            norm_a += a * a;
            norm_b += b * b;
        }

        let denom = (norm_a * norm_b).sqrt();
        if denom > 0.0 {
            dot / denom
        } else {
            0.0
        }
    }

    /// Flush header to disk
    pub fn flush_header(&self) -> Result<()> {
        let count = self.count.load(Ordering::Relaxed);
        let mut header = VectorFileHeader::new(self.dimension as u32);
        header.count = count;

        let mut mmap = self.mmap.write();
        if let Some(mmap) = mmap.as_mut() {
            mmap[0..HEADER_SIZE].copy_from_slice(&header.to_bytes());
            mmap.flush().ok();
        }
        Ok(())
    }

    /// Sync all data to disk
    pub fn sync(&self) -> Result<()> {
        self.flush_header()?;
        if let Some(mmap) = self.mmap.write().as_mut() {
            mmap.flush()
                .map_err(|e| Error::Internal { message: format!("Flush failed: {}", e) })?;
        }
        Ok(())
    }

    /// Iterate over all (id, vector) pairs
    pub fn iter(&self) -> VectorStoreIter<'_> {
        VectorStoreIter {
            store: self,
            idx: 0,
            count: self.len(),
        }
    }
}

impl Drop for VectorStore {
    fn drop(&mut self) {
        self.sync().ok();
    }
}

/// Iterator over vector store entries
pub struct VectorStoreIter<'a> {
    store: &'a VectorStore,
    idx: usize,
    count: usize,
}

impl<'a> Iterator for VectorStoreIter<'a> {
    type Item = (EventId, Vec<f32>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.count {
            return None;
        }

        let id = self.store.get_id_by_index(self.idx)?;
        let vec = self.store.get_by_index(self.idx)?;
        self.idx += 1;
        Some((id, vec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_and_insert() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.vec");
        
        let store = VectorStore::open(&path, 128).unwrap();
        assert_eq!(store.len(), 0);
        assert_eq!(store.dimension(), 128);

        let id = EventId::new();
        let vec: Vec<f32> = (0..128).map(|i| i as f32 / 128.0).collect();
        
        store.insert(id, &vec).unwrap();
        assert_eq!(store.len(), 1);

        let retrieved = store.get(&id).unwrap();
        assert_eq!(retrieved.len(), 128);
        for (a, b) in retrieved.iter().zip(vec.iter()) {
            assert!((a - b).abs() < 1e-6);
        }
    }

    #[test]
    fn test_persistence() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.vec");
        
        let id = EventId::new();
        let vec: Vec<f32> = (0..64).map(|i| i as f32).collect();

        // Write
        {
            let store = VectorStore::open(&path, 64).unwrap();
            store.insert(id, &vec).unwrap();
            store.sync().unwrap();
        }

        // Read back
        {
            let store = VectorStore::open(&path, 64).unwrap();
            assert_eq!(store.len(), 1);
            let retrieved = store.get(&id).unwrap();
            assert_eq!(retrieved, vec);
        }
    }

    #[test]
    fn test_find_similar() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.vec");
        
        let store = VectorStore::open(&path, 4).unwrap();

        // Insert some vectors
        let id1 = EventId { hi: 1, lo: 1 };
        let id2 = EventId { hi: 2, lo: 2 };
        let id3 = EventId { hi: 3, lo: 3 };

        store.insert(id1, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        store.insert(id2, &[0.9, 0.1, 0.0, 0.0]).unwrap(); // Similar to id1
        store.insert(id3, &[0.0, 0.0, 1.0, 0.0]).unwrap(); // Different

        let query = [1.0, 0.0, 0.0, 0.0];
        let results = store.find_similar(&query, 2);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, id1); // Most similar
        assert_eq!(results[1].0, id2); // Second most similar
        assert!(results[0].1 > results[1].1);
    }
}
