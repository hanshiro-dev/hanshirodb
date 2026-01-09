use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use tracing::info;

use hanshiro_core::{
    crypto::{crc32_checksum, MerkleChain, MerkleNode},
    error::{Error, Result},
};

use super::types::*;

/// In-memory struct of an open WAL file.
pub(crate) struct WalFile {
    pub path: PathBuf,
    pub file: BufWriter<File>,
    pub size: u64,
    pub entry_count: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
}

pub(crate) fn create_file(wal_dir: &Path, sequence: u64, config: &WalConfig) -> Result<WalFile> {
    let filename = format!("{:020}.wal", sequence);
    let path = wal_dir.join(&filename);

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(&path)?;

    let mut writer = BufWriter::with_capacity(config.buffer_size, file);

    writer.write_all(WAL_MAGIC)?;
    writer.write_u32::<LittleEndian>(WAL_VERSION)?;
    writer.write_u64::<LittleEndian>(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )?;
    writer.write_u64::<LittleEndian>(sequence)?; // First sequence
    writer.write_u64::<LittleEndian>(sequence)?; // Last sequence (updated on finalize)
    writer.write_u64::<LittleEndian>(0)?; // Entry count
    writer.write_u32::<LittleEndian>(0)?; // Checksum placeholder
    writer.write_all(&[0u8; 16])?; // Reserved
    writer.flush()?;

    Ok(WalFile {
        path,
        file: writer,
        size: WAL_HEADER_SIZE as u64,
        entry_count: 0,
        first_sequence: sequence,
        last_sequence: sequence,
    })
}

pub(crate) fn recover_file(path: &Path, config: &WalConfig) -> Result<(WalFile, u64, MerkleChain)> {
    info!("Recovering from WAL file: {:?}", path);

    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut reader = BufReader::new(file);

    let mut magic = [0u8; 8];
    reader.read_exact(&mut magic)?; // Validate header
    if &magic != WAL_MAGIC {
        return Err(Error::WriteAheadLog {
            message: "Invalid WAL file magic number".to_string(),
            source: None,
        });
    }

    let version = reader.read_u32::<LittleEndian>()?;
    if version != WAL_VERSION {
        return Err(Error::WriteAheadLog {
            message: format!("Unsupported WAL version: {}", version),
            source: None,
        });
    }

    let _creation_time = reader.read_u64::<LittleEndian>()?;
    let first_sequence = reader.read_u64::<LittleEndian>()?;
    let mut last_sequence = reader.read_u64::<LittleEndian>()?;
    let entry_count = reader.read_u64::<LittleEndian>()?;
    let _checksum = reader.read_u32::<LittleEndian>()?;
    reader.read_exact(&mut [0u8; 16])?;

    let mut merkle_chain = MerkleChain::new();
    loop {
        match read_entry(&mut reader) {
            Ok(entry) => {
                merkle_chain.add(&entry.data);
                last_sequence = entry.sequence;
            }
            Err(_) => break,
        }
    }

    let file_size = reader.seek(SeekFrom::End(0))?;
    let mut file = reader.into_inner();
    file.seek(SeekFrom::End(0))?;
    let writer = BufWriter::with_capacity(config.buffer_size, file);

    Ok((
        WalFile {
            path: path.to_path_buf(),
            file: writer,
            size: file_size,
            entry_count,
            first_sequence,
            last_sequence,
        },
        last_sequence + 1,
        merkle_chain,
    ))
}

/// Update header with final sequence/count before rotation.
pub(crate) fn finalize_header(wal_file: &mut WalFile) -> Result<()> {
    wal_file.file.flush()?;
    let file = wal_file.file.get_mut();

    file.seek(SeekFrom::Start(28))?; // Offset of last_sequence
    file.write_u64::<LittleEndian>(wal_file.last_sequence)?;
    file.write_u64::<LittleEndian>(wal_file.entry_count)?;
    file.sync_all()?;
    file.seek(SeekFrom::End(0))?;

    Ok(())
}

pub(crate) fn read_header_last_sequence(path: &Path) -> Result<u64> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(28))?;
    Ok(file.read_u64::<LittleEndian>()?)
}

pub(crate) fn write_entry(writer: &mut impl Write, entry: &WalEntry) -> Result<()> {
    writer.write_u32::<LittleEndian>(entry.data.len() as u32)?;
    writer.write_u64::<LittleEndian>(entry.sequence)?;
    writer.write_u64::<LittleEndian>(entry.timestamp)?;
    writer.write_u8(entry.entry_type as u8)?;
    writer.write_u8(0)?; // Flags
    writer.write_u32::<LittleEndian>(crc32_checksum(&entry.data))?;
    writer.write_all(&[0u8; 6])?; // Reserved

    let prev_hash = entry
        .merkle
        .prev_hash
        .as_ref()
        .map(|h| hex::decode(h).unwrap())
        .unwrap_or_else(|| vec![0u8; 32]);
    writer.write_all(&prev_hash)?;
    writer.write_all(&hex::decode(&entry.merkle.hash).unwrap())?;
    writer.write_all(&hex::decode(&entry.merkle.data_hash).unwrap())?;
    writer.write_all(&entry.data)?;

    Ok(())
}

pub(crate) fn read_entry(reader: &mut impl Read) -> Result<WalEntry> {
    let length = match reader.read_u32::<LittleEndian>() {
        Ok(len) => len as usize,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(Error::WriteAheadLog {
                message: "EOF".to_string(),
                source: Some(Box::new(e)),
            });
        }
        Err(e) => return Err(e.into()),
    };

    let sequence = reader.read_u64::<LittleEndian>()?;
    let timestamp = reader.read_u64::<LittleEndian>()?;
    let entry_type = EntryType::try_from(reader.read_u8()?)?;
    let _flags = reader.read_u8()?;
    let crc = reader.read_u32::<LittleEndian>()?;
    reader.read_exact(&mut [0u8; 6])?;

    let mut prev_hash = [0u8; 32];
    let mut hash = [0u8; 32];
    let mut data_hash = [0u8; 32];
    reader.read_exact(&mut prev_hash)?;
    reader.read_exact(&mut hash)?;
    reader.read_exact(&mut data_hash)?;

    let mut data = vec![0u8; length];
    reader.read_exact(&mut data)?;

    if crc32_checksum(&data) != crc {
        return Err(Error::WriteAheadLog {
            message: "CRC mismatch".to_string(),
            source: None,
        });
    }

    Ok(WalEntry {
        sequence,
        timestamp,
        entry_type,
        data: Bytes::from(data),
        merkle: MerkleNode {
            hash: hex::encode(hash),
            prev_hash: if prev_hash == [0u8; 32] {
                None
            } else {
                Some(hex::encode(prev_hash))
            },
            data_hash: hex::encode(data_hash),
            sequence,
        },
    })
}

pub(crate) fn entry_size(entry: &WalEntry) -> usize {
    ENTRY_HEADER_SIZE + MERKLE_INFO_SIZE + entry.data.len()
}

/// Optimized batch write that minimizes syscalls
pub(crate) fn write_entries_batch<T: AsRef<WalEntry>>(writer: &mut impl Write, entries: &[T]) -> Result<()> {
    let total_size: usize = entries.iter().map(|e| entry_size(e.as_ref())).sum();
    let mut buffer = Vec::with_capacity(total_size);
    
    for entry in entries {
        let entry = entry.as_ref();
        buffer.extend_from_slice(&(entry.data.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&entry.sequence.to_le_bytes());
        buffer.extend_from_slice(&entry.timestamp.to_le_bytes());
        buffer.push(entry.entry_type as u8);
        buffer.push(0); // Flags
        buffer.extend_from_slice(&crc32_checksum(&entry.data).to_le_bytes());
        buffer.extend_from_slice(&[0u8; 6]); // Reserved
        
        let prev_hash = entry.merkle.prev_hash.as_ref()
            .map(|h| hex::decode(h).unwrap())
            .unwrap_or_else(|| vec![0u8; 32]);
        buffer.extend_from_slice(&prev_hash);
        buffer.extend_from_slice(&hex::decode(&entry.merkle.hash).unwrap());
        buffer.extend_from_slice(&hex::decode(&entry.merkle.data_hash).unwrap());
        
        buffer.extend_from_slice(&entry.data);
    }
    
    writer.write_all(&buffer)?;
    Ok(())
}
