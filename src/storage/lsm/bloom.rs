use crate::storage::lsm::{Error, read_integer, write_integer};
use log::info;
use std::fs::File;
use std::io::{Cursor, Write};
use std::path::Path;
use std::{fs, io};
use xxhash_rust::xxh64::xxh64;

const SEED_1: u64 = 0x9E3779B185EBCA87;
const SEED_2: u64 = 0xC2B2AE3D27D4EB4F;

#[derive(Debug)]
pub struct BloomFilter {
    hash_functions: u32,
    blocks: Vec<u64>,
}
impl BloomFilter {
    pub fn new(max_entries: u32, desired_error_rate: f32, hash_functions: u32) -> BloomFilter {
        let bit_len = estimate_bit_vector_size(max_entries, desired_error_rate, hash_functions);
        let blocks = vec![0u64; bit_len / 64 * 8];

        BloomFilter {
            hash_functions,
            blocks,
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Result<BloomFilter, Error> {
        let bytes = fs::read(path)?;
        BloomFilter::try_from(bytes)
    }

    pub fn insert(&mut self, key: &str) {
        let (block, bits) = bits(key, self.blocks.len() as u128, self.hash_functions);

        for bit in bits {
            self.blocks[block] ^= 1 << bit;
        }
    }

    pub fn append<'a>(&mut self, keys: impl IntoIterator<Item= &'a str>) {
        for key in keys {
            self.insert(key);
        }
    }

    pub fn contains(&self, key: &str) -> bool {
        let (block, bits) = bits(key, self.blocks.len() as u128, self.hash_functions);

        for bit in bits {
            if self.blocks[block] >> bit & 1 == 0 {
                return false;
            }
        }
        true
    }
}

#[derive(Debug)]
pub struct BloomFilterWriter {
    bloom_filter: BloomFilter,
    file: File,
}

impl BloomFilterWriter {
    pub fn create(
        max_entries: u32,
        desired_error_rate: f32,
        hash_functions: u32,
        path: impl AsRef<Path>,
    ) -> io::Result<BloomFilterWriter> {
        info!("Creating new bloom filter {:?}", path.as_ref());
        let file = File::create(path)?;

        let bloom_filter = BloomFilter::new(max_entries, desired_error_rate, hash_functions);

        Ok(BloomFilterWriter { bloom_filter, file })
    }

    pub fn insert(&mut self, key: &str) {
        self.bloom_filter.insert(key);
    }

    pub fn sync(&mut self) -> io::Result<()> {
        let bytes = Vec::try_from(&self.bloom_filter)?;
        self.file.write_all(&bytes)
    }
}

#[derive(Debug)]
pub struct BloomFilterReader {
    bloom_filter: BloomFilter,
}

impl BloomFilterReader {
    pub fn open(path: impl AsRef<Path>) -> Result<BloomFilterReader, Error> {
        let bytes = fs::read(path)?;
        Ok(BloomFilterReader {
            bloom_filter: BloomFilter::try_from(bytes)?,
        })
    }

    pub fn contains(&self, key: &str) -> bool {
        self.bloom_filter.contains(key)
    }
}

fn bits(
    key: &str,
    blocks_len: u128,
    hash_functions: u32,
) -> (usize, impl Iterator<Item = usize> + use<>) {
    let first_hash = xxh64(key.as_bytes(), SEED_1) as u128;
    let second_hash = xxh64(key.as_bytes(), SEED_2) as u128;

    let block = (first_hash + second_hash) % blocks_len;
    let mut index = Vec::new();

    for i in 1u128..hash_functions as u128 {
        index.push(((first_hash + i * second_hash) % blocks_len) as usize);
    }

    (block as usize, index.into_iter())
}

impl TryFrom<&BloomFilter> for Vec<u8> {
    type Error = io::Error;
    fn try_from(value: &BloomFilter) -> Result<Self, Self::Error> {
        let mut buf = Vec::new();
        write_integer(&mut buf, value.hash_functions as u64)?;

        for block in &value.blocks {
            for byte in block.to_le_bytes() {
                buf.push(byte);
            }
        }

        Ok(buf)
    }
}

impl TryFrom<Vec<u8>> for BloomFilter {
    type Error = Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let mut cursor = Cursor::new(value);
        let hash_functions = read_integer(&mut cursor)? as u32;

        let begin = cursor.position() as usize;
        let mut bytes = cursor.into_inner();
        drop(bytes.drain(..begin));

        let mut blocks = Vec::new();
        for chunk in bytes.chunks(8) {
            blocks.push(u64::from_le_bytes(chunk.try_into()?));
        }

        Ok((BloomFilter {
            hash_functions,
            blocks,
        }))
    }
}

fn estimate_bit_vector_size(
    max_entries: u32,
    desired_error_rate: f32,
    hash_functions: u32,
) -> usize {
    // source https://read.thecoder.cafe/p/bloom-filters
    let size = -((max_entries * hash_functions) as f32
        / (1.0 - desired_error_rate.powf(1.0 / hash_functions as f32)))
    .ln();

    let filler = 64 * 8 - (size as usize % (64 * 8));
    size as usize + filler
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::temp_dir::TempDir;
    use std::env;

    #[test]
    fn insert() {
        let mut filter = BloomFilter::new(5, 0.01, 5);
        filter.insert("1");
        assert!(filter.contains("1"));
        filter.insert("2");
        assert!(filter.contains("2"));
        filter.insert("3");
        assert!(filter.contains("3"));
        filter.insert("4");
        assert!(filter.contains("4"));
        filter.insert("5");

        assert!(filter.contains("1"));
        assert!(filter.contains("2"));
        assert!(filter.contains("3"));
        assert!(filter.contains("4"));
        assert!(filter.contains("5"));
    }

    #[test]
    fn estimate_bit_vector_size_t() {
        let bits = estimate_bit_vector_size(1953, 0.01, 5);
        assert_eq!(38 * 64 * 8, bits);
    }

    #[test]
    fn serialization() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::create(env::current_dir()?.join("temp"))?;
        let path = temp_dir.path().join("bloom-filter.db");
        let mut filter = BloomFilterWriter::create(5, 0.01, 5, &path)?;
        filter.insert("1");
        filter.insert("2");
        filter.insert("3");
        filter.insert("4");
        filter.insert("5");

        let filter = BloomFilterReader::open(&path)?;

        assert!(filter.contains("1"));
        assert!(filter.contains("2"));
        assert!(filter.contains("3"));
        assert!(filter.contains("4"));
        assert!(filter.contains("5"));

        Ok(())
    }
}
