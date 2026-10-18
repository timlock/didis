use crate::storage::lsm::{Error, read_integer, write_integer};
use std::io;
use std::io::Cursor;
use xxhash_rust::xxh64::xxh64;

const SEED_1: u64 = 0x9E3779B185EBCA87;
const SEED_2: u64 = 0xC2B2AE3D27D4EB4F;

#[derive(Debug, Default)]
pub struct BloomFilterBuilder {
    hashes: Vec<(u64, u64)>,
}

impl BloomFilterBuilder {
    pub fn insert(&mut self, value: &str) {
        self.hashes
            .push((xxh64(value.as_bytes(), SEED_1), xxh64(value.as_bytes(), SEED_2)))
    }

    pub fn append<'a>(&mut self, values: impl IntoIterator<Item= &'a str>) {
        for value in values {
            self.insert(value);
        }
        
    }

    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    pub fn build(self, desired_error_rate: f32, hash_functions: u32) -> BloomFilter {
        let mut bloom_filter =
            BloomFilter::new(self.hashes.len() as u32, desired_error_rate, hash_functions);
        for (first_hash, second_hash) in self.hashes {
            bloom_filter.insert_hash(first_hash, second_hash);
        }

        bloom_filter
    }
}

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

    pub fn insert(&mut self, value: &str) {
        let (block, bits) = self.positions_string(value);

        for bit in bits {
            self.blocks[block] |= 1 << bit;
        }
    }

    fn insert_hash(&mut self, first_hash: u64, second_hash: u64) {
        let (block, bits) = self.positions_hashes(first_hash, second_hash);

        for bit in bits {
            self.blocks[block] |= 1 << bit;
        }
    }

    pub fn append<'a>(&mut self, keys: impl IntoIterator<Item = &'a str>) {
        for key in keys {
            self.insert(key);
        }
    }

    pub fn contains(&self, value: &str) -> bool {
        let (block, bits) = self.positions_string(value);

        for bit in bits {
            if self.blocks[block] >> bit & 1 == 0 {
                return false;
            }
        }
        true
    }

    fn positions_string(&self, key: &str) -> (usize, impl Iterator<Item = usize> + use<>) {
        self.positions_hashes(xxh64(key.as_bytes(), SEED_1), xxh64(key.as_bytes(), SEED_2))
    }

    fn positions_hashes(
        &self,
        first_hash: u64,
        second_hash: u64,
    ) -> (usize, impl Iterator<Item = usize> + use<>) {
        let first_hash = first_hash as u128;
        let second_hash = second_hash as u128;

        let blocks_len = self.blocks.len() as u128;

        let block = (first_hash + second_hash) % blocks_len;
        let mut index = Vec::new();

        for i in 1u128..self.hash_functions as u128 {
            index.push(((first_hash + i * second_hash) % blocks_len) as usize);
        }

        (block as usize, index.into_iter())
    }
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

        Ok(BloomFilter {
            hash_functions,
            blocks,
        })
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
        let mut filter = BloomFilter::new(5, 0.01, 5);
        filter.insert("1");
        filter.insert("2");
        filter.insert("3");
        filter.insert("4");
        filter.insert("5");

        let buf = Vec::try_from(&filter)?;
        let filter = BloomFilter::try_from(buf)?;

        assert!(filter.contains("1"));
        assert!(filter.contains("2"));
        assert!(filter.contains("3"));
        assert!(filter.contains("4"));
        assert!(filter.contains("5"));

        Ok(())
    }
}
