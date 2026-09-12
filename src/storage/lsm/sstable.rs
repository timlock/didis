use crate::storage::lsm::{
    Error, Operation, read_integer, read_length_prefixed_string, read_operation, write_integer,
    write_length_prefixed_string, write_operation,
};
use log::info;
use std::fs::File;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

const BLOCK_SIZE: usize = 4096;

#[derive(Debug)]
pub struct SSTable {
    pub id: u64,
    pub name: String,
    pub path: PathBuf,
    pub key_range: RangeInclusive<String>,
    pub reader: SSTableReader,
}

impl SSTable {
    pub fn new(
        id: u64,
        name: String,
        path: PathBuf,
        key_range: RangeInclusive<String>,
        reader: SSTableReader,
    ) -> SSTable {
        SSTable {
            id,
            name,
            path,
            key_range,
            reader,
        }
    }
    pub fn has_key_in_range(&self, key: &str) -> bool {
        self.key_range.start().as_str() <= key && key <= self.key_range.end().as_str()
    }
}

#[derive(Debug)]
pub struct SSTableWriter {
    file: File,
    block: Vec<u8>,
    keys: Vec<(String, u64)>,
}
impl SSTableWriter {
    pub fn create(table_path: &Path) -> io::Result<SSTableWriter> {
        info!("Creating new table {:?}", table_path);
        let file = File::create(&table_path)?;
        Ok(SSTableWriter {
            file,
            block: Vec::with_capacity(BLOCK_SIZE),
            keys: Vec::new(),
        })
    }

    pub fn write_operation(&mut self, operation: Operation) -> io::Result<()> {
        if self.block.is_empty() {
            self.block.resize(16, 0);

            let stream_pos = self.file.stream_position()?;
            self.keys.push((operation.key().to_owned(), stream_pos));
        }

        let mut buf = Vec::new();
        write_operation(&mut buf, &operation)?;

        if buf.len() + self.block.len() > BLOCK_SIZE && !self.block.is_empty() {
            self.flush_data_block()?;
            self.block.resize(16, 0);

            let data_block_offset = self.file.stream_position()?;
            assert_eq!(data_block_offset % BLOCK_SIZE as u64, 0);
            self.keys
                .push((operation.key().to_owned(), data_block_offset));
        }

        self.block.extend(&buf);
        buf.clear();

        Ok(())
    }

    pub fn write_data_blocks<'a>(
        &mut self,
        operations: impl IntoIterator<Item = &'a Operation>,
    ) -> io::Result<()> {
        assert!(self.block.is_empty());
        self.block.resize(16, 0);

        let mut operations = operations.into_iter().peekable();

        let first_operation = operations
            .peek()
            .expect("write_operations should not be called with an empty slice");
        self.keys.push((first_operation.key().to_owned(), 0));

        let mut buf = Vec::new();
        for operation in operations {
            write_operation(&mut buf, &operation)?;

            if buf.len() + self.block.len() > BLOCK_SIZE && !self.block.is_empty() {
                self.flush_data_block()?;
                self.block.resize(16, 0);

                let data_block_offset = self.file.stream_position()?;
                assert_eq!(data_block_offset % BLOCK_SIZE as u64, 0);
                self.keys
                    .push((operation.key().to_owned(), data_block_offset));
            }

            self.block.extend(&buf);
            buf.clear();
        }
        self.flush_data_block()?;

        Ok(())
    }

    pub fn write_index_blocks(&mut self) -> io::Result<()> {
        self.flush_data_block()?;
        assert!(self.block.is_empty());
        self.block.resize(8, 0);

        let index_block_offset = self.file.stream_position()?;
        assert_eq!(index_block_offset as usize % BLOCK_SIZE, 0);

        let mut buf = Vec::new();
        for i in 0..self.keys.len() {
            let (key, offset) = &self.keys[i];

            write_length_prefixed_string(&mut buf, &key)?;
            write_integer(&mut buf, *offset)?;

            if buf.len() + self.block.len() > BLOCK_SIZE {
                self.flush_index_block()?;

                self.block.resize(8, 0);

                let index_block_offset = self.file.stream_position()? as usize;
                assert_eq!(index_block_offset % BLOCK_SIZE, 0);
            }

            self.block.extend(&buf);
            buf.clear();
        }
        self.flush_index_block()?;

        self.file.write_all(&index_block_offset.to_le_bytes())?;

        Ok(())
    }

    fn flush_data_block(&mut self) -> io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }

        assert_eq!([0u8; 16], &self.block[..16]);
        let content_size = self.block.len() - 16;
        self.block[0..8].clone_from_slice(content_size.to_le_bytes().as_slice());

        let checksum = crc64::crc64(0, &self.block[16..]);
        self.block[8..16].clone_from_slice(checksum.to_le_bytes().as_slice());

        self.add_padding()?;

        self.file.write_all(&self.block)?;
        self.block.clear();

        Ok(())
    }

    fn flush_index_block(&mut self) -> io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }

        assert_eq!([0u8; 8], &self.block[..8]);
        let content_size = self.block.len() - 8;
        self.block[0..8].clone_from_slice(content_size.to_le_bytes().as_slice());

        self.add_padding()?;

        self.file.write_all(&self.block)?;
        self.block.clear();

        Ok(())
    }

    fn add_padding(&mut self) -> io::Result<()> {
        if self.block.len() % BLOCK_SIZE == 0 {
            return Ok(());
        }

        let padding = BLOCK_SIZE - (self.block.len() % BLOCK_SIZE);
        self.block.resize(self.block.len() + padding, 0);

        assert_eq!(self.block.len() % BLOCK_SIZE, 0);

        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct SSTableReader {
    file: BufReader<File>,
}

impl SSTableReader {
    pub fn open(table_path: impl AsRef<Path>) -> io::Result<SSTableReader> {
        info!("Opening table {:?}", table_path.as_ref());

        let file = File::open(table_path)?;
        Ok(SSTableReader {
            file: BufReader::new(file),
        })
    }

    pub fn find_value(&mut self, key: &str) -> Result<Option<Operation>, Error> {
        let offset = match self.find_block(key)? {
            Some(offset) => offset,
            None => return Ok(None),
        };

        let mut data_block_iter = DataBlockIter::new(&mut self.file, offset)?;

        while let Some(next) = data_block_iter.next() {
            let operation = next?;

            if operation.key() > key {
                return Ok(None);
            }

            if operation.key() == key {
                return Ok(Some(operation));
            }
        }

        Ok(None)
    }

    pub fn find_block(&mut self, key: &str) -> Result<Option<u64>, Error> {
        let mut index_iter = self.index_iter()?;

        let mut candidate = None;

        while let Some(next) = index_iter.next() {
            let (got_key, offset) = next?;
            if got_key.as_str() > key {
                return Ok(candidate);
            }

            if got_key.as_str() == key {
                return Ok(Some(offset));
            }

            candidate = Some(offset);
        }

        Ok(candidate)
    }

    fn area_offsets(&mut self) -> io::Result<(u64, u64)> {
        let header_offset = self.file.seek(SeekFrom::End(-8))?;

        let mut first_index_offset_bytes = [0; 8];
        self.file.read_exact(&mut first_index_offset_bytes)?;

        Ok((header_offset, u64::from_le_bytes(first_index_offset_bytes)))
    }

    pub fn index_iter(&mut self) -> io::Result<SSTableIndexIter> {
        let (header_offset, first_index_offset) = self.area_offsets()?;

        let index_block_iter = IndexBlockIter::new(&mut self.file, first_index_offset)?;

        Ok(SSTableIndexIter {
            index_block_iter: Some(index_block_iter),
            header_offset,
        })
    }

    pub fn data_iter(&mut self) -> Result<SSTableDataIter, Error> {
        let (_, first_index_offset) = self.area_offsets()?;
        self.file.rewind()?;

        let data_block_iter = DataBlockIter::new(&mut self.file, 0)?;

        Ok(SSTableDataIter {
            data_block_iter: Some(data_block_iter),
            first_index_offset,
        })
    }
}

struct IndexBlockIter<'a> {
    file: &'a mut BufReader<File>,
    offset: u64,
    content_len: u64,
}

impl<'a> IndexBlockIter<'a> {
    fn new(file: &'a mut BufReader<File>, offset: u64) -> io::Result<IndexBlockIter<'a>> {
        assert_eq!(0, offset % BLOCK_SIZE as u64);
        file.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let content_len = u64::from_le_bytes(buf);

        Ok(IndexBlockIter {
            file,
            offset,
            content_len,
        })
    }
}

impl<'a> Iterator for IndexBlockIter<'a> {
    type Item = Result<(String, u64), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let stream_pos = match self.file.stream_position() {
            Ok(stream_pos) => stream_pos,
            Err(err) => return Some(Err(err.into())),
        };

        if stream_pos >= self.offset + self.content_len + 8 {
            return None;
        }

        let key = match read_length_prefixed_string(&mut self.file) {
            Ok(key) => key,
            Err(err) => return Some(Err(err.into())),
        };

        let offset = match read_integer(&mut self.file) {
            Ok(offset) => offset,
            Err(err) => return Some(Err(err.into())),
        };

        Some(Ok((key, offset)))
    }
}

pub struct SSTableIndexIter<'a> {
    index_block_iter: Option<IndexBlockIter<'a>>,
    header_offset: u64,
}
impl<'a> Iterator for SSTableIndexIter<'a> {
    type Item = Result<(String, u64), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let index_block_iter = self.index_block_iter.as_mut()?;

        match index_block_iter.next() {
            Some(next) => Some(next),
            None => {
                let file = self.index_block_iter.take()?.file;
                let stream_pos = match file.stream_position() {
                    Ok(stream_pos) => stream_pos,
                    Err(err) => return Some(Err(err.into())),
                };

                let padding = BLOCK_SIZE as u64 - (stream_pos % BLOCK_SIZE as u64);
                let next_index_block = stream_pos + padding;

                if next_index_block >= self.header_offset {
                    return None;
                }

                match file.seek_relative(padding as i64) {
                    Ok(()) => {}
                    Err(err) => return Some(Err(err.into())),
                };

                let mut index_block_iter = match IndexBlockIter::new(file, next_index_block) {
                    Ok(index_block_iter) => index_block_iter,
                    Err(err) => return Some(Err(err.into())),
                };

                let next = index_block_iter.next();
                self.index_block_iter = Some(index_block_iter);

                next
            }
        }
    }
}

pub struct SSTableDataIter<'a> {
    data_block_iter: Option<DataBlockIter<'a>>,
    first_index_offset: u64,
}

impl<'a> Iterator for SSTableDataIter<'a> {
    type Item = Result<Operation, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let data_block_iter = self.data_block_iter.as_mut()?;

        let next = match data_block_iter.next() {
            Some(next) => Some(next),
            None => {
                let file = self.data_block_iter.take()?.file;
                let stream_pos = match file.stream_position() {
                    Ok(stream_pos) => stream_pos,
                    Err(err) => return Some(Err(err.into())),
                };

                let padding = BLOCK_SIZE as u64 - (stream_pos % BLOCK_SIZE as u64);
                let next_data_block = stream_pos + padding;

                if next_data_block >= self.first_index_offset {
                    return None;
                }

                let mut data_block_iter = match DataBlockIter::new(file, next_data_block) {
                    Ok(data_block_iter) => data_block_iter,
                    Err(err) => return Some(Err(err.into())),
                };

                let next = data_block_iter.next();
                self.data_block_iter = Some(data_block_iter);

                next
            }
        };

        next
    }
}

struct DataBlockIter<'a> {
    file: &'a mut BufReader<File>,
    offset: u64,
    content_len: u64,
}

impl<'a> DataBlockIter<'a> {
    fn new(file: &'a mut BufReader<File>, offset: u64) -> Result<DataBlockIter<'a>, Error> {
        assert_eq!(0, offset % BLOCK_SIZE as u64);
        file.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let content_len = u64::from_le_bytes(buf);

        file.read_exact(&mut buf)?;
        let want_checksum = u64::from_le_bytes(buf);

        let mut buf = vec![0u8; content_len as usize];
        file.read_exact(&mut buf)?;

        let got_checksum = crc64::crc64(0, &buf);
        if want_checksum != got_checksum {
            return Err(Error::ChecksumMismatch {
                want: want_checksum,
                got: got_checksum,
            });
        }

        file.seek(SeekFrom::Start(offset + 16))?;

        Ok(DataBlockIter {
            file,
            offset,
            content_len,
        })
    }
}

impl<'a> Iterator for DataBlockIter<'a> {
    type Item = Result<Operation, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let stream_pos = match self.file.stream_position() {
            Ok(stream_pos) => stream_pos,
            Err(err) => return Some(Err(err.into())),
        };

        if stream_pos >= self.offset + self.content_len + 16 {
            return None;
        }

        Some(read_operation(&mut self.file))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_dir::TempDir;
    use std::{env, error};

    #[test]
    fn write_data_blocks() -> Result<(), Box<dyn error::Error>> {
        let temp_dir = TempDir::create(env::current_dir()?.join("temp"))?;
        let path = temp_dir.path().join("table");
        let mut writer = SSTableWriter::create(&path)?;


        let mut operations = Vec::new();
        for i in 0..1000 {
            operations.push(Operation::Insert(i.to_string(), (i * 10).to_string()))
        }
        operations.sort_by(|a, b| a.key().cmp(b.key()));

        writer.write_data_blocks(&operations)?;

        writer.write_index_blocks()?;
        writer.sync()?;

        let mut reader = SSTableReader::open(&path)?;
        for operation in operations {
            let key = operation.key().to_string();
            assert_eq!(Some(operation), reader.find_value(&key)?);
        }

        Ok(())
    }
}
