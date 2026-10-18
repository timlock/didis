use crate::storage::lsm::{
    Error, Operation, read_integer, read_length_prefixed_string, read_operation, write_integer,
    write_length_prefixed_string, write_operation,
};
use log::info;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::Path;
use std::{io, iter};

const BLOCK_SIZE: usize = 4096;

#[derive(Debug)]
pub struct SSTableReader {
    index_reader: IndexReader,
    data_reader: DataReader,
}

impl SSTableReader {
    pub fn open<P: AsRef<Path>>(
        index_file_path: P,
        data_file_path: P,
    ) -> io::Result<SSTableReader> {
        let index_reader = IndexReader::open(index_file_path)?;
        let data_reader = DataReader::open(data_file_path)?;

        Ok(SSTableReader {
            index_reader,
            data_reader,
        })
    }

    pub fn find_value(&mut self, key: &str) -> Result<Option<Operation>, Error> {
        let block_offset = match self.index_reader.find_block(key)? {
            Some(block_offset) => block_offset,
            None => return Ok(None),
        };
        self.data_reader.find_value(key, block_offset)
    }

    pub fn iter(&mut self) -> Result<DataIter<'_, BufReader<File>>, Error> {
        self.data_reader.iter()
    }
}

#[derive(Debug)]
pub struct SSTableWriter {
    index_writer: IndexWriter,
    data_writer: DataWriter,
    expect_first_key: bool,
}
impl SSTableWriter {
    pub fn create<P: AsRef<Path>>(
        data_file_path: P,
        index_file_path: P,
    ) -> io::Result<SSTableWriter> {
        Ok(SSTableWriter {
            index_writer: IndexWriter::create(index_file_path)?,
            data_writer: DataWriter::create(data_file_path)?,
            expect_first_key: true,
        })
    }

    pub fn write<'a>(
        &mut self,
        operations: impl IntoIterator<Item = &'a Operation>,
    ) -> io::Result<()> {
        let mut operations = operations.into_iter().peekable();

        if self.expect_first_key {
            match operations.peek() {
                Some(first_operation) => self
                    .index_writer
                    .write_keys(iter::once((first_operation.key(), 0)))?,
                None => return Ok(()),
            };
            self.expect_first_key = false;
        }

        let keys = self.data_writer.write(operations)?;
        self.index_writer.write_keys(keys)?;

        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.index_writer.sync()?;
        self.data_writer.sync()?;
        Ok(())
    }
}

#[derive(Debug)]
struct DataWriter {
    file: File,
    block: Vec<u8>,
    buf: Vec<u8>,
}
impl DataWriter {
    fn create<P: AsRef<Path>>(table_path: P) -> io::Result<DataWriter> {
        info!("Creating new table {:?}", table_path.as_ref());
        let file = File::create(table_path)?;

        let mut block = Vec::with_capacity(BLOCK_SIZE);
        block.resize(16, 0);

        Ok(DataWriter {
            file,
            block,
            buf: Vec::new(),
        })
    }

    fn write<'a>(
        &mut self,
        operations: impl IntoIterator<Item = &'a Operation>,
    ) -> io::Result<impl Iterator<Item = (&'a str, u64)>> {
        let mut keys = Vec::new();

        for operation in operations {
            write_operation(&mut self.buf, operation)?;

            if self.buf.len() + self.block.len() > BLOCK_SIZE && !self.block.is_empty() {
                self.flush()?;
                self.block.resize(16, 0);

                let data_block_offset = self.file.stream_position()?;
                assert_eq!(data_block_offset % BLOCK_SIZE as u64, 0);
                keys.push((operation.key(), data_block_offset));
            }

            self.block.append(&mut self.buf);
            self.buf.clear();
        }

        Ok(keys.into_iter())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }

        assert_eq!([0u8; 16], &self.block[..16]);
        let content_size = self.block.len() - 16;
        self.block[0..8].clone_from_slice(content_size.to_le_bytes().as_slice());

        let checksum = crc64::crc64(0, &self.block[16..]);
        self.block[8..16].clone_from_slice(checksum.to_le_bytes().as_slice());

        self.add_padding();

        self.file.write_all(&self.block)?;
        self.file.flush()?;

        self.block.clear();

        Ok(())
    }

    fn add_padding(&mut self) {
        let padding = calculate_padding(self.block.len() as u64);
        self.block.resize(self.block.len() + padding as usize, 0);

        assert_eq!(self.block.len() % BLOCK_SIZE, 0);
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}

#[derive(Debug)]
struct IndexWriter {
    file: File,
    block: Vec<u8>,
    buf: Vec<u8>,
}
impl IndexWriter {
    fn create<P: AsRef<Path>>(table_path: P) -> io::Result<IndexWriter> {
        info!("Creating new index {:?}", table_path.as_ref());
        let file = File::create(table_path)?;

        let mut block = Vec::with_capacity(BLOCK_SIZE);
        block.resize(8, 0);

        Ok(IndexWriter {
            file,
            block,
            buf: Vec::new(),
        })
    }

    fn write_keys<'a>(&mut self, keys: impl IntoIterator<Item = (&'a str, u64)>) -> io::Result<()> {
        for (key, offset) in keys.into_iter() {
            write_length_prefixed_string(&mut self.buf, key)?;
            write_integer(&mut self.buf, offset)?;

            if self.buf.len() + self.block.len() > BLOCK_SIZE {
                self.flush()?;

                self.block.resize(8, 0);

                let index_block_offset = self.file.stream_position()? as usize;
                assert_eq!(index_block_offset % BLOCK_SIZE, 0);
            }

            self.block.append(&mut self.buf);
            self.buf.clear();
        }

        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }

        assert_eq!([0u8; 8], &self.block[..8]);
        let content_size = self.block.len() - 8;
        self.block[0..8].clone_from_slice(content_size.to_le_bytes().as_slice());

        self.add_padding();

        self.file.write_all(&self.block)?;
        self.file.flush()?;

        self.block.clear();

        Ok(())
    }

    fn add_padding(&mut self) {
        let padding = calculate_padding(self.block.len() as u64);
        self.block.resize(self.block.len() + padding as usize, 0);

        assert_eq!(self.block.len() % BLOCK_SIZE, 0);
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}

#[derive(Debug)]
struct DataReader {
    file: BufReader<File>,
}

impl DataReader {
    fn open(table_path: impl AsRef<Path>) -> io::Result<DataReader> {
        info!("Opening table {:?}", table_path.as_ref());

        let file = File::open(table_path)?;
        Ok(DataReader {
            file: BufReader::new(file),
        })
    }

    fn find_value(&mut self, key: &str, block_offset: u64) -> Result<Option<Operation>, Error> {
        let data_block_iter = match DataBlockIter::new(&mut self.file, block_offset)? {
            Some(data_block_iter) => data_block_iter,
            None => return Ok(None),
        };

        for next in data_block_iter {
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

    pub fn iter(&mut self) -> Result<DataIter<'_, BufReader<File>>, Error> {
        self.file.rewind()?;

        let data_block_iter = DataBlockIter::new(&mut self.file, 0)?;

        Ok(DataIter { data_block_iter })
    }
}

#[derive(Debug)]
struct IndexReader {
    file: BufReader<File>,
}

impl IndexReader {
    fn open(table_path: impl AsRef<Path>) -> io::Result<IndexReader> {
        info!("Opening table {:?}", table_path.as_ref());

        let file = File::open(table_path)?;
        Ok(IndexReader {
            file: BufReader::new(file),
        })
    }

    fn find_block(&mut self, key: &str) -> Result<Option<u64>, Error> {
        let index_iter = self.iter()?;

        let mut candidate = None;

        for next in index_iter {
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

    fn iter(&mut self) -> io::Result<IndexIter<'_, BufReader<File>>> {
        let index_block_iter = IndexBlockIter::new(&mut self.file, 0)?;
        Ok(IndexIter { index_block_iter })
    }
}

pub struct IndexIter<'a, R> {
    index_block_iter: Option<IndexBlockIter<'a, R>>,
}

impl<'a, R> Iterator for IndexIter<'a, R>
where
    R: BufRead + Seek,
{
    type Item = Result<(String, u64), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let index_block_iter = self.index_block_iter.as_mut()?;

        match index_block_iter.next() {
            Some(next) => Some(next),
            None => {
                let file = self.index_block_iter.take()?.source;
                let stream_pos = match file.stream_position() {
                    Ok(stream_pos) => stream_pos,
                    Err(err) => return Some(Err(err.into())),
                };

                let padding = calculate_padding(stream_pos);
                let next_index_block = stream_pos + padding;

                match file.seek_relative(padding as i64) {
                    Ok(()) => {}
                    Err(err) => return Some(Err(err.into())),
                };

                let mut index_block_iter = match IndexBlockIter::new(file, next_index_block) {
                    Ok(index_block_iter) => index_block_iter?,
                    Err(err) => return Some(Err(err.into())),
                };

                let next = index_block_iter.next();
                self.index_block_iter = Some(index_block_iter);

                next
            }
        }
    }
}

struct IndexBlockIter<'a, R> {
    source: &'a mut R,
    offset: u64,
    content_len: u64,
}

impl<'a, R> IndexBlockIter<'a, R>
where
    R: BufRead + Seek,
{
    fn new(source: &'a mut R, offset: u64) -> io::Result<Option<IndexBlockIter<'a, R>>> {
        assert_eq!(0, offset % BLOCK_SIZE as u64);
        source.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; 8];
        match source.read_exact(&mut buf) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err),
        }
        let content_len = u64::from_le_bytes(buf);

        Ok(Some(IndexBlockIter {
            source,
            offset,
            content_len,
        }))
    }
}

impl<'a, R> Iterator for IndexBlockIter<'a, R>
where
    R: BufRead + Seek,
{
    type Item = Result<(String, u64), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let stream_pos = match self.source.stream_position() {
            Ok(stream_pos) => stream_pos,
            Err(err) => return Some(Err(err.into())),
        };

        if stream_pos >= self.offset + self.content_len + 8 {
            return None;
        }

        let key = match read_length_prefixed_string(&mut self.source) {
            Ok(key) => key,
            Err(err) => return Some(Err(err)),
        };

        let offset = match read_integer(&mut self.source) {
            Ok(offset) => offset,
            Err(err) => return Some(Err(err)),
        };

        Some(Ok((key, offset)))
    }
}

pub struct DataIter<'a, R> {
    data_block_iter: Option<DataBlockIter<'a, R>>,
}

impl<'a, R> Iterator for DataIter<'a, R>
where
    R: BufRead + Seek,
{
    type Item = Result<Operation, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let data_block_iter = self.data_block_iter.as_mut()?;

        match data_block_iter.next() {
            Some(next) => Some(next),
            None => {
                let file = self.data_block_iter.take()?.source;
                let stream_pos = match file.stream_position() {
                    Ok(stream_pos) => stream_pos,
                    Err(err) => return Some(Err(err.into())),
                };

                let padding = calculate_padding(stream_pos);
                let next_data_block = stream_pos + padding;

                let mut data_block_iter = match DataBlockIter::new(file, next_data_block) {
                    Ok(data_block_iter) => data_block_iter?,
                    Err(err) => return Some(Err(err)),
                };

                let next = data_block_iter.next();
                self.data_block_iter = Some(data_block_iter);

                next
            }
        }
    }
}

struct DataBlockIter<'a, R> {
    source: &'a mut R,
    offset: u64,
    content_len: u64,
}

impl<'a, R> DataBlockIter<'a, R>
where
    R: BufRead + Seek,
{
    fn new(source: &'a mut R, offset: u64) -> Result<Option<DataBlockIter<'a, R>>, Error> {
        assert_eq!(0, offset % BLOCK_SIZE as u64);
        source.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; 8];
        match source.read_exact(&mut buf) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err.into()),
        }
        let content_len = u64::from_le_bytes(buf);

        source.read_exact(&mut buf)?;
        let want_checksum = u64::from_le_bytes(buf);

        let mut buf = vec![0u8; content_len as usize];
        source.read_exact(&mut buf)?;

        let got_checksum = crc64::crc64(0, &buf);
        if want_checksum != got_checksum {
            return Err(Error::ChecksumMismatch {
                want: want_checksum,
                got: got_checksum,
            });
        }

        source.seek(SeekFrom::Start(offset + 16))?;

        Ok(Some(DataBlockIter {
            source,
            offset,
            content_len,
        }))
    }
}

impl<'a, R> Iterator for DataBlockIter<'a, R>
where
    R: BufRead + Seek,
{
    type Item = Result<Operation, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let stream_pos = match self.source.stream_position() {
            Ok(stream_pos) => stream_pos,
            Err(err) => return Some(Err(err.into())),
        };

        if stream_pos >= self.offset + self.content_len + 16 {
            return None;
        }

        Some(read_operation(&mut self.source))
    }
}

fn calculate_padding(stream_pos: u64) -> u64 {
    if stream_pos.is_multiple_of(BLOCK_SIZE as u64) {
        0
    } else {
        BLOCK_SIZE as u64 - (stream_pos % BLOCK_SIZE as u64)
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
        let index_path = temp_dir.path().join("index.db");
        let table_path = temp_dir.path().join("data.db");
        let mut writer = SSTableWriter::create(table_path.clone(), index_path.clone())?;
        let mut table = SSTableReader::open(index_path, table_path)?;

        let mut operations = Vec::new();
        for i in 0..1000 {
            operations.push(Operation::Insert(i.to_string(), (i * 10).to_string()))
        }
        operations.sort_by(|a, b| a.key().cmp(b.key()));

        writer.write(&operations)?;

        writer.sync()?;

        for operation in operations {
            let key = operation.key().to_string();
            assert_eq!(Some(operation), table.find_value(&key)?);
        }

        Ok(())
    }
}
