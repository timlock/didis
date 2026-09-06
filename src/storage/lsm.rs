use crate::storage::heap::MinHeap;
use log::info;
use std::array::TryFromSliceError;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Display};
use std::fs::{File, OpenOptions};
use std::io::{
    BufRead, BufReader, BufWriter, Cursor, ErrorKind, IntoInnerError, Read, Seek, SeekFrom, Write,
};
use std::num::ParseIntError;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::time::Instant;
use std::{error, fmt, fs, io, mem, usize};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    FromUtf8(FromUtf8Error),
    Utf8(Utf8Error),
    TryFromSliceError(TryFromSliceError),
    ParseIntError(ParseIntError),
    UnknownOperation(u8),
    Truncated,
    InvalidTableName(String),
    InvalidManifestEntry(String),
    NoManifestEntryForLevel(usize),
    IntoInner(IntoInnerError<BufWriter<File>>),
    UnexpectedCharacter { want: char, got: char },
    UnexpectedByte { want: u8, got: u8 },
    ChecksumMismatch { want: u64, got: u64 },
}

impl error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::FromUtf8(e) => write!(f, "{}", e),
            Error::Utf8(e) => write!(f, "{}", e),
            Error::Io(error) => write!(f, "IO error: {error}"),
            Error::TryFromSliceError(e) => write!(f, "{}", e),
            Error::ParseIntError(e) => write!(f, "{}", e),
            Error::UnknownOperation(op_code) => write!(f, "Unknown op code {}", op_code),
            Error::Truncated => write!(f, "Truncated"),
            Error::InvalidManifestEntry(name) => {
                write!(f, "Manifest contains invalid entry {}", name)
            }
            Error::IntoInner(e) => write!(f, "{}", e),
            Error::UnexpectedCharacter { want, got } => {
                write!(f, "Expected character '{want}' got '{got}'")
            }
            Error::UnexpectedByte { want, got } => {
                write!(f, "Expected byte '{want}' got '{got}'")
            }
            Error::NoManifestEntryForLevel(level) => {
                write!(f, "No manifest exists for level {level}")
            }
            Error::InvalidTableName(value) => write!(f, "Invalid table name {value}"),
            Error::ChecksumMismatch { want, got } => {
                write!(f, "Checksum mismatch want {} got {}", want, got)
            }
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(value: FromUtf8Error) -> Self {
        Error::FromUtf8(value)
    }
}

impl From<Utf8Error> for Error {
    fn from(value: Utf8Error) -> Self {
        Error::Utf8(value)
    }
}

impl From<TryFromSliceError> for Error {
    fn from(value: TryFromSliceError) -> Self {
        Error::TryFromSliceError(value)
    }
}
impl From<ParseIntError> for Error {
    fn from(value: ParseIntError) -> Self {
        Error::ParseIntError(value)
    }
}

impl From<IntoInnerError<BufWriter<File>>> for Error {
    fn from(value: IntoInnerError<BufWriter<File>>) -> Self {
        Error::IntoInner(value)
    }
}

#[derive(Debug)]
struct Locations {
    directory: PathBuf,
    manifest: PathBuf,
    manifest_temp: PathBuf,
    wal: PathBuf,
}

impl Locations {
    fn table_path(&self, table_name: impl AsRef<Path>) -> PathBuf {
        self.directory.join(table_name)
    }
}

#[derive(Debug)]
struct IdGenerator {
    next_id: u64,
}

impl IdGenerator {
    fn next(&mut self) -> u64 {
        let next_id = self.next_id;
        self.next_id += 1;

        next_id
    }
}

#[derive(Debug)]
pub struct Storage {
    mem_table: MemTable,
    levels: BTreeMap<usize, Vec<SSTable>>,
    id_generator: IdGenerator,

    write_ahead_log: WriteAheadLogWriter,
    locations: Locations,
    max_table_size: usize,
    level_ratio: usize,
}

impl Storage {
    pub fn new(
        directory_path: PathBuf,
        max_table_size: usize,
        level_ratio: usize,
    ) -> Result<Storage, Error> {
        info!("Setting up storage engine");

        let locations = Locations {
            manifest: directory_path.join("MANIFEST"),
            manifest_temp: directory_path.join("MANIFEST.tmp"),
            wal: directory_path.join("write_ahead_log"),
            directory: directory_path,
        };
        info!("Reading manifest file");
        let levels = match read_manifest(&locations.manifest) {
            Ok(levels) => {
                info!(
                    "Found {} tables",
                    levels.values().map(Vec::len).sum::<usize>()
                );
                levels
            }
            Err(Error::Io(err)) if err.kind() == ErrorKind::NotFound => {
                File::create(&locations.manifest)?;
                BTreeMap::new()
            }
            Err(err) => return Err(err),
        };

        //TODO look for dangling table files

        let wal_writer = WriteAheadLogWriter::open(&locations.wal)?;

        let next_id = levels
            .values()
            .flatten()
            .map(|table| table.id)
            .max()
            .map_or(1, |id| id + 1);
        let id_generator = IdGenerator { next_id };

        let mut storage = Storage {
            mem_table: MemTable::default(),
            levels,
            write_ahead_log: wal_writer,
            locations,
            max_table_size,
            level_ratio,
            id_generator,
        };

        let wal_reader = match WriteAheadLogReader::open(&storage.locations.wal) {
            Ok(wal_reader) => Some(wal_reader),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(Error::from(err)),
        };

        if let Some(wal_reader) = wal_reader {
            info!("Replaying operations from write ahead log file");

            for operation in wal_reader {
                let operation = operation?;
                storage.execute_no_wal(operation)?;
            }
        }

        info!("Storage engine is set up");

        Ok(storage)
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>, Error> {
        if let Some(value) = self.mem_table.inner.get(key) {
            return match value {
                MemTableValue::Value(value) => Ok(Some(value.clone())),
                MemTableValue::Deleted => Ok(None),
            };
        }

        for (level, tables) in self.levels.iter_mut() {
            for table in tables.iter_mut().rev() {
                if table.has_key_in_range(key) {
                    let value = table.reader.find_value(key)?;
                    match value {
                        None => {
                            if *level > 0 {
                                // only level one has tables with overlapping keys, for later levels only one table may contain a certain key
                                break;
                            }
                        }
                        Some(value) => {
                            return match value {
                                Operation::Insert(_, value) => Ok(Some(value.clone())),
                                Operation::Delete(_) => Ok(None),
                            };
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn insert(&mut self, key: String, value: String) -> Result<(), Error> {
        let operation = Operation::Insert(key, value);
        self.write_ahead_log.append(&operation)?;
        self.write_ahead_log.sync_data()?;

        self.execute_no_wal(operation)
    }

    pub fn delete(&mut self, key: String) -> Result<(), Error> {
        let operation = Operation::Delete(key);
        self.write_ahead_log.append(&operation)?;
        self.write_ahead_log.sync_data()?;

        self.execute_no_wal(operation)
    }

    fn execute_no_wal(&mut self, operation: Operation) -> Result<(), Error> {
        match operation {
            Operation::Insert(key, value) => {
                self.mem_table.insert(key, value);
            }
            Operation::Delete(key) => {
                self.mem_table.delete(key.as_str())?;
            }
        }

        if self.mem_table.inner.len() >= self.max_table_size {
            self.flush()?;

            for level in 0..self.levels.len() {
                let tables_len = self.levels.get(&level).map(Vec::len).unwrap_or(0);
                if tables_len > (level + 1) * self.level_ratio {
                    self.compact_level(level)?;
                }
            }
        }

        Ok(())
    }

    fn sync_dir(&self) -> io::Result<()> {
        File::open(self.locations.directory.as_path())?.sync_all()
    }

    fn flush(&mut self) -> Result<(), Error> {
        info!("Flushing memtable");
        let start = Instant::now();

        let mem_table = mem::take(&mut self.mem_table);

        let (table, mut writer) = self.create_table(0)?;

        let min = mem_table
            .inner
            .first_key_value()
            .map(|(key, _)| String::from(key))
            .unwrap_or_else(String::new);
        let max = mem_table
            .inner
            .last_key_value()
            .map(|(key, _)| String::from(key))
            .unwrap_or_else(String::new);
        table.key_range = min..=max;

        let operations = mem_table
            .inner
            .into_iter()
            .map(|(key, operation)| match operation {
                MemTableValue::Value(value) => Operation::Insert(key, value),
                MemTableValue::Deleted => Operation::Delete(key),
            })
            .collect();
        writer.write_data_blocks(operations)?;
        writer.write_indices()?;
        self.sync_dir()?;

        self.save_manifest()?;

        self.write_ahead_log.truncate()?;

        info!("Flush complete took {:?}", start.elapsed());

        Ok(())
    }

    fn compact_level(&mut self, level: usize) -> Result<(), Error> {
        info!("Compacting level {level}");
        let start = Instant::now();

        let mut old_tables = self
            .levels
            .remove(&level)
            .unwrap_or_default()
            .into_iter()
            .map(|table| (level, table))
            .collect::<Vec<_>>();

        old_tables.extend(
            self.levels
                .remove(&(level + 1))
                .unwrap_or_default()
                .into_iter()
                .map(|table| (level + 1, table)),
        );

        let mut old_table_iters = HashMap::new();
        for (level, old_table) in &mut old_tables {
            old_table_iters.insert(old_table.id, (*level, old_table.reader.data_iter()?));
        }

        let (max_table_size, highest_level) = (self.max_table_size, self.levels.len());

        let (mut table, mut writer) = self.create_table(level + 1)?;
        let mut entries = 0;
        let mut key_range = String::new()..=String::new();

        let merger = Merger::new(old_table_iters)?;
        for extracted in merger {
            let (heap_key, operation) = extracted?;

            if let Operation::Delete(_) = &operation
                && level == highest_level
            {
                // Tombstones may only be dropped at the highest level, otherwise operations at
                // levels higher than the current one could resurface
                continue;
            }

            writer.write_data_blocks(vec![operation])?;
            entries += 1;
            key_range = grow_range(heap_key.key, key_range);

            if entries > max_table_size {
                writer.write_indices()?;
                writer.sync()?;

                table.key_range = key_range;

                (table, writer) = self.create_table(level + 1)?;
                entries = 0;
                key_range = String::new()..=String::new();
            }
        }

        writer.write_indices()?;
        writer.sync()?;

        self.sync_dir()?;
        self.save_manifest()?;

        for (_, table) in old_tables {
            info!("Deleting old table {:?}", table.path);
            fs::remove_file(table.path)?;
        }

        info!("Compaction complete, took {:?}", start.elapsed());

        Ok(())
    }

    fn save_manifest(&mut self) -> Result<(), Error> {
        info!("Saving manifest entries");

        let mut manifest_writer = ManifestWriter::create(&self.locations.manifest_temp)?;

        for (level, tables) in self.levels.iter() {
            manifest_writer.write_level(*level, tables)?;
        }

        manifest_writer.sync()?;
        fs::rename(&self.locations.manifest_temp, &self.locations.manifest)?;
        File::open(&self.locations.manifest)?.sync_all()?;
        self.sync_dir()?;

        Ok(())
    }

    fn create_table(&mut self, level: usize) -> Result<(&mut SSTable, SSTableWriter), Error> {
        let table_id = self.id_generator.next();
        let path = self.locations.table_path(format!("TABLE_{}", table_id));

        let writer = SSTableWriter::create(&path)?;

        let reader = SSTableReader::open(&path)?;

        let table = SSTable {
            id: table_id,
            name: format!("TABLE_{}", table_id),
            path,
            key_range: String::new()..=String::new(),
            reader,
        };

        let level_tables = self.levels.entry(level).or_default();
        level_tables.push(table);

        let table = level_tables
            .last_mut()
            .expect("Table should be present after it has been pushed to the level vec");
        Ok((table, writer))
    }
}

struct Merger<'a> {
    sources: HashMap<u64, (usize, SSTableDataIter<'a>)>,
    min_heap: MinHeap<MinHeapKey, Operation>,
}

impl<'a> Merger<'a> {
    fn new(mut sources: HashMap<u64, (usize, SSTableDataIter<'a>)>) -> Result<Merger<'a>, Error> {
        let mut min_heap = MinHeap::default();

        for (table_id, (level, table)) in sources.iter_mut() {
            let next = table.next();
            if let Some(entry) = next {
                let entry = entry?;
                min_heap.insert(
                    MinHeapKey::new(entry.key().to_owned(), *table_id, *level),
                    entry,
                );
            }
        }

        Ok(Merger { sources, min_heap })
    }
}

impl<'a> Iterator for Merger<'a> {
    type Item = Result<(MinHeapKey, Operation), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let (heap_key, operation) = match self.min_heap.extract() {
            Some(next) => next,
            None => return None,
        };

        let mut extracted = self.min_heap.extract_until(|k, _| k.key == heap_key.key);
        extracted.insert(0, heap_key.clone());

        for key in extracted {
            let (level, table) = self
                .sources
                .get_mut(&key.table_id)
                .expect("Each entry of the min-heap should come from an existing SSTable");

            if let Some(entry) = table.next() {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(err) => return Some(Err(err)),
                };

                self.min_heap.insert(
                    MinHeapKey::new(entry.key().to_owned(), key.table_id, *level),
                    entry,
                );
            }
        }

        Some(Ok((heap_key, operation)))
    }
}

#[derive(PartialEq, Debug, Clone)]
struct MinHeapKey {
    key: String,
    table_id: u64,
    table_level: usize,
}

impl MinHeapKey {
    fn new(key: String, table_id: u64, table_level: usize) -> MinHeapKey {
        MinHeapKey {
            key,
            table_id,
            table_level,
        }
    }
}

impl PartialOrd for MinHeapKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.key.partial_cmp(&other.key)? {
            // when a min-heap has multiple entries for the same key, the entry of the newest table should be extracted
            Ordering::Equal => match self.table_level.partial_cmp(&other.table_level)? {
                Ordering::Equal => Some(self.table_id.cmp(&other.table_id).reverse()),
                other => Some(other),
            },
            other => Some(other),
        }
    }
}

#[derive(Default, Debug)]
struct MemTable {
    inner: BTreeMap<String, MemTableValue>,
}

impl MemTable {
    fn insert(&mut self, key: String, value: String) -> Option<MemTableValue> {
        self.inner.insert(key, MemTableValue::new(value))
    }

    fn delete(&mut self, key: &str) -> Result<(), Error> {
        if let Some(value) = self.inner.get_mut(key) {
            *value = MemTableValue::Deleted;
            return Ok(());
        }

        self.inner.insert(key.to_string(), MemTableValue::Deleted);

        Ok(())
    }
}

#[derive(Clone, PartialOrd, PartialEq, Debug)]
enum MemTableValue {
    Value(String),
    Deleted,
}

impl MemTableValue {
    fn new(value: String) -> MemTableValue {
        MemTableValue::Value(value)
    }
}

#[derive(Debug)]
struct SSTable {
    id: u64,
    name: String,
    path: PathBuf,
    key_range: RangeInclusive<String>,
    reader: SSTableReader,
}

impl SSTable {
    fn has_key_in_range(&self, key: &str) -> bool {
        self.key_range.start().as_str() <= key && key <= self.key_range.end().as_str()
    }
}

const BLOCK_SIZE: usize = 4096;

#[derive(Debug)]
struct SSTableWriter {
    file: File,
    block: Vec<u8>,
    keys: Vec<(String, usize)>,
}
impl SSTableWriter {
    fn create(table_path: &Path) -> io::Result<SSTableWriter> {
        info!("Creating new table {:?}", table_path);
        let file = File::create(&table_path)?;
        Ok(SSTableWriter {
            file,
            block: Vec::with_capacity(BLOCK_SIZE),
            keys: Vec::new(),
        })
    }

    fn write_data_blocks(&mut self, operations: Vec<Operation>) -> io::Result<()> {
        if self.keys.is_empty() {
            assert!(self.block.is_empty());

            let first_operation = operations
                .first()
                .expect("write_operations should not be called with an empty slice");
            self.keys.push((first_operation.key().to_owned(), 0));
        }

        assert!(self.block.is_empty());
        self.block.resize(16, 0);

        let mut buf = Vec::new();
        for operation in operations {
            write_operation(&mut buf, &operation)?;

            if buf.len() + self.block.len() > BLOCK_SIZE && !self.block.is_empty() {
                self.flush_data_block()?;
                self.block.resize(16, 0);

                let data_block_offset = self.file.stream_position()? as usize;
                assert_eq!(data_block_offset % BLOCK_SIZE, 0);
                self.keys
                    .push((operation.key().to_owned(), data_block_offset));
            }

            self.block.extend(&buf);
            buf.clear();
        }
        self.flush_data_block()?;

        Ok(())
    }

    fn write_indices(&mut self) -> io::Result<()> {
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

        self.add_padding()?;

        let checksum = crc64::crc64(0, &self.block[16..]);
        self.block[8..16].clone_from_slice(checksum.to_le_bytes().as_slice());

        assert_eq!(self.block.len(), BLOCK_SIZE);

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

        assert_eq!(self.block.len(), BLOCK_SIZE);

        self.file.write_all(&self.block)?;
        self.block.clear();

        Ok(())
    }

    fn add_padding(&mut self) -> io::Result<()> {
        let padding = BLOCK_SIZE - (self.block.len() % BLOCK_SIZE);
        self.block.resize(self.block.len() + padding, 0);

        assert_eq!(self.block.len() % BLOCK_SIZE, 0);

        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}

#[derive(Debug)]
struct SSTableReader {
    file: BufReader<File>,
}

impl SSTableReader {
    fn open(table_path: impl AsRef<Path>) -> io::Result<SSTableReader> {
        info!("Opening table {:?}", table_path.as_ref());

        let file = File::open(table_path)?;
        Ok(SSTableReader {
            file: BufReader::new(file),
        })
    }

    fn find_value(&mut self, key: &str) -> Result<Option<Operation>, Error> {
        let offset = match self.find_block(key)? {
            Some(offset) => offset,
            None => return Ok(None),
        };

        self.file.seek(SeekFrom::Start(offset as u64))?;
        let mut data_block_iter = DataBlockIter::new(&mut self.file)?;

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

    fn find_block(&mut self, key: &str) -> Result<Option<usize>, Error> {
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

    fn first_index_offset(&mut self) -> io::Result<usize> {
        self.file.seek(SeekFrom::End(-8))?;

        let mut offset_bytes = [0; 8];
        self.file.read_exact(&mut offset_bytes)?;

        Ok(usize::from_le_bytes(offset_bytes))
    }

    fn index_iter(&mut self) -> io::Result<SSTableIndexIter> {
        let first_index_offset = self.first_index_offset()?;
        let header_offset = self.file.stream_position()? - 8;

        self.file.seek(SeekFrom::Start(first_index_offset as u64))?;

        let index_block_iter = IndexBlockIter::new(&mut self.file)?;

        Ok(SSTableIndexIter {
            index_block_iter: Some(index_block_iter),
            header_offset,
        })
    }

    fn data_iter(&mut self) -> Result<SSTableDataIter, Error> {
        let first_index_offset = self.first_index_offset()?;
        self.file.seek(SeekFrom::Start(0))?;

        let data_block_iter = DataBlockIter::new(&mut self.file)?;

        Ok(SSTableDataIter {
            data_block_iter: Some(data_block_iter),
            first_index_offset: first_index_offset as u64,
        })
    }
}

struct IndexBlockIter<'a> {
    file: &'a mut BufReader<File>,
    end_of_content: u64,
}

impl<'a> IndexBlockIter<'a> {
    fn new(file: &'a mut BufReader<File>) -> io::Result<IndexBlockIter<'a>> {
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let content_len = u64::from_le_bytes(buf);

        let stream_pos = file.stream_position()?;

        Ok(IndexBlockIter {
            file,
            end_of_content: stream_pos + content_len,
        })
    }
}

impl<'a> Iterator for IndexBlockIter<'a> {
    type Item = Result<(String, usize), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let stream_pos = match self.file.stream_position() {
            Ok(stream_pos) => stream_pos,
            Err(err) => return Some(Err(err.into())),
        };

        if stream_pos >= self.end_of_content {
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

struct SSTableIndexIter<'a> {
    index_block_iter: Option<IndexBlockIter<'a>>,
    header_offset: u64,
}
impl<'a> Iterator for SSTableIndexIter<'a> {
    type Item = Result<(String, usize), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let index_block_iter = self.index_block_iter.as_mut()?;

        let next = match index_block_iter.next() {
            Some(next) => Some(next),
            None => {
                let file = self.index_block_iter.take()?.file;
                let stream_pos = match file.stream_position() {
                    Ok(stream_pos) => stream_pos,
                    Err(err) => return Some(Err(err.into())),
                };

                let padding = BLOCK_SIZE - (stream_pos as usize % BLOCK_SIZE);
                if stream_pos + padding as u64 >= self.header_offset {
                    return None;
                }

                match file.seek_relative(padding as i64) {
                    Ok(()) => {}
                    Err(err) => return Some(Err(err.into())),
                };

                let mut index_block_iter = match IndexBlockIter::new(file) {
                    Ok(index_block_iter) => index_block_iter,
                    Err(err) => return Some(Err(err.into())),
                };

                let next = index_block_iter.next();
                self.index_block_iter = Some(index_block_iter);

                next
            }
        };

        next
    }
}

struct SSTableDataIter<'a> {
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

                let padding = BLOCK_SIZE - (stream_pos as usize % BLOCK_SIZE);

                if stream_pos + padding as u64 >= self.first_index_offset {
                    return None;
                }

                match file.seek_relative(padding as i64) {
                    Ok(()) => {}
                    Err(err) => return Some(Err(err.into())),
                };

                let mut data_block_iter = match DataBlockIter::new(file) {
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
    content_len: u64,
}

impl<'a> DataBlockIter<'a> {
    fn new(file: &'a mut BufReader<File>) -> Result<DataBlockIter<'a>, Error> {
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let content_len = u64::from_le_bytes(buf);

        file.read_exact(&mut buf)?;
        let want_checksum = u64::from_le_bytes(buf);

        let data_offset = file.stream_position()?;

        let padding = BLOCK_SIZE - (content_len as usize % BLOCK_SIZE) - 16;

        let mut buf = vec![0u8; content_len as usize + padding];
        file.read_exact(&mut buf)?;

        let got_checksum = crc64::crc64(0, &buf);
        if want_checksum != got_checksum {
            return Err(Error::ChecksumMismatch {
                want: want_checksum,
                got: got_checksum,
            });
        }

        file.seek(SeekFrom::Start(data_offset))?;

        Ok(DataBlockIter { file, content_len })
    }
}

impl<'a> Iterator for DataBlockIter<'a> {
    type Item = Result<Operation, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let stream_pos = match self.file.stream_position() {
            Ok(stream_pos) => stream_pos,
            Err(err) => return Some(Err(err.into())),
        };

        if stream_pos >= self.content_len {
            return None;
        }

        Some(read_operation(&mut self.file))
    }
}

fn read_manifest(manifest_path: impl AsRef<Path>) -> Result<BTreeMap<usize, Vec<SSTable>>, Error> {
    let mut manifest_reader = ManifestReader::open(&manifest_path)?;

    let levels_table_names = manifest_reader.read_all()?;
    let mut levels = BTreeMap::new();
    for (level, table_names) in levels_table_names {
        let mut tables = Vec::new();

        for (range, table_id, table_name) in table_names {
            let table_path = match manifest_path.as_ref().parent() {
                Some(path) => path.join(&table_name),
                None => PathBuf::from(&table_name),
            };

            let reader = SSTableReader::open(&table_path)?;
            tables.push(SSTable {
                id: table_id,
                name: table_name,
                path: table_path,
                key_range: range,
                reader,
            });
        }

        levels.insert(level, tables);
    }

    Ok(levels)
}

fn parse_table_id(value: &str) -> Result<u64, Error> {
    value
        .strip_prefix("TABLE_")
        .ok_or_else(|| Error::InvalidTableName(value.to_string()))?
        .parse::<u64>()
        .map_err(|err| err.into())
}

#[derive(Debug)]
struct ManifestReader {
    file: BufReader<File>,
}

impl ManifestReader {
    fn open(path: impl AsRef<Path>) -> io::Result<ManifestReader> {
        let file = File::open(path)?;
        Ok(ManifestReader {
            file: BufReader::new(file),
        })
    }

    fn read_all(
        &mut self,
    ) -> Result<Vec<(usize, Vec<(RangeInclusive<String>, u64, String)>)>, Error> {
        self.file.seek(SeekFrom::Start(0))?;

        let mut result = Vec::new();
        while let Some(level) = self.read_next_level() {
            result.push(level?);
        }

        Ok(result)
    }

    fn read_next_level(
        &mut self,
    ) -> Option<Result<(usize, Vec<(RangeInclusive<String>, u64, String)>), Error>> {
        let level = match self.read_level_header()? {
            Ok(level) => level,
            Err(err) => return Some(Err(err)),
        };

        let lines = match self.read_lines_until_next_level() {
            Ok(lines) => lines,
            Err(err) => return Some(Err(err)),
        };

        let mut result = Vec::new();
        for line in lines {
            let table = match Self::parse_table(&line) {
                Ok(table) => table,
                Err(err) => return Some(Err(err)),
            };
            result.push(table);
        }

        Some(Ok((level, result)))
    }

    fn read_lines_until_next_level(&mut self) -> Result<Vec<String>, Error> {
        let mut buf = Vec::new();
        if self.file.read_until(b'[', &mut buf)? == 0 {
            return Ok(Vec::new());
        }
        buf.pop();
        self.file.seek_relative(-1)?;

        let lines = str::from_utf8(&buf)?
            .lines()
            .into_iter()
            .filter(|line| !line.trim().is_empty())
            .map(|line| line.to_owned())
            .collect();

        Ok(lines)
    }

    fn read_level_header(&mut self) -> Option<Result<usize, Error>> {
        let mut line = String::new();
        while line.trim().is_empty() {
            match self.file.read_line(&mut line) {
                Ok(0) => return None,
                Ok(_) => {}
                Err(err) => return Some(Err(err.into())),
            }
        }

        if !line.starts_with('[') && !line.ends_with(']') {
            return Some(Err(Error::InvalidManifestEntry(line.to_owned())));
        }

        let level_str = &line[2..line.len() - 2];
        Some(level_str.parse::<usize>().map_err(Error::from))
    }

    fn parse_table(line: &str) -> Result<(RangeInclusive<String>, u64, String), Error> {
        let (keys, table_name) = line
            .split_once(":")
            .ok_or_else(|| Error::InvalidManifestEntry(line.to_owned()))?;

        let (start_inclusive, end_exclusive) = keys
            .split_once("-")
            .ok_or_else(|| Error::InvalidManifestEntry(line.to_owned()))?;

        let table_id = parse_table_id(&table_name)?;

        Ok((
            start_inclusive.to_owned()..=end_exclusive.to_owned(),
            table_id,
            table_name.to_owned(),
        ))
    }
}

#[derive(Debug)]
struct ManifestWriter {
    file: BufWriter<File>,
}
impl ManifestWriter {
    fn create(path: impl AsRef<Path>) -> io::Result<ManifestWriter> {
        info!("Creating manifest {:?}", path.as_ref());
        let file = File::create(path)?;
        Ok(ManifestWriter {
            file: BufWriter::new(file),
        })
    }

    fn write_level<'a>(
        &mut self,
        level: usize,
        tables: impl IntoIterator<Item = &'a SSTable>,
    ) -> io::Result<()> {
        writeln!(self.file, "[L{}]", level)?;
        for table in tables {
            writeln!(
                self.file,
                "{}-{}:{}",
                table.key_range.start(),
                table.key_range.end(),
                table.name
            )?;
        }

        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        Ok(())
    }
}

#[derive(Debug)]
enum Operation {
    Insert(String, String),
    Delete(String),
}

impl Operation {
    fn key(&self) -> &str {
        match self {
            Operation::Insert(key, _) => key,
            Operation::Delete(key) => key,
        }
    }
}

impl From<&Operation> for OperationCode {
    fn from(value: &Operation) -> Self {
        match value {
            Operation::Insert(_, _) => OperationCode::Insert,
            Operation::Delete(_) => OperationCode::Delete,
        }
    }
}

#[derive(Debug)]
enum OperationCode {
    Insert,
    Delete,
}

impl From<&OperationCode> for u8 {
    fn from(value: &OperationCode) -> Self {
        match value {
            OperationCode::Insert => 1,
            OperationCode::Delete => 2,
        }
    }
}

impl TryFrom<u8> for OperationCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value.into() {
            '1' => Ok(OperationCode::Insert),
            '2' => Ok(OperationCode::Delete),
            _ => Err(Error::UnknownOperation(value)),
        }
    }
}

#[derive(Debug)]
struct WriteAheadLogReader {
    file: BufReader<File>,
}

impl WriteAheadLogReader {
    fn open(wal_path: &Path) -> io::Result<WriteAheadLogReader> {
        info!("Opening write ahead log");
        let wal_file = File::open(wal_path)?;
        Ok(WriteAheadLogReader {
            file: BufReader::new(wal_file),
        })
    }
}

impl<'a> Iterator for WriteAheadLogReader {
    type Item = Result<Operation, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match peek_byte(&mut self.file) {
            Ok(Some(_)) => {}
            Ok(None) => return None,
            Err(err) => return Some(Err(err.into())),
        }

        Some(read_operation(&mut self.file))
    }
}

#[derive(Debug)]
struct WriteAheadLogWriter {
    file: BufWriter<File>,
}

impl WriteAheadLogWriter {
    fn open(wal_path: &Path) -> io::Result<WriteAheadLogWriter> {
        info!("Opening / Creating write ahead log");
        let wal_file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(wal_path)?;

        Ok(WriteAheadLogWriter {
            file: BufWriter::new(wal_file),
        })
    }

    fn append(&mut self, operation: &Operation) -> io::Result<()> {
        write_operation(&mut self.file, operation)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()
    }

    fn truncate(&mut self) -> io::Result<()> {
        self.file.get_mut().set_len(0)?;
        self.sync_data()?;

        Ok(())
    }
}

fn read_length_prefixed_string<R: BufRead + Seek>(source: &mut R) -> Result<String, Error> {
    expect_byte(source, b'$')?;
    let mut buf = Vec::new();
    source.read_until(b';', &mut buf)?;
    match buf.pop() {
        Some(b';') => {}
        Some(_) | None => return Err(Error::Truncated),
    }

    let bytes_array = buf.as_slice().try_into()?;
    let len = usize::from_le_bytes(bytes_array);
    buf.resize(len, 0);
    source.read_exact(&mut buf)?;
    let string = String::from_utf8(buf)?;

    expect_byte(source, b';')?;

    Ok(string)
}

fn write_length_prefixed_string(destination: &mut impl Write, value: &str) -> io::Result<()> {
    destination.write_all(b"$")?;
    let bytes = value.len().to_le_bytes();
    destination.write_all(&bytes)?;
    destination.write_all(b";")?;

    write!(destination, "{};", value)?;
    Ok(())
}

fn read_operation_code<R: BufRead + Seek>(source: &mut R) -> Result<OperationCode, Error> {
    let mut op_code_bytes = [0u8; 1];
    let n = source.read(&mut op_code_bytes)?;
    if n == 0 {
        return Err(Error::Truncated);
    }

    OperationCode::try_from(op_code_bytes[0])
}

fn write_integer(destination: &mut impl Write, integer: usize) -> io::Result<usize> {
    destination.write_all(b":")?;
    let bytes = integer.to_le_bytes();
    destination.write_all(&bytes)?;
    destination.write_all(b";")?;

    Ok(bytes.len() + 2)
}

fn read_integer(mut source: &mut impl BufRead) -> Result<usize, Error> {
    expect_byte(&mut source, b':')?;

    let mut buf = Vec::new();
    if source.read_until(b';', &mut buf)? == 0 {
        return Err(Error::Truncated);
    };
    match buf.pop() {
        Some(b';') => {}
        Some(_) | None => return Err(Error::Truncated),
    }

    let integer = usize::from_le_bytes(buf.as_slice().try_into()?);

    Ok(integer)
}

fn read_operation<R: BufRead + Seek>(source: &mut R) -> Result<Operation, Error> {
    let op_code = read_operation_code(source)?;

    match op_code {
        OperationCode::Insert => {
            let key = read_length_prefixed_string(source)?;
            let value = read_length_prefixed_string(source)?;

            Ok(Operation::Insert(key, value))
        }
        OperationCode::Delete => {
            let key = read_length_prefixed_string(source)?;

            Ok(Operation::Delete(key))
        }
    }
}

fn write_operation_code(destination: &mut impl Write, operation: OperationCode) -> io::Result<()> {
    let op_code = u8::from(&operation);
    write!(destination, "{}", op_code)
}

fn write_operation(destination: &mut impl Write, operation: &Operation) -> io::Result<()> {
    write_operation_code(destination, OperationCode::from(operation))?;

    match operation {
        Operation::Insert(key, value) => {
            write_length_prefixed_string(destination, key)?;
            write_length_prefixed_string(destination, value)?;
        }
        Operation::Delete(key) => {
            write_length_prefixed_string(destination, key)?;
        }
    }
    Ok(())
}

fn grow_range(key: String, range: RangeInclusive<String>) -> RangeInclusive<String> {
    let (mut start, mut end) = range.into_inner();

    if start == "" {
        start = key.clone();
    }
    if end == "" {
        end = key.clone();
    }

    if key < start {
        start = key;
        return start..=end;
    }

    if key > end {
        end = key;
    }
    start..=end
}

fn peek_byte<R: BufRead + Seek>(source: &mut R) -> io::Result<Option<u8>> {
    let mut buf = [0u8];
    match source.read(&mut buf)? {
        0 => return Ok(None),
        _ => {}
    };

    source.seek_relative(-1)?;

    Ok(Some(buf[0]))
}

fn expect_byte(source: &mut impl BufRead, want: u8) -> Result<(), Error> {
    let mut buf = [0; 1];
    source.read_exact(&mut buf)?;
    if buf[0] != want {
        return Err(Error::UnexpectedByte { want, got: buf[0] });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logger;
    use crate::temp_dir::TempDir;
    use log::Level;
    use std::sync::Once;
    use std::{env, error};

    static PUT_FILE: &'static str = include_str!("testdata/put.txt");
    static PUT_DELETE_FILE: &'static str = include_str!("testdata/put-delete.txt");

    static INIT: Once = Once::new();

    fn initialize() {
        INIT.call_once(|| {
            logger::init(Level::Info).unwrap();
        });
    }

    #[derive(Debug)]
    enum Cmd<'a> {
        Get { key: &'a str, want: Option<&'a str> },
        Put { key: &'a str, value: &'a str },
        Del { key: &'a str },
    }
    impl<'a> TryFrom<&'a str> for Cmd<'a> {
        type Error = Box<dyn error::Error>;

        fn try_from(value: &'a str) -> Result<Self, Self::Error> {
            let mut iter = value.split_ascii_whitespace();
            match iter.next().ok_or_else(|| "empty line")? {
                "GET" => {
                    let key = iter.next().ok_or_else(|| format!("truncated '{value}'"))?;
                    let want = iter.next().ok_or_else(|| format!("truncated '{value}'"))?;
                    let want = match want {
                        "NOT_FOUND" => None,
                        other => Some(other),
                    };

                    Ok(Cmd::Get { key, want })
                }
                "PUT" => {
                    let key = iter.next().ok_or_else(|| format!("truncated '{value}'"))?;
                    let value = iter.next().ok_or_else(|| format!("truncated '{value}'"))?;
                    Ok(Cmd::Put { key, value })
                }
                "DELETE" => {
                    let key = iter.next().ok_or_else(|| format!("truncated '{value}'"))?;
                    Ok(Cmd::Del { key })
                }
                other => Err(format!("unknown cmd {other}").into()),
            }
        }
    }

    #[test]
    fn put() -> Result<(), Box<dyn error::Error>> {
        initialize();
        let temp_dir = TempDir::create(env::current_dir()?.join("temp"))?;
        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 200, 5)?;
        let lines = PUT_FILE.lines();
        for (i, line) in lines.enumerate() {
            let cmd = Cmd::try_from(line)?;
            match cmd {
                Cmd::Get { key, want } => {
                    let got = storage.get(key)?;
                    assert_eq!(want, got.as_deref(), "line {} {:?}", i, cmd);
                }
                Cmd::Put { key, value } => {
                    storage.insert(key.to_owned(), value.to_owned())?;
                }
                Cmd::Del { key } => {
                    storage.delete(key.to_owned())?;
                }
            }
        }

        Ok(())
    }

    #[test]
    fn put_delete() -> Result<(), Box<dyn error::Error>> {
        initialize();
        let temp_dir = TempDir::create(env::current_dir()?.join("temp"))?;
        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 200, 5)?;
        let lines = PUT_DELETE_FILE.lines();
        for (i, line) in lines.enumerate() {
            let cmd = Cmd::try_from(line)?;
            match cmd {
                Cmd::Get { key, want } => {
                    let got = storage.get(key)?;
                    assert_eq!(want, got.as_deref(), "line {} {:?}", i, cmd);
                }
                Cmd::Put { key, value } => {
                    storage.insert(key.to_owned(), value.to_owned())?;
                }
                Cmd::Del { key } => {
                    storage.delete(key.to_owned())?;
                }
            }
        }

        Ok(())
    }

    #[test]
    fn put_delete_with_storage_resets() -> Result<(), Box<dyn error::Error>> {
        initialize();
        let temp_dir = TempDir::create(env::current_dir()?.join("temp"))?;
        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 200, 5)?;
        let lines = PUT_DELETE_FILE.lines();
        for (i, line) in lines.enumerate() {
            if i % 800 == 0 {
                storage = Storage::new(temp_dir.path().to_path_buf(), 200, 5)?;
            }

            let cmd = Cmd::try_from(line)?;
            match cmd {
                Cmd::Get { key, want } => {
                    let got = storage.get(key)?;
                    assert_eq!(want, got.as_deref(), "line {} {:?}", i, cmd);
                }
                Cmd::Put { key, value } => {
                    storage.insert(key.to_owned(), value.to_owned())?;
                }
                Cmd::Del { key } => {
                    storage.delete(key.to_owned())?;
                }
            }
        }

        Ok(())
    }

    #[test]
    fn data_survives_crash_before_flush() -> Result<(), Box<dyn error::Error>> {
        initialize();
        let temp_dir = TempDir::create(env::current_dir()?.join("temp"))?;
        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 10, 5)?;

        storage.insert("one".to_string(), "value one".to_string())?;
        storage.insert("two".to_string(), "value two".to_string())?;
        storage.delete("two".to_string())?;

        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 10, 5)?;

        assert_eq!(Some("value one".to_string()), storage.get("one")?);
        assert_eq!(None, storage.get("two")?);

        Ok(())
    }

    #[test]
    fn compaction() -> Result<(), Box<dyn error::Error>> {
        initialize();
        let temp_dir = TempDir::create(env::current_dir()?.join("temp"))?;
        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 2, 2)?;

        storage.insert("1".to_string(), "one".to_string())?;
        storage.insert("2".to_string(), "two".to_string())?;
        storage.insert("3".to_string(), "three".to_string())?;
        storage.insert("4".to_string(), "four".to_string())?;
        storage.insert("5".to_string(), "five".to_string())?;
        storage.delete("2".to_string())?;
        assert_eq!(None, storage.get("2")?);
        storage.insert("2".to_string(), "two".to_string())?;
        storage.insert("3".to_string(), "updated three".to_string())?;

        assert_eq!(Some("one".to_string()), storage.get("1")?);
        assert_eq!(Some("two".to_string()), storage.get("2")?);
        assert_eq!(Some("updated three".to_string()), storage.get("3")?);
        assert_eq!(Some("four".to_string()), storage.get("4")?);
        assert_eq!(Some("five".to_string()), storage.get("5")?);

        Ok(())
    }
}
