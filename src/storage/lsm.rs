use crate::storage::heap::MinHeap;
use log::info;
use std::array::TryFromSliceError;
use std::borrow::Cow;
use std::cmp::{Ordering, min};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Display};
use std::fs::{File, OpenOptions};
use std::io::{
    BufRead, BufReader, BufWriter, ErrorKind, IntoInnerError, Read, Seek, SeekFrom, Write,
};
use std::num::ParseIntError;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::time::{Duration, Instant};
use std::{error, fmt, fs, io, mem};

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
            Error::NoManifestEntryForLevel(level) => {
                write!(f, "No manifest exists for level {level}")
            }
            Error::InvalidTableName(value) => write!(f, "Invalid table name {value}"),
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

pub struct Storage {
    mem_table: MemTable,
    levels: BTreeMap<usize, Vec<(RangeInclusive<String>, SSTableReader)>>,

    write_ahead_log: WriteAheadLogWriter,
    locations: Locations,
    max_table_size: usize,
}

impl Storage {
    pub fn new(directory_path: PathBuf, max_table_size: usize) -> Result<Storage, Error> {
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

        let mut storage = Storage {
            mem_table: MemTable::default(),
            levels,
            write_ahead_log: wal_writer,
            locations,
            max_table_size,
        };

        let wal_reader = match WriteAheadLogReader::open(&storage.locations.wal) {
            Ok(wal_reader) => Some(wal_reader),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(Error::from(err)),
        };

        info!("Replaying operations from write ahead log file");

        if let Some(wal_reader) = wal_reader {
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
            for (key_range, table) in tables.iter_mut().rev() {
                if key_range.start().as_str() <= key && key <= key_range.end().as_str() {
                    let value = table.find(key)?;
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
                if tables_len > (level + 1) * 5 {
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

        let table_id = self.next_id();
        let table_name = format!("TABLE_{}", table_id);
        let table_path = self.locations.table_path(&table_name);
        let mut table_writer = SSTableWriter::open(&table_path)?;
        let mem_table = mem::take(&mut self.mem_table);
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
        let table_range = min..=max;
        table_writer.write(mem_table)?;
        self.sync_dir()?;

        let table_reader = SSTableReader::open(table_id, table_path)?;

        let new_level_zero = match self.levels.get_mut(&0) {
            Some(level_zero) => level_zero,
            None => {
                self.levels.insert(0, Vec::new());
                self.levels.get_mut(&0).expect(
                    "get_mut(0) should return Some when insert(0, value) has been called before",
                )
            }
        };

        new_level_zero.push((table_range, table_reader));

        self.save_manifest()?;

        self.write_ahead_log.truncate()?;

        info!("Flush complete took {:?}", start.elapsed());

        Ok(())
    }

    fn compact_level(&mut self, level: usize) -> Result<(), Error> {
        info!("Compacting level {level}");
        let start = Instant::now();

        let mut table_id = self.next_id();

        let mut old_tables = Vec::new();
        if let Some(old_level) = self.levels.get_mut(&level) {
            old_tables.extend(old_level.drain(..));
        }
        if let Some(old_level) = self.levels.get_mut(&(level + 1)) {
            old_tables.extend(old_level.drain(..));
        }

        let mut old_tables = old_tables
            .iter_mut()
            .map(|(_, table)| (table.id, (level, table)))
            .collect::<HashMap<_, _>>();

        let mut min_heap = MinHeap::default();
        for (_, (level, table)) in old_tables.iter_mut() {
            table.file.seek(SeekFrom::Start(0))?;
            if let Some(entry) = table.next() {
                let entry = entry?;
                min_heap.insert(
                    MinHeapKey::new(entry.key().to_owned(), table.id, *level),
                    entry,
                );
            }
        }

        let mut table_name = format!("TABLE_{}", table_id);
        let mut table_path = self.locations.table_path(&table_name);
        let mut table_writer = SSTableWriter::open(&table_path)?;
        let mut table_entries = 0;
        let mut new_next_level = vec![];
        let mut key_range: Option<RangeInclusive<String>> = None;

        while let Some((heap_key, operation)) = min_heap.extract() {
            if heap_key.key == "bhkja"{
                dbg!();
            }
            match &operation {
                Operation::Insert(_, value) => {
                    table_writer.insert(heap_key.key.clone(), value.to_owned())?;
                    table_entries += 1;
                    key_range = Some(match key_range {
                        Some(key_range) => grow_range(&heap_key.key, key_range),
                        None => heap_key.key.clone()..=heap_key.key.clone(),
                    });
                }
                Operation::Delete(_) if level < self.levels.len() - 1 => {
                    table_writer.delete(heap_key.key.clone())?;
                    table_entries += 1;
                    key_range = Some(match key_range {
                        Some(key_range) => grow_range(&heap_key.key, key_range),
                        None => heap_key.key.clone()..=heap_key.key.clone(),
                    });
                }
                Operation::Delete(_) => {
                    // Tombstones at the lowest level can be dropped, since there are no higher levels
                    // the tombstone does not shadow other operations for that key
                }
            }

            if table_entries > self.max_table_size {
                table_writer.sync()?;

                new_next_level.push((
                    key_range.unwrap(),
                    SSTableReader::open(table_id, table_path)?,
                ));
                key_range = None;

                table_id += 1;
                table_name = format!("TABLE_{}", table_id);
                table_path = self.locations.table_path(&table_name);
                table_writer = SSTableWriter::open(&table_path)?;
                table_entries = 0;
            }

            let mut extracted = min_heap.extract_until(|k, _| k.key == heap_key.key);
            extracted.insert(0, (heap_key, operation));
            for (key, _) in extracted {
                let (level, table) = old_tables
                    .get_mut(&key.table_id)
                    .expect("Each entry of the min-heap should come from an existing SSTable");
                if let Some(entry) = table.next() {
                    let entry = entry?;
                    min_heap.insert(
                        MinHeapKey::new(entry.key().to_owned(), table.id, *level),
                        entry,
                    );
                }
            }
        }

        new_next_level.push((
            key_range.unwrap(),
            SSTableReader::open(table_id, table_path)?,
        ));

        table_writer.sync()?;

        let next_level = if let Some(next_level) = self.levels.get_mut(&(level + 1)) {
            next_level
        } else {
            self.levels.insert(level + 1, Vec::new());
            self.levels.get_mut(&(level + 1)).expect("get_mut(level) should return Some when insert(level, value) has been called before")
        };

        next_level.extend(new_next_level);


        self.sync_dir()?;
        self.save_manifest()?;

        for (_, (_, table)) in old_tables {
            info!("Deleting old table {}", table.file_name);
            fs::remove_file(self.locations.table_path(&table.file_name))?;
        }

        info!("Compaction complete, took {:?}", start.elapsed());

        Ok(())
    }

    fn next_id(&self) -> u64 {
        self.levels
            .values()
            .flatten()
            .map(|(_, table)| table.id)
            .max()
            .map_or(1, |id| id + 1)
    }

    fn save_manifest(&mut self) -> Result<(), Error> {
        info!("Saving manifest entries");

        let mut manifest_writer = ManifestWriter::open(&self.locations.manifest_temp)?;

        for (level, tables) in self.levels.iter() {
            let tables = tables
                .iter()
                .map(|(range, table)| (range.clone(), table.file_name.clone()));
            manifest_writer.write_level(*level, tables)?;
        }

        manifest_writer.sync()?;
        fs::rename(&self.locations.manifest_temp, &self.locations.manifest)?;
        File::open(&self.locations.manifest)?.sync_all()?;
        self.sync_dir()?;

        Ok(())
    }
}

#[derive(PartialEq)]
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

#[derive(Default)]
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

struct SSTableWriter {
    file: BufWriter<File>,
}
impl SSTableWriter {
    fn open(table_path: &Path) -> io::Result<SSTableWriter> {
        info!("Creating new table {:?}", table_path);
        let file = File::create(&table_path)?;
        Ok(SSTableWriter {
            file: BufWriter::new(file),
        })
    }

    fn write(&mut self, mem_table: MemTable) -> io::Result<()> {
        for (key, value) in mem_table.inner {
            match value {
                MemTableValue::Value(value) => {
                    write_operation(&mut self.file, &Operation::Insert(key, value))?;
                }
                MemTableValue::Deleted => {
                    write_operation(&mut self.file, &Operation::Delete(key))?;
                }
            }
        }

        self.sync()
    }

    fn insert(&mut self, key: String, value: String) -> io::Result<()> {
        write_operation(&mut self.file, &Operation::Insert(key, value))
    }

    fn delete(&mut self, key: String) -> io::Result<()> {
        write_operation(&mut self.file, &Operation::Delete(key))
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        Ok(())
    }
}

struct SSTableReader {
    id: u64,
    file_name: String,
    file: BufReader<File>,
}

impl SSTableReader {
    fn open(id: u64, table_path: impl AsRef<Path>) -> io::Result<SSTableReader> {
        let file_name = table_path
            .as_ref()
            .file_name()
            .ok_or_else(|| {
                io::Error::new(
                    ErrorKind::IsADirectory,
                    format!("{:?} is a directory", table_path.as_ref()),
                )
            })?
            .to_os_string()
            .to_string_lossy()
            .into_owned();

        info!("Opening table {:?}", table_path.as_ref());

        let file = File::open(table_path)?;
        Ok(SSTableReader {
            id,
            file_name,
            file: BufReader::new(file),
        })
    }

    fn find(&mut self, key: &str) -> Result<Option<Operation>, Error> {
        self.file.seek(SeekFrom::Start(0))?;

        while let Some(entry) = self.next() {
            let entry = entry?;
            let found = match &entry {
                Operation::Insert(got_key, _) if key == got_key => true,
                Operation::Delete(got_key) if key == got_key => true,
                _ => false,
            };

            if found {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }
}

impl<'a> Iterator for SSTableReader {
    type Item = Result<Operation, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match read_operation(&mut self.file) {
            Ok(None) => None,
            Ok(Some(operation)) => Some(Ok(operation)),
            Err(err) => Some(Err(err)),
        }
    }
}

fn read_manifest(
    manifest_path: impl AsRef<Path>,
) -> Result<BTreeMap<usize, Vec<(RangeInclusive<String>, SSTableReader)>>, Error> {
    let mut manifest_reader = ManifestReader::open(&manifest_path)?;

    let levels_table_names = manifest_reader.read_all()?;
    let mut levels = BTreeMap::new();
    for (level, table_names) in levels_table_names {
        let mut tables = Vec::new();
        for (range, table_id, table_name) in table_names {
            let table_path = match manifest_path.as_ref().parent() {
                Some(path) => path.join(table_name),
                None => PathBuf::from(table_name),
            };

            tables.push((range, SSTableReader::open(table_id, &table_path)?))
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

struct ManifestWriter {
    file: BufWriter<File>,
}
impl ManifestWriter {
    fn open(path: impl AsRef<Path>) -> io::Result<ManifestWriter> {
        info!("Creating manifest {:?}", path.as_ref());
        let file = File::create(path)?;
        Ok(ManifestWriter {
            file: BufWriter::new(file),
        })
    }

    fn write_level(
        &mut self,
        level: usize,
        tables: impl IntoIterator<Item = (RangeInclusive<String>, String)>,
    ) -> io::Result<()> {
        writeln!(self.file, "[L{}]", level)?;
        for (key_range, table_name) in tables {
            writeln!(
                self.file,
                "{}-{}:{table_name}",
                key_range.start(),
                key_range.end()
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
        match read_operation(&mut self.file) {
            Ok(None) => None,
            Ok(Some(operation)) => Some(Ok(operation)),
            Err(err) => Some(Err(err)),
        }
    }
}

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

fn read_string(source: &mut impl BufRead) -> Result<Option<String>, Error> {
    let mut buf = Vec::new();
    source.read_until(b':', &mut buf)?;
    match buf.pop() {
        Some(b':') => {}
        Some(_) | None => return Err(Error::Truncated),
    }
    let len_str = String::from_utf8_lossy(&buf);
    let len = len_str.parse::<usize>()?;
    buf.resize(len, 0);
    source.read_exact(&mut buf)?;
    let string = String::from_utf8(buf)?;

    Ok(Some(string))
}

fn write_string(destination: &mut impl Write, value: &str) -> io::Result<()> {
    write!(destination, "{}:{}", value.len(), value)?;
    Ok(())
}

fn read_operation_code(source: &mut impl Read) -> Result<Option<OperationCode>, Error> {
    let mut op_code_bytes = [0u8; 1];
    let n = source.read(&mut op_code_bytes)?;
    if n == 0 {
        return Ok(None);
    }

    Ok(Some(OperationCode::try_from(op_code_bytes[0])?))
}

fn read_operation(source: &mut impl BufRead) -> Result<Option<Operation>, Error> {
    let op_code = match read_operation_code(source)? {
        Some(op_code) => op_code,
        None => return Ok(None),
    };

    match op_code {
        OperationCode::Insert => {
            let key = read_string(source)?.ok_or_else(|| Error::Truncated)?;
            let value = read_string(source)?.ok_or_else(|| Error::Truncated)?;

            Ok(Some(Operation::Insert(key, value)))
        }
        OperationCode::Delete => {
            let key = read_string(source)?.ok_or_else(|| Error::Truncated)?;

            Ok(Some(Operation::Delete(key)))
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
            write_string(destination, key)?;
            write_string(destination, value)?;
        }
        Operation::Delete(key) => {
            write_string(destination, key)?;
        }
    }
    Ok(())
}

fn grow_range(key: &str, range: RangeInclusive<String>) -> RangeInclusive<String> {
    let (mut start, mut end) = range.into_inner();
    if key < start.as_str() {
        start = key.to_owned();
        return start..=end;
    }

    if key > end.as_str() {
        end = key.to_owned();
    }
    start..=end
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logger;
    use crate::temp_dir::TempDir;
    use log::Level;
    use std::error;
    use std::sync::Once;

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
        let temp_dir = TempDir::new()?;
        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 200)?;
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
        let temp_dir = TempDir::new()?;
        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 200)?;
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
        let temp_dir = TempDir::new()?;
        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 200)?;
        let lines = PUT_DELETE_FILE.lines();
        for (i, line) in lines.enumerate() {
            if i % 800 == 0 {
                if i == 16000 {
                    dbg!();
                }
                storage = Storage::new(temp_dir.path().to_path_buf(), 200)?;
            }

            let cmd = Cmd::try_from(line)?;
            match cmd {
                Cmd::Get { key, want } => {
                    if key == "bhkja" {
                        dbg!();
                    }
                    let got = storage.get(key)?;
                    assert_eq!(want, got.as_deref(), "line {} {:?}", i, cmd);
                }
                Cmd::Put { key, value } => {
                    if key == "bhkja" {
                        dbg!();
                    }
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
        let temp_dir = TempDir::new()?;
        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 10)?;

        storage.insert("one".to_string(), "value one".to_string())?;
        storage.insert("two".to_string(), "value two".to_string())?;
        storage.delete("two".to_string())?;

        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 10)?;

        assert_eq!(Some("value one".to_string()), storage.get("one")?);
        assert_eq!(None, storage.get("two")?);

        Ok(())
    }

    #[test]
    fn compaction() -> Result<(), Box<dyn error::Error>> {
        initialize();
        let temp_dir = TempDir::new()?;
        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 2)?;

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
