use crate::storage::heap::MinHeap;
use std::array::TryFromSliceError;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, IntoInnerError, Read, Seek, SeekFrom, Write};
use std::num::ParseIntError;
use std::path::{Path, PathBuf};
use std::str::Utf8Error;
use std::string::FromUtf8Error;
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
            Error::InvalidTableName(name) => {
                write!(f, "Manifest contains file with invalid name {}", name)
            }
            Error::IntoInner(e) => write!(f, "{}", e),
            Error::UnexpectedCharacter { want, got } => {
                write!(f, "Expected character '{want}' got '{got}'")
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

pub struct Storage {
    mem_table: MemTable,
    manifest: ManifestReader,
    write_ahead_log: WriteAheadLogWriter,
    flush_threshold: usize,
}

impl Storage {
    pub fn new(directory_path: PathBuf, flush_threshold: usize) -> Result<Storage, Error> {
        let manifest = ManifestReader::open(directory_path.join("manifest"))?;

        let wal_path = directory_path.join("write_ahead_log");

        let wal_writer = WriteAheadLogWriter::open(&wal_path)?;

        let mut storage = Storage {
            flush_threshold,
            mem_table: MemTable::default(),
            manifest,
            write_ahead_log: wal_writer,
        };

        let wal_reader = match WriteAheadLogReader::open(&wal_path) {
            Ok(wal_reader) => Some(wal_reader),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(Error::from(err)),
        };

        if let Some(wal_reader) = wal_reader {
            for operation in wal_reader {
                let operation = operation?;
                storage.execute_no_wal(operation)?;
            }
        }

        storage.write_ahead_log.truncate()?;

        Ok(storage)
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>, Error> {
        if let Some(value) = self.mem_table.inner.get(key) {
            return match value {
                MemTableValue::Value(value) => Ok(Some(value.clone())),
                MemTableValue::Deleted => Ok(None),
            };
        }
        for table in self.manifest.tables.iter_mut() {
            let value = table.find(key)?;
            if let Some(value) = value {
                return match value {
                    MemTableValue::Value(value) => Ok(Some(value.clone())),
                    MemTableValue::Deleted => Ok(None),
                };
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

        if self.mem_table.inner.len() >= self.flush_threshold {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        let table_id = self.manifest.next_id();
        let table_path = self
            .manifest
            .manifest_path
            .join(format!("table_{}", table_id));
        let mut table_writer = SSTableWriter::open(&table_path)?;
        let mem_table = mem::take(&mut self.mem_table);
        table_writer.write(mem_table)?;
        sync_all_dir(self.manifest.manifest_path.as_path())?;

        let table = SSTableReader::open(table_id, &table_path)?;
        self.manifest.insert_atomic(table)?;

        self.write_ahead_log.truncate()?;

        Ok(())
    }

    fn compact(&mut self) -> Result<(), Error> {
        let mut table_id = self.manifest.next_id();
        let table_path = self
            .manifest
            .manifest_path
            .join(format!("table_{}", table_id));
        let mut table_writer = SSTableWriter::open(&table_path)?;
        let mut table_entries = 0u64;
        let mut new_tables = Vec::<SSTableReader>::new();

        let mut min_heap = MinHeap::default();
        loop {
            for table in self.manifest.tables.iter_mut() {
                if let Some((key, value)) = table.next_entry()? {
                    min_heap.insert(
                        SSTableMinHeapKey {
                            key,
                            table_id: table.id,
                        },
                        value,
                    );
                }
            }

            match min_heap.extract() {
                Some((key, value)) => {
                    if let MemTableValue::Value(value) = value {
                        table_writer.insert(&key.key, &value)?;
                        table_entries += 1;
                        if table_entries > 2000 {
                            table_writer.sync()?;

                            let table = SSTableReader::open(table_id, &table_path)?;
                            new_tables.push(table);

                            table_id = self.manifest.next_id();
                            let table_path = self
                                .manifest
                                .manifest_path
                                .join(format!("table_{}", table_id));
                            table_writer = SSTableWriter::open(&table_path)?;
                        }
                    }
                }
                None => break,
            };
        }

        table_writer.sync()?;

        let table = SSTableReader::open(table_id, &table_path)?;
        new_tables.push(table);

        table_id = self.manifest.next_id();
        let table_path = self
            .manifest
            .manifest_path
            .join(format!("table_{}", table_id));
        table_writer = SSTableWriter::open(&table_path)?;

        sync_all_dir(self.manifest.manifest_path.as_path())?;

        self.manifest.insert_atomic()


        Ok(())
    }
}

#[derive(PartialEq)]
struct SSTableMinHeapKey {
    key: String,
    table_id: u64,
}

impl PartialOrd for SSTableMinHeapKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ordering = self.key.partial_cmp(&other.key)?;
        // when a min-heap has multiple entries for the same key, the entry of the newest table should be extracted
        Some(ordering.reverse())
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
        let file = File::create(&table_path)?;
        Ok(SSTableWriter {
            file: BufWriter::new(file),
        })
    }

    fn write(&mut self, mem_table: MemTable) -> io::Result<()> {
        for (key, value) in mem_table.inner {
            write_string(&mut self.file, &key)?;
            match value {
                MemTableValue::Value(value) => {
                    write_string(&mut self.file, &value)?;
                }
                MemTableValue::Deleted => {
                    write!(&mut self.file, "d")?;
                }
            }
        }

        self.sync()
    }

    fn insert(&mut self, key: &str, value: &str) -> io::Result<()> {
        write_string(&mut self.file, &key)?;
        write_string(&mut self.file, &value)?;

        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        Ok(())
    }
}

struct SSTableReader {
    id: u64,
    file: BufReader<File>,
}

impl SSTableReader {
    fn open(id: u64, table_path: &Path) -> Result<SSTableReader, Error> {
        let file = File::open(table_path)?;
        Ok(SSTableReader {
            id,
            file: BufReader::new(file),
        })
    }

    fn find(&mut self, key: &str) -> Result<Option<MemTableValue>, Error> {
        self.file.seek(SeekFrom::Start(0))?;

        while let Some(entry) = self.next_string()? {
            if key == entry.as_str() {
                let value = self.next_string()?;

                if try_character(&mut self.file, 'd')? {
                    return Ok(Some(MemTableValue::Deleted));
                }

                let value = value.ok_or(Error::Truncated)?;

                return Ok(Some(MemTableValue::Value(value)));
            } else {
                self.skip_string()?;
                try_character(&mut self.file, 'd')?;
            }
        }

        Ok(None)
    }

    fn next_entry(&mut self) -> Result<Option<(String, MemTableValue)>, Error> {
        let key = match self.next_string()? {
            Some(key) => key,
            None => return Ok(None),
        };

        let value = self.next_string()?;

        if try_character(&mut self.file, 'd')? {
            return Ok(Some((key, MemTableValue::Deleted)));
        }

        let value = value.ok_or(Error::Truncated)?;

        Ok(Some((key, MemTableValue::Value(value))))
    }

    fn skip_string(&mut self) -> Result<(), Error> {
        let entry_len = match self.read_len()? {
            Some(key_len) => key_len,
            None => return Ok(()),
        };

        self.file.seek_relative(entry_len as i64)?;
        Ok(())
    }

    fn next_string(&mut self) -> Result<Option<String>, Error> {
        let entry_len = match self.read_len()? {
            Some(key_len) => key_len,
            None => return Ok(None),
        };

        self.read_string(entry_len)
    }

    fn read_len(&mut self) -> Result<Option<usize>, Error> {
        let mut buf = Vec::new();
        let n = self.file.read_until(b':', &mut buf)?;
        if n == 0 {
            return Ok(None);
        }

        let len_slice = &buf[..n - 1];
        let len_str = str::from_utf8(&len_slice)?;
        let key_len = len_str.parse()?;
        Ok(Some(key_len))
    }

    fn read_string(&mut self, len: usize) -> Result<Option<String>, Error> {
        let mut buf = vec![0u8; len];
        self.file.read_exact(&mut buf)?;
        let value = String::from_utf8(buf)?;
        Ok(Some(value))
    }
}

struct ManifestReader {
    manifest_path: PathBuf,
    file: BufReader<File>,
    tables: Vec<SSTableReader>,
    id: u64,
}

impl ManifestReader {
    fn open(manifest_path: PathBuf) -> Result<ManifestReader, Error> {
        let mut manifest_file = BufReader::new(
            OpenOptions::new()
                .create(true)
                .read(true)
                .open(&manifest_path)?,
        );

        let mut buf = Vec::new();
        manifest_file.read_to_end(&mut buf)?;
        manifest_file.rewind()?;

        let table_names = str::from_utf8(buf.as_slice())?
            .lines()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        let mut tables = Vec::with_capacity(table_names.len());
        for table_name in table_names {
            let table_id = table_name
                .strip_prefix("table_")
                .ok_or_else(|| Error::InvalidTableName(String::from(table_name)))?
                .parse::<u64>()?;
            let table_path = manifest_path.parent().join(table_name);

            tables.push(SSTableReader::open(table_id, &table_path)?)
        }

        Ok(ManifestReader {
            id: tables.len() as u64,
            tables,
            file: manifest_file,
            manifest_path: manifest_path,
        })
    }
    fn insert_atomic(&mut self, table: SSTableReader) -> io::Result<()> {
        let temp_path = self.manifest_path.join("manifest.TEMP");
        let mut temp_manifest_file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_path)?,
        );
        self.tables.push(table);
        for table in &self.tables {
            writeln!(temp_manifest_file, "table_{}", table.id)?;
        }

        let temp_manifest_file = temp_manifest_file.into_inner()?;
        temp_manifest_file.sync_all()?;

        fs::rename(temp_path, self.manifest_path.join("manifest"))?;
        temp_manifest_file.sync_all()?;
        sync_all_dir(self.manifest_path.as_path())?;

        self.file = BufReader::new(temp_manifest_file);

        Ok(())
    }

    fn insert(&mut self, table: SSTableReader) -> io::Result<()> {
        writeln!(self.file.get_mut(), "table_{}", table.id)?;
        self.tables.push(table);

        Ok(())
    }


    fn next_id(&mut self) -> u64 {
        self.id += 1;
        self.id
    }
}

struct ManifestWriter {
    manifest_path: PathBuf,
    file: BufWriter<File>,
    next_id: u64,
}

impl ManifestWriter {
    fn open(manifest_path: PathBuf, next_id: u64) -> Result<ManifestWriter, Error> {
        let mut manifest_file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(&manifest_path)?,
        );


        Ok(ManifestWriter {
            next_id,
            file: manifest_file,
            manifest_path,
        })
    }

    fn insert(&mut self, table: SSTableReader) -> io::Result<()> {
        writeln!(self.file.get_mut(), "table_{}", table.id)?;

        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_mut().sync_all()?;

        Ok(())
    }


    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }
}


enum Operation {
    Insert(String, String),
    Delete(String),
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

fn peek<T>(source: &mut T) -> io::Result<Option<u8>>
where
    T: Read + Seek,
{
    let mut buf = [0u8; 1];
    let n = source.read(&mut buf)?;
    if n == 0 {
        return Ok(None);
    }

    source.seek_relative(-1)?;

    Ok(Some(buf[0]))
}

fn try_character<T>(source: &mut T, want: char) -> io::Result<bool>
where
    T: Read + Seek,
{
    match peek(source)? {
        Some(got) if got as char == want => {
            source.seek_relative(-1)?;
            Ok(true)
        }
        _ => Ok(false),
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

fn sync_all_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_dir::TempDir;
    use std::error;

    #[test]
    fn create_and_populate() -> Result<(), Box<dyn error::Error>> {
        let temp_dir = TempDir::new()?;
        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 10)?;

        for i in 0..100 {
            storage.insert(i.to_string(), i.to_string())?;
        }

        for i in 95..100 {
            storage.delete(i.to_string())?;
        }

        for i in 0..95 {
            let value = storage.get(i.to_string().as_str())?;
            assert_eq!(Some(i.to_string()), value);
        }

        for i in 95..100 {
            let value = storage.get(i.to_string().as_str())?;
            assert_eq!(None, value);
        }

        for i in 0..100 {
            storage.insert(i.to_string(), (i * 2).to_string())?;
        }

        for i in 0..100 {
            let value = storage.get(i.to_string().as_str())?;
            assert_eq!(Some((i * 2).to_string()), value);
        }

        Ok(())
    }

    #[test]
    fn data_survives_crash_before_flush() -> Result<(), Box<dyn error::Error>> {
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
}
