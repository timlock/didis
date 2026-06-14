use std::array::TryFromSliceError;
use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, IntoInnerError, Read, Seek, Write};
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
    manifest: Manifest,
    write_ahead_log: WriteAheadLogWriter,
    flush_threshold: usize,
}

impl Storage {
    pub fn new(directory_path: PathBuf, flush_threshold: usize) -> Result<Storage, Error> {
        let manifest = Manifest::open(directory_path.clone())?;

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
            return Ok(Some(value.clone()));
        }
        for table in self.manifest.tables.iter_mut() {
            let value = table.find(key)?;
            if let Some(value) = value {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    pub fn insert(&mut self, key: String, value: String) -> Result<(), Error> {
        let operation = Operation::Insert(key, value);
        self.write_ahead_log.append(&operation)?;
        self.write_ahead_log.sync_all()?;

        self.execute_no_wal(operation)
    }

    fn execute_no_wal(&mut self, operation: Operation) -> Result<(), Error> {
        match operation {
            Operation::Insert(key, value) => {
                self.mem_table.insert(key, value);
                if self.mem_table.inner.len() >= self.flush_threshold {
                    self.flush()?;
                }
            }
            Operation::Delete(_) => {
                todo!()
            }
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        let table_id = self.manifest.next_id();
        let table_path = self
            .manifest
            .directory_path
            .join(format!("table_{}", table_id));
        let mut table_writer = SSTableWriter::open(&table_path)?;
        let mem_table = mem::take(&mut self.mem_table);
        table_writer.write(mem_table)?;

        let table = SSTableReader::open(table_id, &table_path)?;
        self.manifest.insert(table)?;

        self.write_ahead_log.truncate()?;

        Ok(())
    }
}

#[derive(Default)]
struct MemTable {
    inner: BTreeMap<String, String>,
}

impl MemTable {
    fn insert(&mut self, key: String, value: String) -> Option<String> {
        self.inner.insert(key, value)
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
            write_string(&mut self.file, &value)?;
        }
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

    fn find(&mut self, key: &str) -> Result<Option<String>, Error> {
        while let Some(entry) = self.next_entry()? {
            if key == entry.as_str() {
                break;
            } else {
                self.skip_entry()?;
            }
        }

        let entry = self.next_entry()?;
        Ok(entry)
    }

    fn skip_entry(&mut self) -> Result<(), Error> {
        let entry_len = match self.read_len()? {
            Some(key_len) => key_len,
            None => return Ok(()),
        };

        self.file.seek_relative(entry_len as i64)?;
        Ok(())
    }
    fn next_entry(&mut self) -> Result<Option<String>, Error> {
        let entry_len = match self.read_len()? {
            Some(key_len) => key_len,
            None => return Ok(None),
        };

        self.read_entry(entry_len)
    }
    fn read_len(&mut self) -> Result<Option<usize>, Error> {
        let mut buf = Vec::new();
        let n = self.file.read_until(b':', &mut buf)?;
        if n == 0 {
            return Ok(None);
        }

        let len_slice = &buf[..n - 1];
        let key_len = str::from_utf8(&len_slice)?.parse()?;
        Ok(Some(key_len))
    }

    fn read_entry(&mut self, len: usize) -> Result<Option<String>, Error> {
        let mut buf = vec![0u8; len];
        self.file.read_exact(&mut buf)?;
        let value = String::from_utf8(buf)?;
        Ok(Some(value))
    }
}

struct Manifest {
    directory_path: PathBuf,
    file: BufReader<File>,
    tables: Vec<SSTableReader>,
    id: u64,
}

impl Manifest {
    fn open(directory_path: PathBuf) -> Result<Manifest, Error> {
        let mut manifest_file = BufReader::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(directory_path.join("manifest"))?,
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
            let table_path = directory_path.join(table_name);

            tables.push(SSTableReader::open(table_id, &table_path)?)
        }

        Ok(Manifest {
            id: tables.len() as u64,
            tables,
            file: manifest_file,
            directory_path,
        })
    }
    fn insert(&mut self, table: SSTableReader) -> Result<(), Error> {
        let temp_path = self.directory_path.join("manifest.TEMP");
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

        fs::rename(temp_path, self.directory_path.join("manifest"))?;

        self.file = BufReader::new(temp_manifest_file);

        Ok(())
    }
    fn next_id(&mut self) -> u64 {
        self.id += 1;
        self.id
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
        match value {
            1 => Ok(OperationCode::Insert),
            2 => Ok(OperationCode::Delete),
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
            // .truncate(true)
            .open(wal_path)?;

        Ok(WriteAheadLogWriter {
            file: BufWriter::new(wal_file),
        })
    }

    fn append(&mut self, operation: &Operation) -> io::Result<()> {
        write_operation(&mut self.file, operation)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()
    }

    fn truncate(&mut self) -> io::Result<()> {
        self.file.get_mut().set_len(0)?;
        self.sync_all()?;

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

        for i in 0..100 {
            let value = storage.get(i.to_string().as_str())?;
            assert_eq!(Some(i.to_string()), value);
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
    fn load_existing_storage() -> Result<(), Box<dyn error::Error>> {
        let temp_dir = TempDir::new()?;
        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 10)?;

        for i in 0..100 {
            storage.insert(i.to_string(), i.to_string())?;
        }

        let mut storage = Storage::new(temp_dir.path().to_path_buf(), 10)?;

        for i in 0..100 {
            let value = storage.get(i.to_string().as_str())?;
            assert_eq!(Some(i.to_string()), value);
        }

        Ok(())
    }
}
