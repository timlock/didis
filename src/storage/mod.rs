use std::array::TryFromSliceError;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, Write};
use std::num::ParseIntError;
use std::path::Path;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::{io, mem};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    WrongMagicNumber(Vec<u8>),
    FromUtf8(FromUtf8Error),
    Utf8(Utf8Error),
    TryFromSliceError(TryFromSliceError),
    Truncated,
    InvalidLengthEncoding(u8),
    ExpectedLength,
    ParseIntError(ParseIntError),
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

pub struct Storage {
    mem_table: MemTable,
    manifest: Manifest,
    flush_threshold: usize,
}

impl Storage {
    pub fn new(directory_path: &Path, flush_threshold: usize) -> Result<Storage, Error> {
        let mut manifest_file = BufReader::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(directory_path.join("manifest"))?,
        );

        let mut buf = Vec::new();
        manifest_file.read_to_end(&mut buf)?;
        manifest_file.rewind()?;

        let table_names = str::from_utf8(buf.as_slice())?
            .split(',')
            .collect::<Vec<_>>();

        let mut tables = Vec::with_capacity(table_names.len());
        for table in table_names {
            let table_path = directory_path.join(table);
            let file = File::open(table_path)?;
            let table_id = table
                .strip_prefix("table_")
                .expect("all entries in manifest file should start with 'table_'")
                .parse::<u64>()?;
            tables.push(SSTable {
                id: table_id,
                file: BufReader::new(file),
            })
        }

        Ok(Storage {
            flush_threshold,
            mem_table: MemTable::default(),
            manifest: Manifest {
                id: tables.len() as u64,
                tables,
                file: manifest_file,
            },
        })
    }

    pub fn read(&mut self, key: &str) -> Result<Option<String>, Error> {
        if let Some(value) = self.mem_table.inner.get(key) {
            return Ok(Some(value.clone()));
        }
        for ss_table in self.manifest.tables.iter_mut() {
            let value = ss_table.find(key)?;
            if let Some(value) = value {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    pub fn write(&mut self, key: String, value: String) -> Result<(), Error> {
        self.mem_table.insert(key, value);
        if self.mem_table.inner.len() >= self.flush_threshold {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        let table_id = self.manifest.next_id();
        let mut file = File::create(format!("table_{}", table_id))?;

        let mem_table = mem::take(&mut self.mem_table);
        let bytes = Vec::<u8>::from(mem_table);
        file.write_all(bytes.as_slice())?;
        let ss_table = SSTable {
            id: table_id,
            file: BufReader::new(file),
        };
        self.manifest.tables.push(ss_table);

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

impl From<MemTable> for Vec<u8> {
    fn from(value: MemTable) -> Self {
        let mut bytes = Vec::new();
        for (key, value) in value.inner.iter() {
            let key_bytes = key.as_bytes();
            bytes.extend_from_slice(key_bytes.len().to_le_bytes().as_slice());
            bytes.push(b':');
            bytes.extend_from_slice(key_bytes);

            let value_bytes = value.as_bytes();
            bytes.extend_from_slice(value_bytes.len().to_le_bytes().as_slice());
            bytes.push(b':');
            bytes.extend_from_slice(value_bytes)
        }
        bytes
    }
}

struct SSTable {
    id: u64,
    file: BufReader<File>,
}

impl SSTable {
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

        let a = buf[..n].try_into()?;
        let key_len = usize::from_le_bytes(a);
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
    file: BufReader<File>,
    tables: Vec<SSTable>,
    id: u64,
}

impl Manifest {
    fn insert(&mut self, mut table: SSTable) -> Result<(), Error> {
        write!(table.file.get_mut(), "table_{},", table.id);
        self.tables.push(table);

        Ok(())
    }
    fn next_id(&mut self) -> u64 {
        self.id += 1;
        self.id
    }
}


