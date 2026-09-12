use crate::storage::lsm::{Error, Operation, read_operation, write_operation};
use log::info;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Seek, Write};
use std::path::Path;

#[derive(Debug)]
pub struct WriteAheadLogReader {
    file: BufReader<File>,
}

impl WriteAheadLogReader {
    pub fn open(wal_path: &Path) -> io::Result<WriteAheadLogReader> {
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
pub struct WriteAheadLogWriter {
    file: BufWriter<File>,
}

impl WriteAheadLogWriter {
    pub fn open(wal_path: &Path) -> io::Result<WriteAheadLogWriter> {
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

    pub fn append(&mut self, operation: &Operation) -> io::Result<()> {
        write_operation(&mut self.file, operation)
    }

    pub fn sync_data(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()
    }

    pub fn truncate(&mut self) -> io::Result<()> {
        self.file.get_mut().set_len(0)?;
        self.sync_data()?;

        Ok(())
    }
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
