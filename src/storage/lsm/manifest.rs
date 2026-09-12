use crate::storage::lsm::sstable::SSTable;
use crate::storage::lsm::{Error};
use log::info;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::ops::RangeInclusive;
use std::path::Path;

#[derive(Debug)]
pub struct ManifestReader {
    file: BufReader<File>,
}

impl ManifestReader {
    pub fn open(path: impl AsRef<Path>) -> io::Result<ManifestReader> {
        let file = File::open(path)?;
        Ok(ManifestReader {
            file: BufReader::new(file),
        })
    }

    pub fn read_all(
        &mut self,
    ) -> Result<Vec<(u64, Vec<(RangeInclusive<String>, u64, String)>)>, Error> {
        self.file.seek(SeekFrom::Start(0))?;

        let mut result = Vec::new();
        while let Some(level) = self.read_next_level() {
            result.push(level?);
        }

        Ok(result)
    }

    fn read_next_level(
        &mut self,
    ) -> Option<Result<(u64, Vec<(RangeInclusive<String>, u64, String)>), Error>> {
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

    fn read_level_header(&mut self) -> Option<Result<u64, Error>> {
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
        Some(level_str.parse::<u64>().map_err(Error::from))
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
pub struct ManifestWriter {
    file: BufWriter<File>,
}
impl ManifestWriter {
    pub fn create(path: impl AsRef<Path>) -> io::Result<ManifestWriter> {
        info!("Creating manifest {:?}", path.as_ref());
        let file = File::create(path)?;
        Ok(ManifestWriter {
            file: BufWriter::new(file),
        })
    }

    pub fn write_level<'a>(
        &mut self,
        level: u64,
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

    pub fn sync(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        Ok(())
    }
}
fn parse_table_id(value: &str) -> Result<u64, Error> {
    value
        .strip_prefix("TABLE_")
        .ok_or_else(|| Error::InvalidTableName(value.to_string()))?
        .parse::<u64>()
        .map_err(|err| err.into())
}
