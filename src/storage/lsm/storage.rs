use crate::storage::lsm::heap::MinHeap;
use crate::storage::lsm::manifest::{ManifestReader, ManifestWriter};
use crate::storage::lsm::sstable::{SSTable, SSTableDataIter, SSTableReader, SSTableWriter};
use crate::storage::lsm::wal::{WriteAheadLogReader, WriteAheadLogWriter};
use crate::storage::lsm::{Error, Operation};
use log::info;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{ErrorKind};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::{fs, io, mem};

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
    levels: BTreeMap<u64, Vec<SSTable>>,
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
                let tables_len = self.levels.get(&(level as u64)).map(Vec::len).unwrap_or(0);
                if tables_len > (level + 1) * self.level_ratio {
                    self.compact_level(level as u64)?;
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
            .collect::<Vec<_>>();
        writer.write_data_blocks(&operations)?;
        writer.write_index_blocks()?;
        self.sync_dir()?;

        self.save_manifest()?;

        self.write_ahead_log.truncate()?;

        info!("Flush complete took {:?}", start.elapsed());

        Ok(())
    }

    fn compact_level(&mut self, level: u64) -> Result<(), Error> {
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

        let merger = Merger::new(old_table_iters)?;
        for extracted in merger {
            let (heap_key, operation) = extracted?;

            if let Operation::Delete(_) = &operation
                && level == highest_level as u64
            {
                // Tombstones may only be dropped at the highest level, otherwise operations at
                // levels higher than the current one could resurface
                continue;
            }

            entries += 1;

            if entries > max_table_size {
                writer.write_index_blocks()?;
                writer.sync()?;

                (table, writer) = self.create_table(level + 1)?;
                entries = 1;
            }

            writer.write_operation(operation)?;
            table.key_range = grow_range(heap_key.key, table.key_range.clone());
        }

        writer.write_index_blocks()?;
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

    fn create_table(&mut self, level: u64) -> Result<(&mut SSTable, SSTableWriter), Error> {
        let table_id = self.id_generator.next();
        let path = self.locations.table_path(format!("TABLE_{}", table_id));

        let writer = SSTableWriter::create(&path)?;

        let reader = SSTableReader::open(&path)?;

        let table = SSTable::new(
            table_id,
            format!("TABLE_{}", table_id),
            path,
            String::new()..=String::new(),
            reader,
        );

        let level_tables = self.levels.entry(level).or_default();
        level_tables.push(table);

        let table = level_tables
            .last_mut()
            .expect("Table should be present after it has been pushed to the level vec");
        Ok((table, writer))
    }
}

struct Merger<'a> {
    sources: HashMap<u64, (u64, SSTableDataIter<'a>)>,
    min_heap: MinHeap<MinHeapKey, Operation>,
}

impl<'a> Merger<'a> {
    fn new(mut sources: HashMap<u64, (u64, SSTableDataIter<'a>)>) -> Result<Merger<'a>, Error> {
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
    table_level: u64,
}

impl MinHeapKey {
    fn new(key: String, table_id: u64, table_level: u64) -> MinHeapKey {
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
fn read_manifest(manifest_path: impl AsRef<Path>) -> Result<BTreeMap<u64, Vec<SSTable>>, Error> {
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
                    if key == "yivzv" {
                        dbg!()
                    }
                    let got = storage.get(key)?;
                    assert_eq!(want, got.as_deref(), "line {} {:?}", i, cmd);
                }
                Cmd::Put { key, value } => {
                    if key == "yivzv" {
                        dbg!()
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
