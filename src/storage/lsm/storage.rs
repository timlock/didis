use crate::heap::MinHeap;
use crate::radix_tree::RadixTree;
use crate::storage::lsm::bloom::{BloomFilter, BloomFilterBuilder};
use crate::storage::lsm::manifest::{Entry, ManifestReader, ManifestWriter};
use crate::storage::lsm::sstable::{SSTableReader, SSTableWriter};
use crate::storage::lsm::wal::{WriteAheadLogReader, WriteAheadLogWriter};
use crate::storage::lsm::{Error, Operation};
use log::info;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::ErrorKind;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::{fs, io, iter, mem};

#[derive(Debug)]
struct Locations {
    directory: PathBuf,
    manifest: PathBuf,
    manifest_temp: PathBuf,
    wal: PathBuf,
}

impl Locations {
    fn table_dir_path(&self, table_name: impl AsRef<Path>) -> PathBuf {
        self.directory.join(table_name)
    }

    fn data_file_path(&self, table_name: impl AsRef<Path>) -> PathBuf {
        self.table_dir_path(table_name).join("data.db")
    }

    fn index_file_path(&self, table_name: impl AsRef<Path>) -> PathBuf {
        self.table_dir_path(table_name).join("index.db")
    }

    fn bloom_filter_file_path(&self, table_name: impl AsRef<Path>) -> PathBuf {
        self.table_dir_path(table_name).join("bloom-filter.db")
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
struct SSTableMetadata {
    id: u64,
    name: String,
    dir_path: PathBuf,
    key_range: RangeInclusive<String>,
    level: u32,
}

impl SSTableMetadata {
    fn has_key_in_range(&self, key: &str) -> bool {
        self.key_range.start().as_str() <= key && key <= self.key_range.end().as_str()
    }
}

#[derive(Debug)]
pub struct Storage {
    mem_table: MemTable,
    levels: BTreeMap<u32, Vec<(SSTableMetadata, SSTableReader, BloomFilter)>>,
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
        let levels = match read_manifest(&locations.manifest, &locations) {
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
            .map(|(table)| table.id)
            .max()
            .map_or(1, |id| id + 1);
        let id_generator = IdGenerator { next_id };

        let mut storage = Storage {
            mem_table: MemTable::default(),
            levels: BTreeMap::new(),
            write_ahead_log: wal_writer,
            locations,
            max_table_size,
            level_ratio,
            id_generator,
        };

        for (_, tables) in levels {
            for table in tables {
                storage.open_table(table)?;
            }
        }

        let wal_reader = match WriteAheadLogReader::open(&storage.locations.wal) {
            Ok(wal_reader) => Some(wal_reader),
            Err(err) if err.kind() == ErrorKind::NotFound => None,
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
            for (table, reader, bloom_filter) in tables.iter_mut().rev() {
                if table.has_key_in_range(key) && bloom_filter.contains(key) {
                    let value = reader.find_value(key)?;
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
                let tables_len = self.levels.get(&(level as u32)).map(Vec::len).unwrap_or(0);
                if tables_len >= (level + 1) * self.level_ratio {
                    self.compact_level(level as u32)?;
                }
            }
        }

        Ok(())
    }

    fn sync_root_dir(&self) -> io::Result<()> {
        File::open(self.locations.directory.as_path())?.sync_all()
    }

    fn sync_table_dir(&self, table_name: &str) -> io::Result<()> {
        File::open(self.locations.table_dir_path(table_name))?.sync_all()
    }

    fn flush(&mut self) -> Result<(), Error> {
        info!("Flushing memtable");
        let start = Instant::now();

        let mem_table = mem::take(&mut self.mem_table);

        let (mut table, mut writer) = create_table(self.id_generator.next(), 0, &self.locations)?;

        let operations = mem_table
            .inner
            .into_iter()
            .map(|(key, operation)| match operation {
                MemTableValue::Value(value) => Operation::Insert(key, value),
                MemTableValue::Deleted => Operation::Delete(key),
            })
            .collect::<Vec<_>>();

        let min = operations
            .first()
            .expect("flush should not be called when mem_table is empty")
            .key()
            .to_owned();
        let max = operations
            .last()
            .expect("flush should not be called when mem_table is empty")
            .key()
            .to_owned();
        table.key_range = min..=max;

        writer.write(&operations)?;
        let mut bloom_filter_builder = BloomFilterBuilder::default();
        let keys = operations.iter().map(|operation| operation.key());
        bloom_filter_builder.append(keys);

        self.save_table(table, writer, bloom_filter_builder)?;
        self.save_manifest()?;

        self.write_ahead_log.truncate()?;

        info!("Flush complete took {:?}", start.elapsed());

        Ok(())
    }

    fn compact_level(&mut self, level: u32) -> Result<(), Error> {
        info!("Compacting level {level}");
        let start = Instant::now();

        let (max_table_size, highest_level) = (self.max_table_size, self.levels.len());

        let mut old_levels = self.remove_levels(level..=level + 1);

        let mut old_table_iters = HashMap::new();
        for (level, tables) in &mut old_levels {
            for (old_table_metadata, old_table_reader, _) in tables {
                old_table_iters.insert(old_table_metadata.id, (*level, old_table_reader.iter()?));
            }
        }

        let (mut table, mut writer) =
            create_table(self.id_generator.next(), level + 1, &self.locations)?;

        let mut bloom_filter_builder = BloomFilterBuilder::default();
        let merger = Merger::new(old_table_iters)?;
        for extracted in merger {
            let (heap_key, operation) = extracted?;

            if let Operation::Delete(_) = &operation
                && level == highest_level as u32
            {
                // Tombstones may only be dropped at the highest level, otherwise operations at
                // levels higher than the current one could resurface
                continue;
            }

            if bloom_filter_builder.len() > max_table_size {
                self.save_table(table, writer, bloom_filter_builder)?;
                bloom_filter_builder = BloomFilterBuilder::default();

                (table, writer) =
                    create_table(self.id_generator.next(), level + 1, &self.locations)?;
            }

            bloom_filter_builder.insert(operation.key());
            writer.write(iter::once(&operation))?;
            table.key_range = grow_range(heap_key.key, table.key_range.clone());
        }

        self.save_table(table, writer, bloom_filter_builder)?;

        self.save_manifest()?;

        for metadata in old_levels
            .into_values()
            .flatten()
            .map(|(metadata, _, _)| metadata)
        {
            info!("Deleting old table directory {:?}", metadata.dir_path);
            fs::remove_dir_all(metadata.dir_path)?;
        }

        info!("Compaction complete, took {:?}", start.elapsed());

        Ok(())
    }

    fn save_table(
        &mut self,
        metadata: SSTableMetadata,
        mut writer: SSTableWriter,
        bloom_filter_builder: BloomFilterBuilder,
    ) -> Result<(), Error> {
        writer.sync()?;

        let bloom_filter = bloom_filter_builder.build(0.01, 5);

        let bloom_filter_file_path = self.locations.bloom_filter_file_path(&metadata.name);
        create_bloom_filter(&bloom_filter, &bloom_filter_file_path)?;

        self.sync_table_dir(&metadata.name)?;

        self.open_table(metadata)?;
        Ok(())
    }

    fn save_manifest(&mut self) -> Result<(), Error> {
        let mut manifest_writer = ManifestWriter::create(&self.locations.manifest_temp)?;

        for (level, tables) in self.levels.iter() {
            manifest_writer.write_level(
                *level,
                tables
                    .iter()
                    .map(|(table, _, _)| (&table.key_range, table.name.as_str())),
            )?;
        }

        manifest_writer.sync()?;
        fs::rename(&self.locations.manifest_temp, &self.locations.manifest)?;
        File::open(&self.locations.manifest)?.sync_all()?;
        self.sync_root_dir()?;

        Ok(())
    }

    fn open_table(&mut self, metadata: SSTableMetadata) -> Result<(), Error> {
        let data_file_path = self.locations.data_file_path(&metadata.name);
        let index_file_path = self.locations.index_file_path(&metadata.name);
        let bloom_filter_file_path = self.locations.bloom_filter_file_path(&metadata.name);

        let reader = SSTableReader::open(index_file_path, data_file_path)?;

        let bytes = fs::read(&bloom_filter_file_path)?;
        let bloom_filter = BloomFilter::try_from(bytes)?;

        let level_tables = self.levels.entry(metadata.level).or_default();
        level_tables.push((metadata, reader, bloom_filter));

        Ok(())
    }

    fn remove_levels(
        &mut self,
        levels: RangeInclusive<u32>,
    ) -> HashMap<u32, Vec<(SSTableMetadata, SSTableReader, BloomFilter)>> {
        let mut tables = HashMap::new();
        for level in levels {
            tables.insert(level, self.levels.remove(&level).unwrap_or_default());
        }

        tables
    }
}

struct Merger<I> {
    sources: HashMap<u64, (u32, I)>,
    min_heap: MinHeap<MinHeapKey, Operation>,
}

impl<I> Merger<I>
where
    I: Iterator<Item = Result<Operation, Error>>,
{
    fn new(mut sources: HashMap<u64, (u32, I)>) -> Result<Merger<I>, Error> {
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

impl<I> Iterator for Merger<I>
where
    I: Iterator<Item = Result<Operation, Error>>,
{
    type Item = Result<(MinHeapKey, Operation), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let (heap_key, operation) = self.min_heap.extract()?;

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

fn create_table(
    table_id: u64,
    level: u32,
    locations: &Locations,
) -> Result<(SSTableMetadata, SSTableWriter), Error> {
    let table_name = format!("TABLE_{table_id}");
    let table_dir_path = locations.table_dir_path(&table_name);
    let data_file_path = locations.data_file_path(&table_name);
    let index_file_path = locations.index_file_path(&table_name);

    fs::create_dir(&table_dir_path)?;
    let writer = SSTableWriter::create(&data_file_path, &index_file_path)?;

    let table = SSTableMetadata {
        id: table_id,
        name: table_name,
        dir_path: table_dir_path,
        key_range: String::new()..=String::new(),
        level,
    };

    Ok((table, writer))
}

fn create_bloom_filter(bloom_filter: &BloomFilter, path: impl AsRef<Path>) -> io::Result<()> {
    let bytes = Vec::try_from(bloom_filter)?;
    fs::write(&path, bytes)?;

    File::open(&path)?.sync_all()?;
    Ok(())
}

#[derive(PartialEq, Debug, Clone)]
struct MinHeapKey {
    key: String,
    table_id: u64,
    table_level: u32,
}

impl MinHeapKey {
    fn new(key: String, table_id: u64, table_level: u32) -> MinHeapKey {
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
    inner: RadixTree<MemTableValue>,
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

        self.inner.insert(key.to_owned(), MemTableValue::Deleted);

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
fn read_manifest(
    manifest_path: impl AsRef<Path>,
    locations: &Locations,
) -> Result<BTreeMap<u32, Vec<SSTableMetadata>>, Error> {
    let mut manifest_reader = ManifestReader::open(&manifest_path)?;

    let level_entries = manifest_reader.read_all()?;
    let mut levels = BTreeMap::new();
    for (level, entries) in level_entries {
        let mut tables = Vec::new();

        for entry in entries {
            let table_dir_path = locations.table_dir_path(&entry.name);

            tables.push(SSTableMetadata {
                id: entry.id,
                name: entry.name,
                dir_path: table_dir_path,
                key_range: entry.key_range,
                level,
            });
        }

        levels.insert(level, tables);
    }

    Ok(levels)
}

fn grow_range(key: String, range: RangeInclusive<String>) -> RangeInclusive<String> {
    let (mut start, mut end) = range.into_inner();

    if start.is_empty() {
        start = key.clone();
    }
    if end.is_empty() {
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
    use std::error;

    #[test]
    fn merger() -> Result<(), Box<dyn error::Error>> {
        let mut operations = HashMap::new();
        operations.insert(
            3,
            (
                0,
                vec![
                    Ok(Operation::Delete("1".to_owned())),
                    Ok(Operation::Delete("2".to_owned())),
                ]
                .into_iter(),
            ),
        );
        operations.insert(
            4,
            (
                0,
                vec![
                    Ok(Operation::Delete("1".to_owned())),
                    Ok(Operation::Delete("2".to_owned())),
                ]
                .into_iter(),
            ),
        );
        operations.insert(
            1,
            (
                1,
                vec![
                    Ok(Operation::Delete("1".to_owned())),
                    Ok(Operation::Delete("2".to_owned())),
                ]
                .into_iter(),
            ),
        );
        operations.insert(
            2,
            (
                1,
                vec![
                    Ok(Operation::Delete("1".to_owned())),
                    Ok(Operation::Delete("2".to_owned())),
                ]
                .into_iter(),
            ),
        );

        let mut merger = Merger::new(operations)?;
        assert_eq!(
            (
                MinHeapKey {
                    key: "1".to_owned(),
                    table_id: 4,
                    table_level: 0
                },
                Operation::Delete("1".to_owned())
            ),
            merger.next().unwrap()?,
        );

        assert_eq!(
            (
                MinHeapKey {
                    key: "2".to_owned(),
                    table_id: 4,
                    table_level: 0
                },
                Operation::Delete("2".to_owned())
            ),
            merger.next().unwrap()?,
        );

        assert!(merger.next().is_none());

        Ok(())
    }
}
