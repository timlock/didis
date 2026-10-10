use didis::logger;
use didis::storage::lsm::storage::Storage;
use didis::temp_dir::TempDir;
use log::Level;
use std::sync::Once;
use std::{env, error};

static PUT_FILE: &str = include_str!("testdata/put.txt");
static PUT_DELETE_FILE: &str = include_str!("testdata/put-delete.txt");

static INIT: Once = Once::new();

fn initialize() {
    INIT.call_once(|| {
        let _ = logger::init(Level::Info); // returns err when logger is already initialized, can be ignored
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
        match iter.next().ok_or("empty line")? {
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
                assert_eq!(want, got.as_deref(), "line {i} {cmd:?}");
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
                assert_eq!(want, got.as_deref(), "line {i} {cmd:?}");
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
                assert_eq!(want, got.as_deref(), "line {i} {cmd:?}");
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

    storage.insert("one".to_owned(), "value one".to_owned())?;
    storage.insert("two".to_owned(), "value two".to_owned())?;
    storage.delete("two".to_owned())?;

    let mut storage = Storage::new(temp_dir.path().to_path_buf(), 10, 5)?;

    assert_eq!(Some("value one".to_owned()), storage.get("one")?);
    assert_eq!(None, storage.get("two")?);

    Ok(())
}

#[test]
fn compaction() -> Result<(), Box<dyn error::Error>> {
    initialize();
    let temp_dir = TempDir::create(env::current_dir()?.join("temp"))?;
    let mut storage = Storage::new(temp_dir.path().to_path_buf(), 2, 2)?;

    storage.insert("1".to_owned(), "one".to_owned())?;
    storage.insert("2".to_owned(), "two".to_owned())?;
    storage.insert("3".to_owned(), "three".to_owned())?;
    storage.insert("4".to_owned(), "four".to_owned())?;
    storage.insert("5".to_owned(), "five".to_owned())?;
    storage.delete("2".to_owned())?;
    assert_eq!(None, storage.get("2")?);
    storage.insert("2".to_owned(), "two".to_owned())?;
    storage.insert("3".to_owned(), "updated three".to_owned())?;

    assert_eq!(Some("one".to_owned()), storage.get("1")?);
    assert_eq!(Some("two".to_owned()), storage.get("2")?);
    assert_eq!(Some("updated three".to_owned()), storage.get("3")?);
    assert_eq!(Some("four".to_owned()), storage.get("4")?);
    assert_eq!(Some("five".to_owned()), storage.get("5")?);

    Ok(())
}