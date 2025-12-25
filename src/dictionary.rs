use crate::parser::command::{ExpireRule, OverwriteRule, SetValueExpireRule};
use crate::persistence;
use crate::persistence::{Value, ValueType, RDB};
use std::borrow::Cow;
use std::cmp::min;
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::time::Duration;
use std::{collections::HashMap, error, io, time::SystemTime};

#[derive(Default)]
pub struct Dictionary {
    inner: HashMap<String, Entry>,
}

impl Dictionary {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
    pub fn get(&self, key: &str) -> Result<Option<&str>, Error> {
        let entry = match self.inner.get(key) {
            Some(entry) => entry,
            None => return Ok(None),
        };

        let string_value = entry.get_string()?;

        let expires_at = match entry.expires_at {
            Some(expires_at) => expires_at,
            None => return Ok(Some(string_value)),
        };

        if SystemTime::UNIX_EPOCH + Duration::from(expires_at) < SystemTime::now() {
            return Ok(None);
        }

        Ok(Some(string_value))
    }

    pub fn exists(&mut self, key: &str) -> bool {
        self.inner.contains_key(key)
    }

    pub fn delete(&mut self, key: &str) -> bool {
        match self.inner.remove(key) {
            Some(_) => true,
            None => false,
        }
    }

    pub fn increment(&mut self, key: &str, by: i64) -> Result<i64, Error> {
        let value = self.get_string_or_insert_mut(key, "0")?;

        increment(value, by)
    }

    pub fn decrement(&mut self, key: &str, by: i64) -> Result<i64, Error> {
        let entry = self.get_string_or_insert_mut(key, "0")?;

        decrement(entry, by)
    }

    pub fn set(
        &mut self,
        key: &str,
        value: String,
        overwrite_rule: Option<OverwriteRule>,
        get: bool,
        expire_rule: Option<SetValueExpireRule>,
    ) -> Result<Option<String>, Error> {
        match overwrite_rule {
            Some(OverwriteRule::NotExists) if self.inner.contains_key(key) => {
                return Err(Error::OverrideConflict);
            }
            Some(OverwriteRule::Exists) if !self.inner.contains_key(key) => {
                return Err(Error::OverrideConflict);
            }
            _ => {}
        };

        let keep_ttl = matches!(expire_rule, Some(SetValueExpireRule::KeepTTL));

        let (old_value, old_expires_at) = if keep_ttl || get {
            self.remove_string(key)?
        } else {
            self.inner.remove(key);
            (None, None)
        };

        let expires_at = expire_rule
            .as_ref()
            .and_then(|r| r.calculate_expire_time(old_expires_at));

        self.inner.insert(
            key.to_string(),
            Entry::new(EntryType::String(value), expires_at),
        );

        match get {
            true => Ok(old_value),
            false => Ok(None),
        }
    }
    pub fn list_range(&self, key: &str, start: i64, end: i64) -> Result<Vec<&String>, Error> {
        let list = match self.get_list(key).transpose()? {
            Some(list) => list,
            None => return Ok(Vec::new()),
        };

        let start = to_range_index(start, list.len());
        let end = to_range_index(end, list.len());

        if start > end {
            return Ok(Vec::new());
        }

        Ok(list.range(start..=end).collect())
    }

    pub fn left_push(&mut self, key: &str, items: Vec<Cow<str>>) -> Result<i64, Error> {
        let list = self.get_list_or_insert_mut(key)?;
        for item in items {
            list.push_front(item.into_owned());
        }

        Ok(list.len() as i64)
    }

    pub fn right_push(&mut self, key: &str, items: Vec<Cow<str>>) -> Result<i64, Error> {
        let list = self.get_list_or_insert_mut(key)?;
        for item in items {
            list.push_back(item.into_owned());
        }

        Ok(list.len() as i64)
    }

    pub fn expire(&mut self, key: &str, seconds: u64, expire_rule: Option<ExpireRule>) -> bool {
        let entry = match self.inner.get_mut(key) {
            Some(entry) => entry,
            None => return false,
        };
        let new_expires_at = SystemTime::now() + Duration::from_secs(seconds);

        match expire_rule {
            Some(ExpireRule::HasExpiry) => {
                if entry.expires_at.is_none() {
                    return false;
                }
            }
            Some(ExpireRule::NoExpiry) => {
                if entry.expires_at.is_some() {
                    return false;
                }
            }
            Some(ExpireRule::GreaterThan) => {
                if let Some(expires_at) = entry.expires_at
                    && new_expires_at
                        < SystemTime::UNIX_EPOCH + Duration::from_millis(expires_at.milliseconds())
                {
                    return false;
                }
            }
            Some(ExpireRule::LessThan) => {
                if let Some(expires_at) = entry.expires_at
                    && new_expires_at
                        > SystemTime::UNIX_EPOCH + Duration::from_millis(expires_at.milliseconds())
                {
                    return false;
                }
            }
            None => {}
        };

        let expires_at_secs = new_expires_at
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        entry.expires_at = Some(Timestamp::Seconds(expires_at_secs as u32));

        true
    }

    pub fn snapshot(&self) -> RDB {
        let mut hash_map = HashMap::new();
        for (key, entry) in &self.inner {
            hash_map.insert(key.clone(), persistence::Value::from(entry.clone()));
        }

        RDB::new(HashMap::new(), HashMap::from([(0, hash_map)]))
    }

    fn get_string_or_insert_mut(&mut self, key: &str, value: &str) -> Result<&mut String, Error> {
        if let None = self.inner.get(key) {
            self.inner.insert(
                key.to_string(),
                Entry::new(EntryType::String(String::from(value)), None),
            );
        }

        self.get_string_mut(key)
            .expect("map entry should be populated after an insert")
    }

    fn get_list_or_insert_mut(&mut self, key: &str) -> Result<&mut VecDeque<String>, Error> {
        if let None = self.inner.get(key) {
            self.inner.insert(
                key.to_string(),
                Entry::new(EntryType::List(Default::default()), None),
            );
        }

        self.get_list_mut(key)
            .expect("map entry should be populated after an insert")
    }

    fn get_string(&self, key: &str) -> Option<Result<&String, Error>> {
        let entry = self.inner.get(key)?;
        Some(entry.get_string())
    }

    fn get_string_mut(&mut self, key: &str) -> Option<Result<&mut String, Error>> {
        let entry = self.inner.get_mut(key)?;
        Some(entry.get_string_mut())
    }

    fn get_list(&self, key: &str) -> Option<Result<&VecDeque<String>, Error>> {
        let entry = self.inner.get(key)?;
        Some(entry.get_list())
    }
    fn get_list_mut(&mut self, key: &str) -> Option<Result<&mut VecDeque<String>, Error>> {
        let entry = self.inner.get_mut(key)?;
        Some(entry.get_list_mut())
    }

    fn remove_string(&mut self, key: &str) -> Result<(Option<String>, Option<Timestamp>), Error> {
        match self.inner.get(key) {
            Some(entry) => match entry.entry_type {
                EntryType::String(_) => {}
                _ => return Err(Error::WrongType),
            },
            None => return Ok((None, None)),
        };

        let old = self
            .inner
            .remove(key)
            .expect("map entry should exist after a get call");
        match old.entry_type {
            EntryType::String(value) => Ok((Some(value), old.expires_at)),
            _ => {
                panic!("map entry should not change its type after a get call")
            }
        }
    }
}

fn to_range_index(relative_index: i64, max: usize) -> usize {
    if relative_index < 0 {
        max.checked_sub(relative_index.abs() as usize)
            .unwrap_or_default()
    } else {
        min(max - 1, relative_index as usize)
    }
}

#[derive(Debug)]
pub enum Error {
    OverrideConflict,
    NoInteger,
    WrongType,
    Io(io::Error),
    Persistence(persistence::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::OverrideConflict => {
                write!(f, "Override conflict")
            }
            Error::NoInteger => {
                write!(f, "value is not an integer or out of range")
            }
            Error::WrongType => {
                write!(
                    f,
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )
            }
            Error::Io(err) => err.fmt(f),
            Error::Persistence(err) => err.fmt(f),
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<persistence::Error> for Error {
    fn from(value: persistence::Error) -> Self {
        Error::Persistence(value)
    }
}

impl error::Error for Error {}

#[derive(Debug, Clone)]
enum EntryType {
    String(String),
    List(VecDeque<String>),
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Timestamp {
    Seconds(u32),
    Milliseconds(u64),
}

impl Timestamp {
    pub fn milliseconds(&self) -> u64 {
        match self {
            Timestamp::Seconds(s) => *s as u64 * 1000,
            Timestamp::Milliseconds(ms) => *ms,
        }
    }
    pub fn seconds(&self) -> u32 {
        match self {
            Timestamp::Seconds(s) => *s,
            Timestamp::Milliseconds(ms) => (*ms / 1000) as u32,
        }
    }
}

impl From<Timestamp> for Duration {
    fn from(value: Timestamp) -> Self {
        match value {
            Timestamp::Seconds(s) => Duration::from_secs(s as u64),
            Timestamp::Milliseconds(ms) => Duration::from_millis(ms),
        }
    }
}

#[derive(Debug, Clone)]
struct Entry {
    entry_type: EntryType,
    expires_at: Option<Timestamp>,
}

impl Entry {
    fn new(entry_type: EntryType, expires_at: Option<Timestamp>) -> Entry {
        Entry {
            entry_type,
            expires_at,
        }
    }
    fn get_string(&self) -> Result<&String, Error> {
        match &self.entry_type {
            EntryType::String(entry) => Ok(entry),
            EntryType::List(_) => Err(Error::WrongType),
        }
    }

    fn get_string_mut(&mut self) -> Result<&mut String, Error> {
        match &mut self.entry_type {
            EntryType::String(entry) => Ok(entry),
            EntryType::List(_) => Err(Error::WrongType),
        }
    }

    fn get_list(&self) -> Result<&VecDeque<String>, Error> {
        match &self.entry_type {
            EntryType::String(_) => Err(Error::WrongType),
            EntryType::List(list) => Ok(list),
        }
    }

    fn get_list_mut(&mut self) -> Result<&mut VecDeque<String>, Error> {
        match &mut self.entry_type {
            EntryType::String(_) => Err(Error::WrongType),
            EntryType::List(list) => Ok(list),
        }
    }
}

impl From<Entry> for persistence::Value {
    fn from(value: Entry) -> Self {
        match value.entry_type {
            EntryType::String(string) => {
                persistence::Value::new(value.expires_at, ValueType::String(string))
            }
            EntryType::List(list) => {
                persistence::Value::new(value.expires_at, ValueType::List(Vec::from(list)))
            }
        }
    }
}

impl From<persistence::Value> for Entry {
    fn from(value: Value) -> Self {
        let entry_type = match value.value_type {
            ValueType::String(string) => EntryType::String(string),
            ValueType::List(list) => EntryType::List(VecDeque::from(list)),
            ValueType::Set(set) => {
                todo!()
            }
        };

        Entry::new(entry_type, value.expires_at)
    }
}

fn increment(string: &mut String, by: i64) -> Result<i64, Error> {
    let mut integer_value: i64 = string.parse().map_err(|err| Error::NoInteger)?;
    integer_value = integer_value
        .checked_add(by)
        .ok_or_else(|| Error::NoInteger)?;
    *string = integer_value.to_string();

    Ok(integer_value)
}

fn decrement(string: &mut String, by: i64) -> Result<i64, Error> {
    let mut integer_value: i64 = string.parse().map_err(|err| Error::NoInteger)?;
    integer_value = integer_value
        .checked_sub(by)
        .ok_or_else(|| Error::NoInteger)?;
    *string = integer_value.to_string();

    Ok(integer_value)
}
