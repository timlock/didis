use crate::parser::command::{ExpireRule, OverwriteRule};
use std::borrow::Cow;
use std::cmp::{max, min};
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::ops::RangeInclusive;
use std::{collections::HashMap, error, time::SystemTime};

enum Entry {
    String(StringEntry),
    List(VecDeque<String>),
}

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

        let entry = match entry {
            Entry::String(entry) => entry,
            Entry::List(_) => return Err(Error::WrongType),
        };

        let expires_at = match entry.expires_at {
            Some(expires_at) => expires_at,
            None => return Ok(Some(entry.value.as_str())),
        };

        if expires_at < SystemTime::now() {
            return Ok(None);
        }

        Ok(Some(entry.value.as_str()))
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
        let entry = self.get_string_or_insert_mut(key, "0")?;

        entry.increment(by)
    }

    pub fn decrement(&mut self, key: &str, by: i64) -> Result<i64, Error> {
        let entry = self.get_string_or_insert_mut(key, "0")?;

        entry.decrement(by)
    }

    pub fn set(
        &mut self,
        key: &str,
        value: String,
        overwrite_rule: Option<OverwriteRule>,
        get: bool,
        expire_rule: Option<ExpireRule>,
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

        let keep_ttl = matches!(expire_rule, Some(ExpireRule::KeepTTL));

        let old_value = if keep_ttl || get {
            self.remove_string(key).transpose()?
        } else {
            self.inner.remove(key);
            None
        };

        let mut expires_at = expire_rule.as_ref().and_then(|r| r.calculate_expire_time());

        if let Some(old_value) = &old_value
            && keep_ttl
        {
            expires_at = old_value.expires_at;
        }

        self.inner.insert(
            key.to_string(),
            Entry::String(StringEntry::new(value, expires_at)),
        );

        match get {
            true => Ok(old_value.map(|entry| entry.value)),
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

    fn get_string_or_insert_mut(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<&mut StringEntry, Error> {
        if let None = self.inner.get(key) {
            self.inner.insert(
                key.to_string(),
                Entry::String(StringEntry::new(String::from(value), None)),
            );
        }

        self.get_string_mut(key)
            .expect("map entry should be populated after an insert")
    }

    fn get_list_or_insert_mut(&mut self, key: &str) -> Result<&mut VecDeque<String>, Error> {
        if let None = self.inner.get(key) {
            self.inner
                .insert(key.to_string(), Entry::List(Default::default()));
        }

        self.get_list_mut(key)
            .expect("map entry should be populated after an insert")
    }

    fn get_string(&self, key: &str) -> Option<Result<&StringEntry, Error>> {
        match self.inner.get(key)? {
            Entry::String(string_entry) => Some(Ok(string_entry)),
            Entry::List(_) => Some(Err(Error::WrongType)),
        }
    }

    fn get_string_mut(&mut self, key: &str) -> Option<Result<&mut StringEntry, Error>> {
        match self.inner.get_mut(key)? {
            Entry::String(string_entry) => Some(Ok(string_entry)),
            Entry::List(_) => Some(Err(Error::WrongType)),
        }
    }

    fn get_list(&self, key: &str) -> Option<Result<&VecDeque<String>, Error>> {
        match self.inner.get(key)? {
            Entry::String(_) => Some(Err(Error::WrongType)),
            Entry::List(list) => Some(Ok(list)),
        }
    }
    fn get_list_mut(&mut self, key: &str) -> Option<Result<&mut VecDeque<String>, Error>> {
        match self.inner.get_mut(key)? {
            Entry::String(_) => Some(Err(Error::WrongType)),
            Entry::List(list) => Some(Ok(list)),
        }
    }

    fn remove_string(&mut self, key: &str) -> Option<Result<StringEntry, Error>> {
        match self.inner.get(key)? {
            Entry::String(_) => {}
            _ => return Some(Err(Error::WrongType)),
        };
        let old = self
            .inner
            .remove(key)
            .expect("map entry should exist after a get call");
        match old {
            Entry::String(string_entry) => Some(Ok(string_entry)),
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
        }
    }
}

impl error::Error for Error {}

struct StringEntry {
    value: String,
    expires_at: Option<SystemTime>,
}

impl StringEntry {
    fn new(value: String, expires_at: Option<SystemTime>) -> Self {
        Self { value, expires_at }
    }

    fn increment(&mut self, by: i64) -> Result<i64, Error> {
        let mut integer_value: i64 = self.value.parse().map_err(|err| Error::NoInteger)?;
        integer_value = integer_value
            .checked_add(by)
            .ok_or_else(|| Error::NoInteger)?;
        self.value = integer_value.to_string();

        Ok(integer_value)
    }

    fn decrement(&mut self, by: i64) -> Result<i64, Error> {
        let mut integer_value: i64 = self.value.parse().map_err(|err| Error::NoInteger)?;
        integer_value = integer_value
            .checked_sub(by)
            .ok_or_else(|| Error::NoInteger)?;
        self.value = integer_value.to_string();

        Ok(integer_value)
    }
}
