use crate::parser::command::{ExpireRule, OverwriteRule};
use std::fmt::{Display, Formatter};
use std::{collections::HashMap, error, time::SystemTime};

#[derive(Default)]
pub struct Dictionary {
    inner: HashMap<String, StringEntry>,
}
impl Dictionary {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
    pub fn get(&self, key: &str) -> Option<&str> {
        let entry = match self.inner.get(key) {
            Some(entry) => entry,
            None => return None,
        };

        let expires_at = match entry.expires_at {
            Some(expires_at) => expires_at,
            None => return Some(entry.value.as_str()),
        };

        if expires_at < SystemTime::now() {
            return None;
        }

        Some(entry.value.as_str())
    }

    pub fn delete(&mut self, key: &str) -> Option<String> {
        self.inner.remove(key).map(|entry| entry.value)
    }

    pub fn increment(&mut self, key: &str, by: i64) -> Result<i64, Error> {
        let entry = self.set_if_absent(key, StringEntry::new(String::from("0"), None));

        entry.increment(by)
    }

    pub fn decrement(&mut self, key: &str, by: i64) -> Result<i64, Error> {
        let entry = self.set_if_absent(key, StringEntry::new(String::from("0"), None));

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

        let mut expires_at = expire_rule
            .as_ref()
            .map(|r| r.calculate_expire_time())
            .flatten();
        let old = self.inner.remove(key);
        if let Some(ref old) = old {
            if let Some(ExpireRule::KeepTTL) = expire_rule {
                expires_at = old.expires_at;
            }
        }
        let entry = StringEntry::new(value, expires_at);
        self.inner.insert(key.to_owned(), entry);
        match get {
            true => Ok(old.map(|e| e.value)),
            false => Ok(None),
        }
    }

    fn set_if_absent(&mut self, key: &str, entry: StringEntry) -> &mut StringEntry {
        if let None = self.inner.get(key) {
            self.inner.insert(key.to_string(), entry);
        }

        self.inner
            .get_mut(key)
            .expect("map entry should be populated after an insert")
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
