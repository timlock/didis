use crate::parser::command::{ExpireRule, OverwriteRule};
use std::fmt::{Display, Formatter};
use std::{collections::HashMap, error, time::SystemTime};

pub struct Dictionary {
    inner: HashMap<String, Entry>,
}
impl Dictionary {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner
            .get(key)
            .map(|value| match value.expires_at {
                Some(t) if t > SystemTime::now() => Some(value.value.as_str()),
                Some(_) => None,
                None => Some(value.value.as_str()),
            })
            .flatten()
    }
    pub fn set(
        &mut self,
        key: String,
        value: String,
        overwrite_rule: Option<OverwriteRule>,
        get: bool,
        expire_rule: Option<ExpireRule>,
    ) -> Result<Option<String>, Error> {
        match overwrite_rule {
            Some(OverwriteRule::NotExists) if self.inner.contains_key(&key) => {
                return Err(Error::OverrideConflict)
            }
            Some(OverwriteRule::Exists) if !self.inner.contains_key(&key) => {
                return Err(Error::OverrideConflict)
            }
            _ => {}
        };

        let mut expires_at = expire_rule
            .as_ref()
            .map(|r| r.calculate_expire_time())
            .flatten();
        let old = self.inner.remove(&key);
        if let Some(ref old) = old {
            if let Some(ExpireRule::KeepTTL) = expire_rule {
                expires_at = old.expires_at;
            }
        }
        let entry = Entry::new(value, expires_at);
        self.inner.insert(key, entry);
        match get {
            true => Ok(old.map(|e| e.value)),
            false => Ok(None),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    OverrideConflict,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::OverrideConflict => {
                write!(f, "Override conflict")
            }
        }
    }
}

impl error::Error for Error {}

struct Entry {
    value: String,
    expires_at: Option<SystemTime>,
}

impl Entry {
    fn new(value: String, expires_at: Option<SystemTime>) -> Self {
        Self { value, expires_at }
    }
}
