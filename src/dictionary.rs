use crate::parser::command::{ExpireRule, OverwriteRule};
use std::fmt::{Display, Formatter};
use std::{collections::HashMap, error, time::SystemTime};

pub struct Dictionary<V> {
    inner: HashMap<Vec<u8>, Entry<V>>,
}
impl<V> Dictionary<V> {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        self.inner
            .get(key)
            .map(|value| match value.expires_at {
                Some(t) if t > SystemTime::now() => Some(&value.value),
                Some(_) => None,
                None => Some(&value.value),
            })
            .flatten()
    }
    pub fn set(
        &mut self,
        key: Vec<u8>,
        value: V,
        overwrite_rule: Option<OverwriteRule>,
        get: bool,
        expire_rule: Option<ExpireRule>,
    ) -> Result<Option<V>, Error> {
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
            if let Some(ExpireRule::KEEPTTL) = expire_rule {
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

struct Entry<V> {
    value: V,
    expires_at: Option<SystemTime>,
}

impl<V> Entry<V> {
    fn new(value: V, expires_at: Option<SystemTime>) -> Self {
        Self { value, expires_at }
    }
}
