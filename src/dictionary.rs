use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};
use crate::parser::command::{ExpireRule, OverwriteRule};

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
    ) -> Option<V> {
        let can_set = match overwrite_rule {
            Some(OverwriteRule::NotExists) => !self.inner.contains_key(&key),
            Some(OverwriteRule::Exists) => self.inner.contains_key(&key),
            None => true,
        };
        if can_set {
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
            if get {
                return old.map(|e| e.value);
            }
        }
        None
    }
}
struct Entry<V> {
    value: V,
    expires_at: Option<SystemTime>,
}

impl<V> Entry<V> {
    fn new(value: V, expires_at: Option<SystemTime>) -> Self {
        Self { value, expires_at }
    }
}

