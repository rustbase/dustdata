use super::error::{Error, ErrorKind, Result};
use std::collections::HashMap;

#[derive(Clone)]
pub struct Memtable {
    pub table: HashMap<String, bson::Bson>,
}

#[allow(clippy::new_without_default)]
impl Memtable {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: &str, value: bson::Bson) -> Result<Option<bson::Bson>> {
        if self.contains(key) {
            return Err(Error::new(ErrorKind::AlreadyExists));
        }

        self.table.insert(key.to_string(), value);

        Ok(None)
    }

    pub fn contains(&self, key: &str) -> bool {
        self.table.contains_key(key)
    }

    pub fn get(&self, key: &str) -> Option<bson::Bson> {
        self.table.get(key).cloned()
    }

    pub fn delete(&mut self, key: &str) -> Result<Option<bson::Bson>> {
        if !self.contains(key) {
            return Err(Error::new(ErrorKind::KeyNotFound));
        }

        let value = self.table.remove(key);

        Ok(value)
    }

    pub fn update(&mut self, key: &str, new_value: bson::Bson) -> Result<Option<bson::Bson>> {
        if !self.contains(key) {
            return Err(Error::new(ErrorKind::KeyNotFound));
        }

        let old_value = self.table.insert(key.to_string(), new_value);

        Ok(old_value)
    }

    pub fn clear(&mut self) {
        self.table.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn get_memtable(&self) -> HashMap<String, bson::Bson> {
        self.table.clone()
    }
}
