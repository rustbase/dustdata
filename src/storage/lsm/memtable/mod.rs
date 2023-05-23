use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use super::error::{Error, ErrorKind, Result};

#[derive(Clone)]
pub struct Memtable {
    pub table: Arc<RwLock<HashMap<String, bson::Bson>>>,
}

#[allow(clippy::new_without_default)]
impl Memtable {
    pub fn new() -> Self {
        Self {
            table: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn insert(&mut self, key: &str, value: bson::Bson) -> Result<()> {
        if self.contains(key) {
            return Err(Error::new(ErrorKind::AlreadyExists));
        }

        self.table.write().unwrap().insert(key.to_string(), value);

        Ok(())
    }

    pub fn contains(&self, key: &str) -> bool {
        self.table.read().unwrap().contains_key(key)
    }

    pub fn get(&self, key: &str) -> Option<bson::Bson> {
        self.table.read().unwrap().get(key).cloned()
    }

    pub fn delete(&mut self, key: &str) -> Result<Option<bson::Bson>> {
        if !self.contains(key) {
            return Err(Error::new(ErrorKind::KeyNotFound));
        }

        let value = self.table.write().unwrap().remove(key);

        Ok(value)
    }

    pub fn update(&mut self, key: &str, new_value: bson::Bson) -> Result<Option<bson::Bson>> {
        if !self.contains(key) {
            return Err(Error::new(ErrorKind::KeyNotFound));
        }

        let old_value = self
            .table
            .write()
            .unwrap()
            .insert(key.to_string(), new_value);

        Ok(old_value)
    }

    pub fn clear(&mut self) {
        self.table.write().unwrap().clear();
    }

    pub fn is_empty(&self) -> bool {
        self.table.read().unwrap().is_empty()
    }

    pub fn get_memtable(&self) -> HashMap<String, bson::Bson> {
        self.table.read().unwrap().clone()
    }
}
