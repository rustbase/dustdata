use std::collections::HashMap;
use chrono::prelude::*;

use std::sync::Arc;
use std::sync::Mutex;
use std::mem;

pub type AppCache = Arc<Mutex<Cache>>;

#[derive(Clone)]
pub struct CacheItem {
    pub result: bson::Bson,
    pub date: DateTime<Utc>,
    pub bytes_size: usize,
}
#[derive(Clone)]
pub struct Cache {
    items: HashMap<String, CacheItem>,
    pub max_size: usize,
    pub current_size: usize,
}

impl Cache {
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            items: HashMap::new(),
            current_size: 0,
        }
    }

    pub fn new_app_cache(max_size: usize) -> AppCache {
        Arc::new(Mutex::new(Cache::new(max_size)))
    }

    pub fn get(&self, key: &str) -> Option<&CacheItem> {
        self.items.get(key)
    }

    pub fn add(&mut self, key: String, value: bson::Bson) {
        let value_size = mem::size_of_val(&value);
        if value_size > self.max_size || self.current_size + value_size > self.max_size {
            panic!("Value too large");
        }

        if self.items.contains_key(&key) {
            panic!("Key already exists");
        }

        self.items.insert(key, CacheItem {
            result: value,
            date: Utc::now(),
            bytes_size: value_size,
        });
        self.current_size += value_size;
        self.drop(self.items.keys().next().unwrap().to_string());
    }

    pub fn drop(&mut self, key: String) {
        self.current_size -= self.items.get(&key).unwrap().bytes_size;
        self.items.remove(&key);
    }

    pub fn clear(&mut self) {
        self.current_size = 0;
        self.items.clear();
    }
}