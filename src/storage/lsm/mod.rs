use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::ops::Deref;
use std::path;
use std::sync::{Arc, Mutex};

use crate::bloom::BloomFilter;

mod filter;
mod index;
mod sstable;
mod writer;

#[derive(Clone)]
pub struct LsmConfig {
    pub flush_threshold: usize,
    pub sstable_path: String,
}

#[derive(Clone)]
pub struct Lsm {
    pub memtable: Arc<Mutex<BTreeMap<String, bson::Document>>>,
    pub memtable_size: usize,
    pub lsm_config: LsmConfig,
    pub dense_index: Arc<Mutex<HashMap<String, String>>>,
    pub bloom_filter: Arc<Mutex<BloomFilter>>,
}

impl Lsm {
    pub fn new(config: LsmConfig) -> Lsm {
        let bloom_rate = 0.01;

        let index = if index::check_if_index_exists(&config.sstable_path) {
            index::read_index(&config.sstable_path)
        } else {
            HashMap::new()
        };

        let bloom_filter = if filter::check_if_filter_exists(&config.sstable_path) {
            filter::read_filter(&config.sstable_path)
        } else {
            BloomFilter::new(bloom_rate, 100000)
        };

        if !path::Path::new(&config.sstable_path).exists() {
            std::fs::create_dir_all(&config.sstable_path).unwrap();
        }

        Lsm {
            memtable: Arc::new(Mutex::new(BTreeMap::new())),
            bloom_filter: Arc::new(Mutex::new(bloom_filter)),
            dense_index: Arc::new(Mutex::new(index)),
            lsm_config: config,
            memtable_size: 0, // The current memtable size (in bytes)
        }
    }

    pub fn insert(&mut self, key: &str, value: bson::Document) -> Result<(), &str> {
        if self.contains(key) {
            return Err("Key already exists");
        }

        self.memtable_size += mem::size_of_val(&value);
        self.memtable.lock().unwrap().insert(key.to_string(), value);
        self.bloom_filter.lock().unwrap().insert(key);

        if self.memtable_size >= self.lsm_config.flush_threshold {
            self.flush();
        }

        self.update_index();

        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<bson::Document> {
        if !self.contains(key) {
            return None;
        }

        let document = self.memtable.lock().unwrap();

        match document.get(&key.to_string()) {
            Some(document) => Some(document.clone()),
            None => {
                let dense_index = self.dense_index.lock().unwrap();
                let offset = dense_index.get(&key.to_string()).unwrap();
                sstable::Segment::read_with_offset(
                    offset.to_string(),
                    self.lsm_config.sstable_path.to_string(),
                )
            }
        }
    }

    pub fn delete(&mut self, key: &str) -> Result<(), &str> {
        if !self.contains(key) {
            return Err("Key does not exist");
        }

        if self.memtable.lock().unwrap().contains_key(&key.to_string()) {
            self.memtable.lock().unwrap().remove(&key.to_string());
        } else {
            self.dense_index.lock().unwrap().remove(&key.to_string());
            self.bloom_filter.lock().unwrap().delete(key);
        }
        Ok(())
    }

    pub fn update(&mut self, key: &str, value: bson::Document) -> Result<(), &str> {
        if !self.contains(key) {
            return Err("Key does not exist");
        }

        self.delete(key).unwrap();
        self.insert(key, value).unwrap();

        Ok(())
    }

    pub fn flush(&mut self) {
        let segment = sstable::Segment::from_tree(
            &self.get_memtable(),
            self.lsm_config.sstable_path.as_str(),
        );

        for token in segment.1 {
            self.dense_index.lock().unwrap().insert(token.0, token.1);
        }

        self.memtable.lock().unwrap().clear();
        self.memtable_size = 0;
    }

    pub fn get_memtable(&self) -> BTreeMap<String, bson::Document> {
        self.memtable.lock().unwrap().clone()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.bloom_filter.lock().unwrap().contains(key)
    }

    pub fn clear(&self) {
        self.memtable.lock().unwrap().clear();
        self.dense_index.lock().unwrap().clear();
    }

    pub fn update_index(&self) {
        let index = self.dense_index.lock().unwrap().clone();
        index::write_index(&self.lsm_config.sstable_path, &index);
    }
}

impl Drop for Lsm {
    fn drop(&mut self) {
        let memtable = self.memtable.lock().unwrap();

        if memtable.len() > 0 {
            let mut dense_index = self.dense_index.lock().unwrap();

            let segments = sstable::Segment::from_tree(
                memtable.deref(),
                self.lsm_config.sstable_path.as_str(),
            );

            for token in segments.1 {
                dense_index.insert(token.0, token.1);
            }

            index::write_index(&self.lsm_config.sstable_path, dense_index.deref());

            let mut keys = Vec::new();

            for segment in dense_index.deref() {
                keys.push(segment.0.clone());
            }

            filter::write_filter(
                &self.lsm_config.sstable_path,
                self.bloom_filter.lock().unwrap().deref(),
            );
        }
    }
}
