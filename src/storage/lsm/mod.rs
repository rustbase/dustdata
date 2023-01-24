use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::ops::Deref;
use std::path;
use std::sync::{Arc, RwLock};

use crate::bloom::BloomFilter;
use crate::dustdata::{Error, ErrorCode, Result};

use self::snapshots::Snapshot;

pub mod filter;
pub mod index;
pub mod snapshots;
pub mod sstable;
mod writer;

#[derive(Clone, Debug)]
pub struct LsmConfig {
    pub flush_threshold: usize,
    pub sstable_path: path::PathBuf,
}

#[derive(Clone)]
pub struct Lsm {
    pub memtable: Arc<RwLock<BTreeMap<String, bson::Bson>>>,
    pub memtable_size: usize,
    pub lsm_config: LsmConfig,
    pub snapshots: snapshots::SnapshotManager,
    pub dense_index: Arc<RwLock<HashMap<String, String>>>,
    pub bloom_filter: Arc<RwLock<BloomFilter>>,
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

        let snapshots = snapshots::SnapshotManager::new(
            std::path::Path::new(&config.sstable_path).join("snapshots"),
        );

        Lsm {
            memtable: Arc::new(RwLock::new(BTreeMap::new())),
            bloom_filter: Arc::new(RwLock::new(bloom_filter)),
            dense_index: Arc::new(RwLock::new(index)),
            lsm_config: config,
            memtable_size: 0, // The current memtable size (in bytes)
            snapshots,
        }
    }

    pub fn insert(&mut self, key: &str, value: bson::Bson) -> Result<()> {
        if self.contains(key) {
            return Err(Error {
                code: ErrorCode::KeyExists,
                message: "Key already exists".to_string(),
            });
        }

        self.memtable_size += mem::size_of_val(&value);

        self.memtable
            .write()
            .unwrap()
            .insert(key.to_string(), value);
        self.bloom_filter.write().unwrap().insert(key);

        if self.memtable_size >= self.lsm_config.flush_threshold {
            self.flush().unwrap();
        }

        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<bson::Bson>> {
        if !self.contains(key) {
            return Ok(None);
        }

        let memtable = self.memtable.read().unwrap();

        match memtable.get(&key.to_string()) {
            Some(document) => Ok(Some(document.clone())),
            None => {
                let dense_index = self.dense_index.read().unwrap();
                let offset = dense_index.get(&key.to_string()).unwrap();
                Ok(sstable::Segment::read_with_offset(
                    offset.to_string(),
                    &self.lsm_config.sstable_path,
                ))
            }
        }
    }

    pub fn delete(&mut self, key: &str) -> Result<()> {
        if !self.contains(key) {
            return Err(Error {
                code: ErrorCode::KeyNotExists,
                message: "Key does not exist".to_string(),
            });
        }

        let mut memtable = self.memtable.write().unwrap();

        if memtable.contains_key(&key.to_string()) {
            memtable.remove(&key.to_string());

            drop(memtable);
        } else {
            self.dense_index.write().unwrap().remove(&key.to_string());
        }

        self.bloom_filter.write().unwrap().delete(key);

        Ok(())
    }

    pub fn update(&mut self, key: &str, value: bson::Bson) -> Result<()> {
        if !self.contains(key) {
            return Err(Error {
                code: ErrorCode::KeyNotExists,
                message: "Key does not exist".to_string(),
            });
        }

        let mut memtable = self.memtable.write().unwrap();
        let mut bloom_filter = self.bloom_filter.write().unwrap();

        // Delete the old value from the bloom filter
        bloom_filter.delete(key);

        let mut dense_index = self.dense_index.write().unwrap();
        dense_index.remove(&key.to_string());
        drop(dense_index);

        memtable.insert(key.to_string(), value);
        drop(memtable);

        bloom_filter.insert(key);
        drop(bloom_filter);

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        let memtable = self.get_memtable();

        if memtable.is_empty() {
            return Ok(());
        }

        let mut dense_index = self.dense_index.write().unwrap();

        let segments = sstable::Segment::from_tree(&memtable, &self.lsm_config.sstable_path);

        for token in segments.1 {
            dense_index.insert(token.0, token.1);
        }

        index::write_index(&self.lsm_config.sstable_path, dense_index.deref());

        let mut keys = Vec::new();

        for segment in dense_index.deref() {
            keys.push(segment.0.clone());
        }

        drop(dense_index);

        filter::write_filter(
            &self.lsm_config.sstable_path,
            self.bloom_filter.read().unwrap().deref(),
        );

        self.memtable.write().unwrap().clear();
        self.memtable_size = 0;

        Ok(())
    }

    pub fn get_memtable(&self) -> BTreeMap<String, bson::Bson> {
        self.memtable.read().unwrap().clone()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.bloom_filter.read().unwrap().contains(key)
    }

    pub fn clear(&self) {
        self.memtable.write().unwrap().clear();
        self.dense_index.write().unwrap().clear();
    }

    pub fn update_index(&self) {
        let index = self.dense_index.read().unwrap().clone();
        index::write_index(&self.lsm_config.sstable_path, &index);
    }

    pub fn list_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();

        for key in self.memtable.read().unwrap().keys() {
            keys.push(key.clone());
        }

        for key in self.dense_index.read().unwrap().keys() {
            keys.push(key.clone());
        }

        keys
    }

    pub fn drop(&mut self) {
        self.clear();
        self.bloom_filter.write().unwrap().clear();
    }

    pub fn load_snapshot(path: path::PathBuf, snapshot: Snapshot) {
        sstable::Segment::from_tree(snapshot.get_memtable(), &path);
        index::write_index(&path, snapshot.get_dense_index());
        filter::write_filter(&path, snapshot.get_bloom_filter());
    }
}

impl Drop for Lsm {
    fn drop(&mut self) {
        let memtable = self.memtable.read().unwrap();
        let mut dense_index = self.dense_index.write().unwrap();

        if memtable.is_empty() {
            return;
        }

        let segments = sstable::Segment::from_tree(memtable.deref(), &self.lsm_config.sstable_path);

        for token in segments.1 {
            dense_index.insert(token.0, token.1);
        }

        let mut keys = Vec::new();

        for segment in dense_index.deref() {
            keys.push(segment.0.clone());
        }

        index::write_index(&self.lsm_config.sstable_path, dense_index.deref());

        filter::write_filter(
            &self.lsm_config.sstable_path,
            self.bloom_filter.read().unwrap().deref(),
        );
    }
}
