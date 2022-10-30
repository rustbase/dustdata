use logs::Method;
use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::ops::Deref;
use std::path;
use std::sync::{Arc, Mutex};

use crate::bloom::BloomFilter;
use crate::dd_println;
use crate::dustdata::{Error, ErrorCode, Result};

mod filter;
mod index;
mod logs;
mod sstable;
mod writer;

#[derive(Clone)]
pub struct LsmConfig {
    pub flush_threshold: usize,
    pub sstable_path: String,
    pub verbose: bool,
}

#[derive(Clone)]
pub struct Lsm {
    pub memtable: Arc<Mutex<BTreeMap<String, bson::Bson>>>,
    pub memtable_size: usize,
    pub lsm_config: LsmConfig,
    pub dense_index: Arc<Mutex<HashMap<String, String>>>,
    pub bloom_filter: Arc<Mutex<BloomFilter>>,
    pub logs: Arc<Mutex<logs::Logs>>,
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

        let logs = logs::Logs::new(config.clone().sstable_path);

        Lsm {
            memtable: Arc::new(Mutex::new(BTreeMap::new())),
            bloom_filter: Arc::new(Mutex::new(bloom_filter)),
            dense_index: Arc::new(Mutex::new(index)),
            lsm_config: config,
            logs: Arc::new(Mutex::new(logs)),
            memtable_size: 0, // The current memtable size (in bytes)
        }
    }

    pub fn handle_ctrlc(&mut self) {
        let c_mem = Arc::clone(&self.memtable);
        let c_den = Arc::clone(&self.dense_index);
        let c_config = self.lsm_config.clone();
        let c_bloom = Arc::clone(&self.bloom_filter);

        ctrlc::set_handler(move || {
            if c_config.verbose {
                dd_println!("Ctrl-C detected.");
            }

            let memtable = c_mem.lock().unwrap();
            let mut dense_index = c_den.lock().unwrap();

            if memtable.len() > 0 {
                if c_config.verbose {
                    dd_println!("Flushing memtable to disk...");
                }

                let segments =
                    sstable::Segment::from_tree(memtable.deref(), c_config.sstable_path.as_str());

                for token in segments.1 {
                    dense_index.insert(token.0, token.1);
                }

                let mut keys = Vec::new();

                for segment in dense_index.deref() {
                    keys.push(segment.0.clone());
                }
            } else if c_config.verbose {
                dd_println!("No data to flush.");
            }

            index::write_index(&c_config.sstable_path, dense_index.deref());
            filter::write_filter(&c_config.sstable_path, c_bloom.lock().unwrap().deref());

            std::process::exit(0);
        })
        .ok();
    }

    pub fn insert(&mut self, key: &str, value: bson::Bson) -> Result<()> {
        if self.contains(key) {
            return Err(Error {
                code: ErrorCode::KeyExists,
                message: "Key already exists".to_string(),
            });
        }

        self.memtable_size += mem::size_of_val(&value);

        let c_key = key.to_string();
        let c_value = value.clone();
        let logs = Arc::clone(&self.logs);
        std::thread::spawn(move || {
            logs.lock().unwrap().write(Method::Insert(c_key, c_value));
        });

        self.memtable.lock().unwrap().insert(key.to_string(), value);
        self.bloom_filter.lock().unwrap().insert(key);

        if self.memtable_size >= self.lsm_config.flush_threshold {
            self.flush();
        }

        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<bson::Bson>> {
        if !self.contains(key) {
            return Ok(None);
        }

        let memtable = self.memtable.lock().unwrap();

        match memtable.get(&key.to_string()) {
            Some(document) => Ok(Some(document.clone())),
            None => {
                let dense_index = self.dense_index.lock().unwrap();
                let offset = dense_index.get(&key.to_string()).unwrap();
                Ok(sstable::Segment::read_with_offset(
                    offset.to_string(),
                    self.lsm_config.sstable_path.to_string(),
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

        let mut memtable = self.memtable.lock().unwrap();

        let c_key = key.to_string();
        let logs = Arc::clone(&self.logs);
        std::thread::spawn(move || {
            logs.lock().unwrap().write(Method::Delete(c_key));
        });

        if memtable.contains_key(&key.to_string()) {
            memtable.remove(&key.to_string());

            drop(memtable);
        } else {
            self.dense_index.lock().unwrap().remove(&key.to_string());
        }

        self.bloom_filter.lock().unwrap().delete(key);

        Ok(())
    }

    pub fn update(&mut self, key: &str, value: bson::Bson) -> Result<()> {
        if !self.contains(key) {
            return Err(Error {
                code: ErrorCode::KeyNotExists,
                message: "Key does not exist".to_string(),
            });
        }

        let mut memtable = self.memtable.lock().unwrap();
        let mut bloom_filter = self.bloom_filter.lock().unwrap();

        let c_key = key.to_string();
        let c_value = value.clone();
        let logs = Arc::clone(&self.logs);
        std::thread::spawn(move || {
            logs.lock().unwrap().write(Method::Update(c_key, c_value));
        });

        // Delete the old value from the bloom filter
        bloom_filter.delete(key);

        let mut dense_index = self.dense_index.lock().unwrap();
        dense_index.remove(&key.to_string());
        drop(dense_index);

        memtable.insert(key.to_string(), value);
        drop(memtable);

        bloom_filter.insert(key);
        drop(bloom_filter);

        Ok(())
    }

    pub fn flush(&mut self) {
        if self.lsm_config.verbose {
            dd_println!("Flushing memtable to disk...");
        }

        let mut dense_index = self.dense_index.lock().unwrap();

        let segments = sstable::Segment::from_tree(
            &self.get_memtable(),
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

        drop(dense_index);

        filter::write_filter(
            &self.lsm_config.sstable_path,
            self.bloom_filter.lock().unwrap().deref(),
        );

        self.memtable.lock().unwrap().clear();
        self.memtable_size = 0;
    }

    pub fn get_memtable(&self) -> BTreeMap<String, bson::Bson> {
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

    pub fn list_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();

        for key in self.memtable.lock().unwrap().keys() {
            keys.push(key.clone());
        }

        for key in self.dense_index.lock().unwrap().keys() {
            keys.push(key.clone());
        }

        keys
    }

    pub fn drop(&mut self) {
        self.clear();
        self.bloom_filter.lock().unwrap().clear();
    }
}

impl Drop for Lsm {
    fn drop(&mut self) {
        let memtable = self.memtable.lock().unwrap();
        let mut dense_index = self.dense_index.lock().unwrap();

        if self.lsm_config.verbose {
            dd_println!("LSM is being dropped.");
        }

        if memtable.len() > 0 {
            if self.lsm_config.verbose {
                dd_println!("Flushing memtable to disk.");
            }

            let segments = sstable::Segment::from_tree(
                memtable.deref(),
                self.lsm_config.sstable_path.as_str(),
            );

            for token in segments.1 {
                dense_index.insert(token.0, token.1);
            }

            let mut keys = Vec::new();

            for segment in dense_index.deref() {
                keys.push(segment.0.clone());
            }
        } else if self.lsm_config.verbose {
            dd_println!("No memtable to flush to disk.");
        }

        index::write_index(&self.lsm_config.sstable_path, dense_index.deref());

        filter::write_filter(
            &self.lsm_config.sstable_path,
            self.bloom_filter.lock().unwrap().deref(),
        );
    }
}
