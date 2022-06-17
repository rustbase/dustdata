use std::sync::{Mutex, Arc};
use std::path;
use std::mem;

use crate::utils;
#[derive(Clone)]
/// A LSM configuration
/// # Arguments
/// * `flush_threshold` - The number of bytes to flush before flushing to disk
pub struct LsmConfig {
    pub flush_threshold: usize,
    pub sstable_path: String,
}
pub struct Lsm {
    memtable: Arc<Mutex<rbtree::RBTree<String, bson::Document>>>,
    pub memtable_size: usize,
    pub lsm_config: LsmConfig,
}

impl Lsm {
    pub fn new(config: LsmConfig) -> Lsm {
        Lsm {
            memtable: Arc::new(Mutex::new(rbtree::RBTree::new())),
            lsm_config: config,
            memtable_size: 0,
        }
    }

    pub fn insert(&mut self, key: &str, value: bson::Document) -> Result<(), &str> {
        if self.get(key).is_some() {
            return Err("Key already exists");
        }

        self.memtable_size += mem::size_of_val(&value);
        self.memtable.lock().unwrap().insert(key.to_string(), value);

        if self.memtable_size >= self.lsm_config.flush_threshold {
            self.flush();
        }
        
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<bson::Document> {
        let document = self.memtable.lock().unwrap();

        match document.get(&key.to_string()) {
            Some(document) => Some(document.clone()),
            None => {
                // Search on sstable
                todo!();
            },
        }
    }

    pub fn delete(&mut self, key: &str) {
        self.memtable.lock().unwrap().remove(&key.to_string());
    }

    pub fn flush(&mut self) {
        todo!();
    }

    pub fn get_memtable(&self) -> rbtree::RBTree<String, bson::Document> {
        self.memtable.lock().unwrap().clone()
    }

}

#[cfg(test)]
mod lsm_tests {
    use super::*;

    #[test]
    pub fn create_lsm() {
        let lsm = Lsm::new(LsmConfig {
            flush_threshold: 128, // 128 bytes
            sstable_path: "./test_data/sstable".to_string(),
        });
    }
}