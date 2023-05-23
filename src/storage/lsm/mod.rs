use std::ops::Deref;
use std::{mem, path};

pub mod error;
pub mod filter;
pub mod index;
pub mod logging;
pub mod memtable;
pub mod sstable;

use error::{Error, ErrorKind, Result};

#[derive(Clone, Debug)]
pub struct LsmConfig {
    pub flush_threshold: usize,
    pub sstable_path: path::PathBuf,
}

#[derive(Clone)]
pub struct Lsm {
    pub memtable: memtable::Memtable,
    pub lsm_config: LsmConfig,
    pub dense_index: index::Index,
    pub bloom_filter: filter::Filter,
    pub sstable: sstable::SSTable,
    pub logging: logging::Logging,
}

impl Lsm {
    pub fn new(lsm_config: LsmConfig) -> Lsm {
        let dense_index = index::Index::new(lsm_config.clone().sstable_path);
        let sstable = sstable::SSTable::new(lsm_config.clone().sstable_path);
        let bloom_filter = filter::Filter::new(lsm_config.clone().sstable_path);
        let logging = logging::Logging::new(lsm_config.clone().sstable_path);
        let memtable = memtable::Memtable::new();

        Lsm {
            memtable,
            bloom_filter,
            dense_index,
            sstable,
            lsm_config,
            logging,
        }
    }

    pub fn insert(&mut self, key: &str, value: bson::Bson) -> Result<bson::Bson> {
        if self.contains(key) {
            return Err(Error::new(ErrorKind::AlreadyExists));
        }

        self.logging.insert(key, value.clone());
        self.memtable.insert(key, value.clone())?;
        self.bloom_filter.insert(key);

        if mem::size_of_val(&self.memtable.table) >= self.lsm_config.flush_threshold {
            self.flush().unwrap();
        }

        Ok(value)
    }

    pub fn get(&self, key: &str) -> Result<Option<bson::Bson>> {
        if !self.contains(key) {
            return Ok(None);
        }

        match self.memtable.get(key) {
            Some(document) => Ok(Some(document)),
            None => {
                let dense_index = self.dense_index.index.read().unwrap();
                let (file_index, offset) = dense_index.get(&key.to_string()).unwrap();

                self.sstable.get(file_index, offset)
            }
        }
    }

    pub fn delete(&mut self, key: &str) -> Result<bson::Bson> {
        if !self.contains(key) {
            return Err(Error::new(ErrorKind::KeyNotFound));
        }

        let value = self.get(key).unwrap().unwrap();
        self.logging.delete(key, value.clone());
        self.memtable.delete(key).ok();
        self.dense_index
            .index
            .write()
            .unwrap()
            .remove(&key.to_string());
        self.bloom_filter.delete(key);

        Ok(value)
    }

    pub fn update(&mut self, key: &str, value: bson::Bson) -> Result<bson::Bson> {
        if !self.contains(key) {
            return Err(Error::new(ErrorKind::KeyNotFound));
        }

        let old_value = self.get(key)?;

        self.logging
            .update(key, old_value.clone().unwrap(), value.clone());

        self.memtable
            .table
            .write()
            .unwrap()
            .insert(key.to_string(), value);

        Ok(old_value.unwrap())
    }

    pub fn flush(&mut self) -> Result<()> {
        if !self.memtable.is_empty() {
            let memtable = self.memtable.get_memtable();
            let segments = sstable::Segment::from_tree(&memtable);

            let file_index = self.sstable.write_segment_file(segments.0).unwrap();

            let mut dense_index = self.dense_index.index.write().unwrap();

            for (key, offset) in segments.1 {
                dense_index.insert(key.to_string(), (file_index, offset));
            }

            drop(dense_index);

            self.memtable.clear();
        }

        self.dense_index.write_index();
        self.bloom_filter.flush();
        self.logging.flush();

        Ok(())
    }

    pub fn rollback(&mut self, offset: u32) -> Result<()> {
        let reverse_ops = self.logging.rollback(offset);

        for reverse_op in reverse_ops {
            self.execute_logging_op(reverse_op)?;
        }

        Ok(())
    }

    pub fn contains(&self, key: &str) -> bool {
        self.bloom_filter.contains(key)
    }

    pub fn clear(&mut self) {
        self.memtable.clear();
        self.dense_index.index.write().unwrap().clear();
        self.bloom_filter.clear();
    }

    pub fn list_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();

        for key in self.memtable.table.read().unwrap().keys() {
            keys.push(key.clone());
        }

        for key in self.dense_index.index.read().unwrap().keys() {
            keys.push(key.clone());
        }

        keys
    }

    fn execute_logging_op(&mut self, op: logging::LogOp) -> Result<()> {
        match op {
            logging::LogOp::Insert { key, value } => self.insert(&key, value),
            logging::LogOp::Delete { key, value: _ } => self.delete(&key),
            logging::LogOp::Update {
                key,
                old_value: _,
                new_value,
            } => self.update(&key, new_value),
        }?;

        Ok(())
    }
}

impl Drop for Lsm {
    fn drop(&mut self) {
        if !self.memtable.is_empty() {
            let memtable = self.memtable.table.read().unwrap();

            let segments = sstable::Segment::from_tree(memtable.deref());
            let file_index = self.sstable.write_segment_file(segments.0).unwrap();

            let mut dense_index = self.dense_index.index.write().unwrap();

            for (key, offset) in segments.1 {
                dense_index.insert(key.to_string(), (file_index, offset));
            }

            drop(dense_index);
        }

        self.dense_index.write_index();
        self.bloom_filter.flush();
    }
}
