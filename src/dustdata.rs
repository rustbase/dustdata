use super::storage;

use std::path;
use std::sync::{Arc, Mutex};
use storage::lsm;

/// A LSM configuration
/// # Arguments
/// * `flush_threshold` - The number of bytes to flush before flushing to disk
#[derive(Clone)]
pub struct LsmConfig {
    pub flush_threshold: usize,
}

/// A DustData configuration
/// # Arguments
/// * `path` - The path to the data directory
/// * `cache_size` - The size of the cache (in bytes)
/// * `lsm_config` - The LSM configuration
#[derive(Clone)]
pub struct DustDataConfig {
    pub cache_size: usize,
    pub path: String,
    pub lsm_config: LsmConfig,
}

pub struct DustData {
    pub config: DustDataConfig,
    pub cache: Arc<Mutex<super::cache::Cache>>,
    pub lsm: storage::lsm::Lsm,
}

impl DustData {
    pub fn new(configuration: DustDataConfig) -> Self {
        let path = path::Path::new(&configuration.path);

        let cache = super::cache::Cache::new_app_cache(configuration.cache_size);

        let lsm = storage::lsm::Lsm::new(lsm::LsmConfig {
            flush_threshold: configuration.lsm_config.flush_threshold,
            sstable_path: path.to_str().unwrap().to_string(),
        });

        Self {
            cache,
            lsm,
            config: configuration,
        }
    }

    /// Get a value with a key
    /// # Arguments
    /// - `key`: a key to search for
    /// # Returns
    /// - `Some(bson::Document)` if value was found returns a bson document
    pub fn get(&mut self, key: &str) -> Option<bson::Document> {
        let cache = self.cache.lock().unwrap();
        let document = cache.get(key);

        if let Some(document) = document {
            return Some(document.result.as_document().unwrap().clone());
        }

        let document = self.lsm.get(key);

        if document.is_some() {
            self.cache.lock().unwrap().add(
                key.to_string(),
                bson::Bson::Document(document.as_ref().unwrap().clone()),
            );
        }

        document
    }

    /// Insert a value with a key
    /// # Arguments
    /// - `key`: a key.
    /// - `document`: a bson document to insert
    pub fn insert(&mut self, key: &str, document: bson::Document) -> Result<(), &str> {
        self.lsm.insert(key, document)
    }

    /// Delete a value with a key
    /// # Arguments
    /// - `key`: a key to search for and delete it.
    pub fn delete(&mut self, key: &str) -> Result<(), &str> {
        self.lsm.delete(key)
    }

    /// Update a value with a key
    /// # Arguments
    /// - `key`: a key to search for and update it.
    /// - `document`: the new document to replace the old one.
    pub fn update(&mut self, key: &str, document: bson::Document) -> Result<(), &str> {
        self.lsm.update(key, document)
    }
}
