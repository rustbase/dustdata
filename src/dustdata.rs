use super::storage;

use std::path;
use std::fs;
use storage::lsm;


/// A DustData configuration
/// # Arguments
/// * `path` - The path to the data directory
/// * `use_compression` - Whether to use compression
/// * `cache_size` - The size of the cache (in bytes)
#[derive(Clone)]
pub struct DustDataConfig {
    pub cache_size: usize,
    pub path: String,
    pub lsm_config: storage::lsm::LsmConfig,
}

pub struct DustData {
    pub config: DustDataConfig,
    pub cache: super::cache::Cache,
    pub lsm: storage::lsm::Lsm,
}

impl DustData {
    pub fn new(configuration: DustDataConfig) -> Self {
        let path = path::Path::new(&configuration.path);

        let cache = super::cache::Cache::new(configuration.cache_size);

        if !path.join("sstable").exists() {
            fs::create_dir_all(path.join("sstable")).unwrap();
        }

        Self {
            cache,
            lsm: storage::lsm::Lsm::new(configuration.clone().lsm_config),
            config: configuration,
        }
    }

    /// Get a value with a key
    /// # Arguments
    /// - `key`: a key to search for
    /// # Returns
    /// - `Some(bson::Document)` if value was found returns a bson document
    pub fn get(&mut self, key: &str) -> Option<bson::Document> {
        let document = self.cache.get(key);

        if let Some(document) = document {
            return Some(document.result.as_document().unwrap().clone());
        }

        let document = self.lsm.get(key);

        if document.is_some() {
            self.cache.add(key.to_string(), bson::Bson::Document(document.as_ref().unwrap().clone()));
        }

        document
    }

    /// Insert a value with a key
    /// # Arguments
    /// - `key`: a key.
    /// - `document`: a bson document to insert
    pub fn insert(&mut self, key: &str, document: bson::Document) {
        self.lsm.insert(key, document).unwrap();
    }

    /// Delete a value with a key
    /// # Arguments
    /// - `key`: a key to search for and delete it.
    pub fn delete(&mut self, key: &str) {
        self.lsm.delete(key);
    }

    /// Update a value with a key
    /// # Arguments
    /// - `key`: a key to search for and update it.
    /// - `document`: the new document to replace the old one.
    pub fn update(&mut self, key: &str, document: bson::Document) {
        self.lsm.update(key, document);
    }
}