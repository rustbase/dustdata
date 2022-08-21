use super::storage;

use std::ops::Add;
use std::path;
use storage::lsm;

/// A LSM configuration
/// # Arguments
/// * `flush_threshold` - The number of bytes to flush before flushing to disk
#[derive(Clone)]
pub struct LsmConfig {
    pub flush_threshold: Size,
}

/// A DustData configuration
/// # Arguments
/// * `path` - The path to the data directory
/// * `cache_size` - The size of the cache (in bytes)
/// * `lsm_config` - The LSM configuration
#[derive(Clone)]
pub struct DustDataConfig {
    pub path: String,
    pub lsm_config: LsmConfig,
}

pub struct DustData {
    pub config: DustDataConfig,
    pub lsm: storage::lsm::Lsm,
}

#[derive(Clone)]
pub enum Size {
    Bytes(usize),
    Kilobytes(usize),
    Megabytes(usize),
    Gigabytes(usize),
}

impl Add for Size {
    type Output = Size;

    fn add(self, rhs: Self) -> Self::Output {
        match self {
            Size::Bytes(lhs) => match rhs {
                Size::Bytes(rhs) => Size::Bytes(lhs + rhs),
                Size::Kilobytes(rhs) => Size::Bytes(lhs + (rhs * 1024)),
                Size::Megabytes(rhs) => Size::Bytes(lhs + (rhs * 1024 * 1024)),
                Size::Gigabytes(rhs) => Size::Bytes(lhs + (rhs * 1024 * 1024 * 1024)),
            },
            Size::Kilobytes(lhs) => match rhs {
                Size::Bytes(rhs) => Size::Bytes(lhs + (rhs * 1024)),
                Size::Kilobytes(rhs) => Size::Kilobytes(lhs + rhs),
                Size::Megabytes(rhs) => Size::Kilobytes(lhs + (rhs * 1024)),
                Size::Gigabytes(rhs) => Size::Kilobytes(lhs + (rhs * 1024 * 1024)),
            },
            Size::Megabytes(lhs) => match rhs {
                Size::Bytes(rhs) => Size::Bytes(lhs + (rhs * 1024 * 1024)),
                Size::Kilobytes(rhs) => Size::Bytes(lhs + (rhs * 1024 * 1024 * 1024)),
                Size::Megabytes(rhs) => Size::Megabytes(lhs + rhs),
                Size::Gigabytes(rhs) => Size::Megabytes(lhs + (rhs * 1024)),
            },
            Size::Gigabytes(lhs) => match rhs {
                Size::Bytes(rhs) => Size::Bytes(lhs + (rhs * 1024 * 1024 * 1024)),
                Size::Kilobytes(rhs) => Size::Bytes(lhs + (rhs * 1024 * 1024 * 1024 * 1024)),
                Size::Megabytes(rhs) => Size::Bytes(lhs + (rhs * 1024 * 1024 * 1024 * 1024 * 1024)),
                Size::Gigabytes(rhs) => Size::Gigabytes(lhs + rhs),
            },
        }
    }
}

pub fn parse_size(size: Size) -> usize {
    match size {
        Size::Bytes(bytes) => bytes,
        Size::Kilobytes(kilobytes) => kilobytes * 1024,
        Size::Megabytes(megabytes) => megabytes * 1024 * 1024,
        Size::Gigabytes(gigabytes) => gigabytes * 1024 * 1024 * 1024,
    }
}

impl DustData {
    pub fn new(configuration: DustDataConfig) -> Self {
        let path = path::Path::new(&configuration.path);

        let lsm = storage::lsm::Lsm::new(lsm::LsmConfig {
            flush_threshold: parse_size(configuration.clone().lsm_config.flush_threshold),
            sstable_path: path.to_str().unwrap().to_string(),
        });

        Self {
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
        self.lsm.get(key)
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
