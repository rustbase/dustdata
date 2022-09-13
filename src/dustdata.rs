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
/// * `verbose` - Whether or not to print verbose output
/// * `path` - The path to the data directory
/// * `lsm_config` - The LSM configuration
#[derive(Clone)]
pub struct DustDataConfig {
    pub path: String,
    pub verbose: bool,
    pub lsm_config: LsmConfig,
}

pub struct DustData {
    pub config: DustDataConfig,
    pub lsm: storage::lsm::Lsm,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Size {
    Bytes(usize),
    Kilobytes(usize),
    Megabytes(usize),
    Gigabytes(usize),
}

impl Add for Size {
    type Output = Size;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Size::Bytes(a), Size::Bytes(b)) => Size::Bytes(a + b),
            (Size::Kilobytes(a), Size::Kilobytes(b)) => Size::Kilobytes(a + b),
            (Size::Megabytes(a), Size::Megabytes(b)) => Size::Megabytes(a + b),
            (Size::Gigabytes(a), Size::Gigabytes(b)) => Size::Gigabytes(a + b),
            _ => panic!("Cannot add two different sizes"),
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

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum ErrorCode {
    NotFound,
    KeyExists,
    KeyNotExists,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Code: {} - Message: {}", self.code, self.message)
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ErrorCode::NotFound => write!(f, "NotFound"),
            ErrorCode::KeyExists => write!(f, "KeyExists"),
            ErrorCode::KeyNotExists => write!(f, "KeyNotExists"),
        }
    }
}

impl DustData {
    pub fn new(configuration: DustDataConfig) -> Self {
        let path = path::Path::new(&configuration.path);

        let mut lsm = storage::lsm::Lsm::new(lsm::LsmConfig {
            verbose: configuration.verbose,
            flush_threshold: parse_size(configuration.clone().lsm_config.flush_threshold),
            sstable_path: path.to_str().unwrap().to_string(),
        });

        lsm.handle_ctrlc();

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
    pub fn get(&self, key: &str) -> Result<Option<bson::Document>> {
        self.lsm.get(key)
    }

    /// Insert a value with a key
    /// # Arguments
    /// - `key`: a key.
    /// - `document`: a bson document to insert
    pub fn insert(&mut self, key: &str, document: bson::Document) -> Result<()> {
        self.lsm.insert(key, document)
    }

    /// Delete a value with a key
    /// # Arguments
    /// - `key`: a key to search for and delete it.
    pub fn delete(&mut self, key: &str) -> Result<()> {
        self.lsm.delete(key)
    }

    /// Update a value with a key
    /// # Arguments
    /// - `key`: a key to search for and update it.
    /// - `document`: the new document to replace the old one.
    pub fn update(&mut self, key: &str, document: bson::Document) -> Result<()> {
        self.lsm.update(key, document)
    }

    /// Check if key exists.
    /// # Arguments
    /// - `key`: a key to check if exists.
    pub fn contains(&mut self, key: &str) -> bool {
        self.lsm.contains(key)
    }
}

#[cfg(test)]
mod size_tests {
    use super::*;

    #[test]
    fn add_impl_bytes() {
        let size = Size::Bytes(1);
        let size2 = Size::Bytes(2);
        let size3 = parse_size(size + size2);
        assert_eq!(size3, parse_size(Size::Bytes(3)));
    }

    #[test]
    fn add_impl_gb() {
        let size = Size::Gigabytes(1);
        let size2 = Size::Gigabytes(2);
        let size3 = parse_size(size + size2);
        assert_eq!(size3, parse_size(Size::Gigabytes(3)));
    }

    #[test]
    fn add_impl_mb() {
        let size = Size::Megabytes(1);
        let size2 = Size::Megabytes(2);
        let size3 = parse_size(size + size2);
        assert_eq!(size3, parse_size(Size::Megabytes(3)));
    }

    #[test]
    fn add_impl_kb() {
        let size = Size::Kilobytes(1);
        let size2 = Size::Kilobytes(2);
        let size3 = parse_size(size + size2);
        assert_eq!(size3, parse_size(Size::Kilobytes(3)));
    }
}
