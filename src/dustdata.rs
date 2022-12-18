use super::storage;

use std::ops::Add;
use std::path;
use storage::lsm;

/// A LSM configuration
/// # Arguments
/// * `flush_threshold` - The number of bytes to flush before flushing to disk
/// * `detect_exit_signals` - Whether or not to detect exit signals (SIGTERM, SIGHUP, etc.)
#[derive(Clone)]
pub struct LsmConfig {
    pub flush_threshold: Size,
    pub detect_exit_signals: bool,
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
        fn calc(a: usize, b: usize) -> Size {
            let mut bytes = a + b;

            let mut gigabytes = 0;
            let mut megabytes = 0;
            let mut kilobytes = 0;

            while bytes >= 1024 * 1024 * 1024 {
                gigabytes += 1;
                bytes -= 1024 * 1024 * 1024;
            }

            while bytes >= 1024 * 1024 {
                megabytes += 1;
                bytes -= 1024 * 1024;
            }

            while bytes >= 1024 {
                kilobytes += 1;
                bytes -= 1024;
            }

            if gigabytes > 0 {
                Size::Gigabytes(gigabytes)
            } else if megabytes > 0 {
                Size::Megabytes(megabytes)
            } else if kilobytes > 0 {
                Size::Kilobytes(kilobytes)
            } else {
                Size::Bytes(bytes)
            }
        }

        match (self, rhs) {
            (Size::Bytes(a), Size::Bytes(b)) => calc(a, b),
            (Size::Bytes(a), Size::Kilobytes(b)) => calc(a, b * 1024),
            (Size::Bytes(a), Size::Megabytes(b)) => calc(a, b * 1024 * 1024),
            (Size::Bytes(a), Size::Gigabytes(b)) => calc(a, b * 1024 * 1024 * 1024),

            (Size::Kilobytes(a), Size::Bytes(b)) => calc(a * 1024, b * 1024),
            (Size::Kilobytes(a), Size::Kilobytes(b)) => calc(a * 1024, b * 1024),
            (Size::Kilobytes(a), Size::Megabytes(b)) => calc(a * 1024, b * 1024 * 1024),
            (Size::Kilobytes(a), Size::Gigabytes(b)) => calc(a * 1024, b * 1024 * 1024 * 1024),

            (Size::Megabytes(a), Size::Bytes(b)) => calc(a * 1024 * 1024, b),
            (Size::Megabytes(a), Size::Kilobytes(b)) => calc(a * 1024 * 1024, b * 1024),
            (Size::Megabytes(a), Size::Megabytes(b)) => calc(a * 1024 * 1024, b * 1024 * 1024),
            (Size::Megabytes(a), Size::Gigabytes(b)) => {
                calc(a * 1024 * 1024, b * 1024 * 1024 * 1024)
            }

            (Size::Gigabytes(a), Size::Bytes(b)) => calc(a * 1024 * 1024 * 1024, b),
            (Size::Gigabytes(a), Size::Kilobytes(b)) => calc(a * 1024 * 1024 * 1024, b * 1024),
            (Size::Gigabytes(a), Size::Megabytes(b)) => {
                calc(a * 1024 * 1024 * 1024, b * 1024 * 1024)
            }
            (Size::Gigabytes(a), Size::Gigabytes(b)) => {
                calc(a * 1024 * 1024 * 1024, b * 1024 * 1024 * 1024)
            }
        }
    }
}

pub fn size_to_usize(size: Size) -> usize {
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
            flush_threshold: size_to_usize(configuration.clone().lsm_config.flush_threshold),
            sstable_path: path.to_str().unwrap().to_string(),
        });

        if configuration.lsm_config.detect_exit_signals {
            lsm.handle_ctrlc();
        }

        Self {
            lsm,
            config: configuration,
        }
    }

    /// Get a value with a key
    /// # Arguments
    /// - `key`: a key to search for
    /// # Returns
    /// - `Result<Option<(bson::Bson)>>` if value was found returns a bson document
    pub fn get(&self, key: &str) -> Result<Option<bson::Bson>> {
        self.lsm.get(key)
    }

    /// Insert a value with a key
    /// # Arguments
    /// - `key`: a key.
    /// - `document`: a bson document to insert
    pub fn insert(&mut self, key: &str, bson: bson::Bson) -> Result<()> {
        self.lsm.insert(key, bson)
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
    pub fn update(&mut self, key: &str, bson: bson::Bson) -> Result<()> {
        self.lsm.update(key, bson)
    }

    /// Check if key exists.
    /// # Arguments
    /// - `key`: a key to check if exists.
    pub fn contains(&mut self, key: &str) -> bool {
        self.lsm.contains(key)
    }

    pub fn list_keys(&self) -> Result<Vec<String>> {
        Ok(self.lsm.list_keys())
    }
}

#[cfg(test)]
mod size_tests {
    use super::*;

    #[test]
    fn add_impl_bytes() {
        let size = Size::Bytes(1);
        let size2 = Size::Bytes(2);
        assert_eq!(size + size2, Size::Bytes(3));
    }

    #[test]
    fn add_impl_gb() {
        let size = Size::Gigabytes(1);
        let size2 = Size::Gigabytes(2);
        assert_eq!(size + size2, Size::Gigabytes(3));
    }

    #[test]
    fn add_impl_mb() {
        let size = Size::Megabytes(1);
        let size2 = Size::Megabytes(2);
        assert_eq!(size + size2, Size::Megabytes(3));
    }

    #[test]
    fn add_impl_kb() {
        let size = Size::Kilobytes(1);
        let size2 = Size::Kilobytes(2);
        assert_eq!(size + size2, Size::Kilobytes(3));
    }
}
