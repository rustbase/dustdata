use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct DustDataConfig {
    pub wal: WALConfig,
    pub data_path: PathBuf,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone)]
pub struct WALConfig {
    pub log_path: PathBuf,
    pub max_log_size: u64,
}

impl Default for DustDataConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl DustDataConfig {
    pub fn new() -> Self {
        Self {
            wal: WALConfig::new(),
            data_path: PathBuf::from("./data"),
            storage: StorageConfig::new(),
        }
    }

    /// The path to the data directory.
    /// Default: ./data
    pub fn data_path<P: AsRef<Path>>(&mut self, data_path: P) -> &mut Self {
        self.data_path = data_path.as_ref().to_path_buf();
        self
    }

    /// The storage configuration.
    /// Default: StorageConfig::new()
    pub fn storage(&mut self, storage: StorageConfig) -> &mut Self {
        self.storage = storage;
        self
    }

    pub fn build(&self) -> Self {
        self.clone()
    }
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub max_data_chunk_size: usize,
    pub max_data_chunks: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageConfig {
    pub fn new() -> Self {
        Self {
            max_data_chunk_size: 10 * 1028 * 1028, // 10MB
            max_data_chunks: 10,
        }
    }

    /// The maximum size of a data chunk.
    /// Default: 10MB
    pub fn max_data_chunk_size(&mut self, max_data_chunk_size: usize) -> &mut Self {
        self.max_data_chunk_size = max_data_chunk_size;
        self
    }

    /// The maximum number of data chunks.
    /// Default: 10
    pub fn max_data_chunks(&mut self, max_data_chunks: usize) -> &mut Self {
        self.max_data_chunks = max_data_chunks;
        self
    }
}

impl Default for WALConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl WALConfig {
    pub fn new() -> Self {
        Self {
            log_path: PathBuf::from("./log"),
            max_log_size: 5 * 1024 * 1024, // 5MB
        }
    }

    /// The path to the log file relative to the data directory.
    /// Default: <data_path>/log
    pub fn log_path<P: AsRef<Path>>(&mut self, log_path: P) -> &mut Self {
        self.log_path = log_path.as_ref().to_path_buf();
        self
    }

    /// The maximum size of the log file.
    /// Default: 5MB
    /// This is the maximum size of the log file before it is rotated.
    pub fn max_log_size(&mut self, max_log_size: u64) -> &mut Self {
        self.max_log_size = max_log_size;
        self
    }
}
