use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
};

static COMPRESSION_CONFIG: OnceLock<CompressionConfig> = OnceLock::new();
static DUSTDATA_CONFIG: OnceLock<DustDataConfig> = OnceLock::new();

pub(super) fn compression_config() -> &'static CompressionConfig {
    COMPRESSION_CONFIG.get_or_init(CompressionConfig::default)
}

pub(super) fn dustdata_config() -> &'static DustDataConfig {
    DUSTDATA_CONFIG.get_or_init(DustDataConfig::default)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenOptions {
    /// Open the database in read-only mode.
    ReadOnly,
    /// Open the database in read-write mode.
    ReadWrite,
}

#[derive(Debug, Clone)]
pub struct DustDataConfig {
    pub data_path: PathBuf,
    pub open_options: OpenOptions,
}

impl Default for DustDataConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl DustDataConfig {
    pub fn new() -> Self {
        Self {
            data_path: PathBuf::from("./data"),
            open_options: OpenOptions::ReadWrite,
        }
    }

    /// The path to the data directory.
    /// Default: ./data
    pub fn data_path<P: AsRef<Path>>(&mut self, data_path: P) -> &mut Self {
        self.data_path = data_path.as_ref().to_path_buf();
        self
    }

    /// The open options for the database.
    /// Default: OpenOptions::ReadWrite
    /// This is the mode in which the database is opened.
    pub fn open_options(&mut self, open_options: OpenOptions) -> &mut Self {
        self.open_options = open_options;
        self
    }

    pub fn build(self) {
        DUSTDATA_CONFIG.set(self).unwrap();
    }
}

#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// The compression level.
    ///
    /// The integer here is typically on a scale of 0-9 where 0 means "no
    /// compression" and 9 means "take as long as you'd like".
    pub level: u32,
    /// Enable compression
    pub enabled: bool,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressionConfig {
    pub fn new() -> Self {
        Self {
            level: 6,
            enabled: false,
        }
    }

    /// The compression level.
    ///
    /// Default: 6
    pub fn level(&mut self, level: u32) -> &mut Self {
        self.level = level;
        self
    }

    /// Enable compression
    ///
    /// Default: true
    pub fn enabled(&mut self, enabled: bool) -> &mut Self {
        self.enabled = enabled;
        self
    }

    pub fn build(self) {
        COMPRESSION_CONFIG.set(self).unwrap();
    }
}
