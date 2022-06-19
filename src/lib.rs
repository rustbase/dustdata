mod dustdata;
mod cache;
mod storage;
mod utils;

/// Initialize the database
/// # Arguments
/// * `config` - A configuration object
/// # Returns
/// - DustData object
pub fn initialize(config: dustdata::DustDataConfig) -> dustdata::DustData {
    dustdata::DustData::new(config)
}

#[cfg(test)]
mod dustdata_tests {
    use super::*;

    fn get_default_config() -> dustdata::DustDataConfig {
        dustdata::DustDataConfig {
            path: "./test_data".to_string(),
            lsm_config: storage::lsm::LsmConfig {
                flush_threshold: 128,
                sstable_path: "./test_data/sstable".to_string(),
            },
            cache_size: 256,
        }
    }

    #[test]
    fn test_initialize() {
        initialize(get_default_config());
    }
}