mod bloom;
mod dustdata;
mod storage;

pub use self::{dustdata::DustData, dustdata::DustDataConfig, dustdata::LsmConfig, dustdata::Size};

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

    #[test]
    fn insert_document() {
        let config = DustDataConfig {
            path: "./test_data".to_string(),
            lsm_config: LsmConfig {
                flush_threshold: Size::Megabytes(128),
            },
        };

        let mut dd = initialize(config);

        dd.insert(
            "test",
            bson::doc! {
                "test": "test"
            },
        )
        .unwrap();

        assert!(dd.get("test").is_some());
    }
}
