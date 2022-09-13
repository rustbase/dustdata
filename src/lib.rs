mod bloom;
mod dustdata;
mod storage;
mod utils;

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

    fn get_default_config() -> DustDataConfig {
        DustDataConfig {
            verbose: false,
            path: "./test_data".to_string(),
            lsm_config: LsmConfig {
                flush_threshold: Size::Megabytes(128),
            },
        }
    }

    #[test]
    fn insert_document() {
        let config = get_default_config();

        let mut dd = initialize(config);

        dd.insert(
            "insert_doc",
            bson::bson!({
                "test": "test"
            }),
        )
        .unwrap();

        assert!(dd.get("insert_doc").unwrap().is_some());

        dd.delete("insert_doc").unwrap(); // delete the test document
    }

    #[test]
    fn update_document() {
        let config = get_default_config();

        let mut dd = initialize(config);
        dd.insert(
            "update_doc",
            bson::bson!({
                "test": "test"
            }),
        )
        .unwrap();

        dd.update(
            "update_doc",
            bson::bson! ({
                "test": "test2"
            }),
        )
        .unwrap();

        let get = dd.get("update_doc").unwrap().unwrap();
        let get = get.as_document().unwrap();

        let get = get.get("test").unwrap().as_str().unwrap();

        assert_eq!(get, "test2");

        dd.delete("update_doc").unwrap(); // delete the test document
    }
}
