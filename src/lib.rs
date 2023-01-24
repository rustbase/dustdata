mod bloom;
mod dustdata;
mod storage;

pub use self::{
    dustdata::DustData, dustdata::DustDataConfig, dustdata::Error, dustdata::ErrorCode,
    dustdata::LsmConfig, dustdata::Result, dustdata::Size, storage::lsm::snapshots,
};
pub use bson;

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
            path: std::path::Path::new("./test_data/dustdata").to_path_buf(),
            lsm_config: LsmConfig {
                flush_threshold: Size::Megabytes(128),
            },
        }
    }

    #[test]
    fn insert_document() {
        let config = get_default_config();

        let mut dd = initialize(config);

        let now = std::time::SystemTime::now();
        dd.insert(
            "insert_doc",
            bson::bson!({
                "test": "test"
            }),
        )
        .unwrap();
        println!("Insert took: {:?}", now.elapsed().unwrap());

        let now = std::time::SystemTime::now();
        assert!(dd.get("insert_doc").unwrap().is_some());
        println!("Get took: {:?}", now.elapsed().unwrap());

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

        let now = std::time::SystemTime::now();
        dd.update(
            "update_doc",
            bson::bson! ({
                "test": "test2"
            }),
        )
        .unwrap();
        println!("Update took: {:?}", now.elapsed().unwrap());

        let ls = dd.list_keys().unwrap();

        let get = dd.get("update_doc").unwrap().unwrap();
        let get = get.as_document().unwrap();

        let get = get.get("test").unwrap().as_str().unwrap();

        assert_eq!(get, "test2");
        assert!(ls.contains(&"update_doc".to_string()));

        dd.delete("update_doc").unwrap(); // delete the test document
    }

    #[test]
    fn reading_on_sstable() {
        let config = get_default_config();

        let mut dd = initialize(config);

        dd.insert(
            "read_sstable",
            bson::bson!({
                "test": "test"
            }),
        )
        .unwrap();

        // flush the sstable
        dd.flush().unwrap();

        let ls = dd.list_keys().unwrap();

        let now = std::time::SystemTime::now();
        let get = dd.get("read_sstable").unwrap().unwrap();
        println!("SSTable Get took: {:?}", now.elapsed().unwrap());
        let get = get.as_document().unwrap();

        let get = get.get("test").unwrap().as_str().unwrap();

        assert_eq!(get, "test");
        assert!(ls.contains(&"read_sstable".to_string()));

        dd.delete("read_sstable").unwrap(); // delete the test document
    }
}
