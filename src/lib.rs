pub mod bloom;
pub mod storage;

pub use bson;

#[cfg(test)]
mod dustdata_tests {
    use super::*;

    use storage::lsm::{Lsm, LsmConfig};

    fn initialize() -> Lsm {
        let config = LsmConfig {
            flush_threshold: 100,
            sstable_path: std::path::PathBuf::from("./test_data"),
        };

        Lsm::new(config)
    }

    #[test]
    fn memtable_test() {
        let mut dd = initialize();

        let doc = bson::bson!({
            "name": "Pedro",
            "age": 20,
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "state": "NY",
                "zip": 10001
            },
        });

        dd.insert("user:1", doc).unwrap();

        assert!(dd.contains("user:1"));

        let user = dd.get("user:1").unwrap().unwrap();
        let user = user.as_document().unwrap();

        let name = user.get("name").unwrap().as_str().unwrap();
        assert_eq!(name, "Pedro");

        let age = user.get("age").unwrap().as_i32().unwrap();
        assert_eq!(age, 20);

        dd.delete("user:1").unwrap();
    }

    #[test]
    fn sstable_test() {
        let mut dd = initialize();

        let doc = bson::bson!({
            "name": "John",
            "age": 26,
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "state": "NY",
                "zip": 10001
            },
        });

        dd.insert("user:2", doc).unwrap();

        assert!(dd.contains("user:2"));

        dd.flush().unwrap();

        assert!(dd.contains("user:2"));

        let user = dd.get("user:2").unwrap().unwrap();
        let user = user.as_document().unwrap();

        let name = user.get("name").unwrap().as_str().unwrap();
        assert_eq!(name, "John");

        let age = user.get("age").unwrap().as_i32().unwrap();
        assert_eq!(age, 26);

        dd.delete("user:2").unwrap();
    }
}
