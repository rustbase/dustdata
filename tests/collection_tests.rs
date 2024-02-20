use dustdata::DustData;

pub fn test_config() -> dustdata::DustDataConfig {
    dustdata::DustDataConfig::default()
        .data_path("./test_data")
        .build()
}

#[test]
pub fn collection_insert_operation() {
    let dustdata = DustData::new(test_config()).unwrap();
    let collection = dustdata.collection::<String>("insert_collection");

    collection
        .start_lazy(|t| {
            t.insert("key", "value".to_string());
        })
        .unwrap();

    let value = collection.get("key").unwrap().unwrap();

    assert_eq!(value, "value");
}

#[test]
pub fn collection_update_operation() {
    let dustdata = DustData::new(test_config()).unwrap();
    let collection = dustdata.collection::<String>("update_collection");

    collection
        .start_lazy(|t| {
            t.insert("key", "value".to_string());
        })
        .unwrap();

    collection
        .start_lazy(|t| {
            t.update("key", "new_value".to_string());
        })
        .unwrap();

    let value = collection.get("key").unwrap().unwrap();

    assert_eq!(value, "new_value");
}

#[test]
pub fn collection_delete_operation() {
    let dustdata = DustData::new(test_config()).unwrap();
    let collection = dustdata.collection::<String>("delete_collection");

    collection
        .start_lazy(|t| {
            t.insert("key", "value".to_string());
        })
        .unwrap();

    collection
        .start_lazy(|t| {
            t.delete("key");
        })
        .unwrap();

    let value = collection.get("key").unwrap();

    assert!(value.is_none());
}

#[test]
pub fn collection_revert_operation() {
    let dustdata = DustData::new(test_config()).unwrap();
    let collection = dustdata.collection::<String>("revert_operation_collection");

    let mut rolledback_transaction = collection
        .start_lazy(|t| {
            t.insert("key", "value".to_string());
        })
        .unwrap();

    collection
        .rollback_transaction(&mut rolledback_transaction)
        .unwrap();

    let value = collection.get("key").unwrap();

    assert!(value.is_none());
}
