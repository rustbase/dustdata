use dustdata::DustData;

pub fn test_config() -> dustdata::DustDataConfig {
    dustdata::DustDataConfig::default()
        .data_path("./test_data")
        .build()
}

#[test]
pub fn collection_insert_operation() {
    let dustdata = DustData::new(test_config()).unwrap();
    let collection = dustdata.collection::<String>("insert_and_get");

    let mut transaction = collection.start();
    transaction.insert("key", "value".to_string());
    collection.commit(&mut transaction).unwrap();

    let value = collection.get("key").unwrap().unwrap();

    assert_eq!(value, "value");
}

#[test]
pub fn collection_update_operation() {
    let dustdata = DustData::new(test_config()).unwrap();
    let collection = dustdata.collection::<String>("update");

    let mut transaction = collection.start();
    transaction.insert("key", "value".to_string());
    collection.commit(&mut transaction).unwrap();

    let mut transaction = collection.start();
    transaction.update("key", "new_value".to_string());
    collection.commit(&mut transaction).unwrap();

    let value = collection.get("key").unwrap().unwrap();

    assert_eq!(value, "new_value");
}

#[test]
pub fn collection_delete_operation() {
    let dustdata = DustData::new(test_config()).unwrap();
    let collection = dustdata.collection::<String>("remove");

    let mut transaction = collection.start();
    transaction.insert("key", "value".to_string());
    collection.commit(&mut transaction).unwrap();

    let mut transaction = collection.start();
    transaction.delete("key");
    collection.commit(&mut transaction).unwrap();

    let value = collection.get("key").unwrap();

    assert!(value.is_none());
}

#[test]
pub fn collection_revert_operation() {
    let dustdata = DustData::new(test_config()).unwrap();
    let collection = dustdata.collection::<String>("revert_operation_collection");

    let mut transaction = collection.start();
    transaction.insert("key", "value".to_string());
    collection.commit(&mut transaction).unwrap();

    let mut transaction = collection.start();
    transaction.update("key", "new_value".to_string());
    collection.commit(&mut transaction).unwrap();

    let mut transaction = collection.start();
    transaction.delete("key");
    collection.commit(&mut transaction).unwrap();

    let transaction = collection.rollback_transaction(&mut transaction).unwrap();

    let value = collection.get("key").unwrap().unwrap();

    assert_eq!(value, "new_value");
    println!("{:?}", transaction);
}
