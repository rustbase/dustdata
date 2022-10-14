use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    path,
};

pub fn check_if_index_exists(path: &str) -> bool {
    let _path = path::Path::new(path).join("index");

    _path.exists()
}

pub fn write_index(path: &str, index: &BTreeMap<String, String>) {
    let _path = path::Path::new(path).join("index");

    if index.is_empty() {
        return;
    }

    let mut doc = bson::Document::new();

    for (key, offset) in index.iter() {
        doc.insert(key, offset.to_string());
    }

    let mut file = fs::File::create(_path).unwrap();
    file.write_all(&bson::to_vec(&doc).expect("Failed to serialize index"))
        .unwrap();

    file.sync_all().unwrap();
}

pub fn read_index(path: &str) -> BTreeMap<String, String> {
    let _path = path::Path::new(path).join("index");

    let mut file = fs::File::open(_path).unwrap();
    let mut bytes_to_read: Vec<u8> = Vec::new();
    file.read_to_end(&mut bytes_to_read).unwrap();

    let mut index: BTreeMap<String, String> = BTreeMap::new();
    let index_bson: bson::Document = bson::from_slice(&bytes_to_read).unwrap();

    for doc in index_bson {
        index.insert(doc.0.to_string(), doc.1.as_str().unwrap().to_string());
    }

    index
}
