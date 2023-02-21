use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    path,
};

pub fn check_if_index_exists(path: &path::Path) -> bool {
    let _path = path.join("index");

    _path.exists()
}

pub fn write_index(path: &path::Path, index: &HashMap<String, String>) {
    let _path = path.join("index");

    let doc = bson::to_vec(index).unwrap();

    let mut file = fs::File::create(_path).unwrap();
    file.write_all(&doc).unwrap();

    file.sync_data().unwrap();
    file.flush().unwrap();
}

pub fn read_index(path: &path::Path) -> HashMap<String, String> {
    let _path = path.join("index");

    let mut file = fs::File::open(_path).unwrap();
    let mut bytes_to_read: Vec<u8> = Vec::new();
    file.read_to_end(&mut bytes_to_read).unwrap();

    let index_bson: HashMap<String, String> = bson::from_slice(&bytes_to_read).unwrap();

    index_bson
}
