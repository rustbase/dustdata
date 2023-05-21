use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    ops::Deref,
    path,
    sync::{Arc, RwLock},
};

#[derive(Clone)]
pub struct Index {
    pub index: Arc<RwLock<HashMap<String, (usize, u64 /* which file and which offset */)>>>,
    pub index_path: path::PathBuf,
}

impl Index {
    pub fn new(index_path: path::PathBuf) -> Self {
        let index = if index_path.exists() {
            Index::read_index(&index_path)
        } else {
            HashMap::new()
        };

        Self {
            index: Arc::new(RwLock::new(index)),
            index_path,
        }
    }

    fn read_index(path: &path::Path) -> HashMap<String, (usize, u64)> {
        let mut file = fs::File::open(path).unwrap();
        let mut bytes_to_read: Vec<u8> = Vec::new();
        file.read_to_end(&mut bytes_to_read).unwrap();

        let index_bson: HashMap<String, (usize, u64)> = bson::from_slice(&bytes_to_read).unwrap();

        index_bson
    }

    pub fn write_index(&self) {
        let index = self.index.read().unwrap();
        let doc = bson::to_vec(index.deref()).unwrap();

        let mut file = fs::File::create(self.index_path.clone()).unwrap();
        file.write_all(&doc).unwrap();

        file.sync_all().unwrap();
        file.flush().unwrap();
    }
}
