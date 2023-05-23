use crate::bloom::BloomFilter;
use lz4::{Decoder, EncoderBuilder};
use std::{
    io::{Read, Write},
    path,
    sync::{Arc, RwLock},
};

use fs2::FileExt;

#[derive(Clone)]
pub struct Filter {
    pub bloom: Arc<RwLock<BloomFilter>>,
    path: path::PathBuf,
}

impl Filter {
    pub fn new(path: path::PathBuf) -> Self {
        let path = path.join("DUSTDATA.filter");
        let bloom_rate = 0.01;

        let bloom_filter = if path.exists() {
            Filter::read_filter(&path)
        } else {
            BloomFilter::new(bloom_rate, 100000)
        };

        Self {
            bloom: Arc::new(RwLock::new(bloom_filter)),
            path,
        }
    }

    fn write_filter(path: &path::Path, filter: &BloomFilter) {
        let filter_file = std::fs::File::create(path).unwrap();

        filter_file.lock_exclusive().unwrap();

        let mut encoder = EncoderBuilder::new()
            .level(4)
            .build(filter_file)
            .expect("cannot create encoder");

        let filter_content = bson::to_vec(filter).unwrap();

        encoder.write_all(&filter_content).unwrap();
        encoder.flush().unwrap();

        encoder.writer().unlock().unwrap();
    }

    fn read_filter(path: &path::Path) -> BloomFilter {
        let filter_file = std::fs::File::open(path).unwrap();

        filter_file.lock_exclusive().unwrap();

        let mut decoder = Decoder::new(filter_file).unwrap();

        let mut filter: Vec<u8> = Vec::new();
        decoder.read_to_end(&mut filter).unwrap();

        bson::from_slice(&filter).unwrap()
    }

    pub fn insert(&mut self, key: &str) {
        self.bloom.write().unwrap().insert(key);
    }

    pub fn contains(&self, key: &str) -> bool {
        self.bloom.read().unwrap().contains(key)
    }

    pub fn delete(&mut self, key: &str) {
        self.bloom.write().unwrap().delete(key);
    }

    pub fn flush(&mut self) {
        let filter = self.bloom.read().unwrap().clone();
        Filter::write_filter(&self.path, &filter);
    }

    pub fn clear(&mut self) {
        self.bloom.write().unwrap().clear();
    }
}

impl Drop for Filter {
    fn drop(&mut self) {
        self.flush();

        let file = std::fs::File::open(self.path.clone()).unwrap();

        file.unlock().unwrap();
    }
}
