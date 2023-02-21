use crate::bloom::BloomFilter;
use crate::storage::lsm::Lsm;
use lz4::{Decoder, EncoderBuilder};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{Read, Write};
use std::path::Path;

#[derive(Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub memtable: BTreeMap<String, bson::Bson>,
    pub bloom_filter: BloomFilter,
    pub dense_index: HashMap<String, String>,
}

impl Snapshot {
    pub fn new(
        memtable: BTreeMap<String, bson::Bson>,
        bloom_filter: BloomFilter,
        dense_index: HashMap<String, String>,
    ) -> Snapshot {
        Snapshot {
            memtable,
            bloom_filter,
            dense_index,
        }
    }

    /// It opens a file, reads it into a buffer, and then deserializes the buffer into a BSON document
    ///
    /// Arguments:
    ///
    /// * `path`: The path to the file you want to read from.
    ///
    /// Returns:
    ///
    /// A `Snapshot` struct.
    pub fn snapshot_from_file(path: &Path) -> Snapshot {
        let file = fs::File::open(path).unwrap();
        let mut decoder = Decoder::new(file).unwrap();

        let mut snapshot = Vec::new();
        decoder.read_to_end(&mut snapshot).unwrap();

        bson::from_slice(&snapshot).unwrap()
    }

    /// It creates a new directory in the path provided, creates a new snapshot, serializes it, and writes
    /// it to a file
    ///
    /// Arguments:
    ///
    /// * `lsm`: &Lsm - the LSM tree we want to snapshot
    /// * `path`: The path to the directory where the snapshot will be saved.
    ///
    /// Returns:
    ///
    /// A string representing the timestamp of the snapshot.
    pub fn snapshot_to_file(lsm: &Lsm, path: &Path) -> String {
        if !path.exists() {
            std::fs::create_dir_all(path).unwrap();
        }

        let snapshot = Snapshot::new(
            lsm.memtable.read().unwrap().clone(),
            lsm.bloom_filter.read().unwrap().clone(),
            lsm.dense_index.read().unwrap().clone(),
        );

        let now = chrono::Local::now();
        let timestamp = now.format("%Y-%m-%d-%H-%M-%S").to_string();

        let snapshot_path = path.join(timestamp.clone());
        let file = fs::File::create(snapshot_path).unwrap();

        let snapshot = bson::to_vec(&snapshot).unwrap();

        let mut encoder = EncoderBuilder::new()
            .build(file)
            .expect("cannot create encoder");

        encoder.write_all(&snapshot).unwrap();
        encoder.flush().unwrap();

        timestamp
    }

    pub fn get_memtable(&self) -> &BTreeMap<String, bson::Bson> {
        &self.memtable
    }

    pub fn get_bloom_filter(&self) -> &BloomFilter {
        &self.bloom_filter
    }

    pub fn get_dense_index(&self) -> &HashMap<String, String> {
        &self.dense_index
    }
}
