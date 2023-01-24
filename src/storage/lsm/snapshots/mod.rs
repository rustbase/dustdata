use lz4::{Decoder, EncoderBuilder};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::bloom::BloomFilter;

use super::Lsm;

#[derive(Clone, Debug)]
pub struct SnapshotManager {
    path: PathBuf,
}

impl SnapshotManager {
    pub fn new(path: PathBuf) -> Self {
        SnapshotManager { path }
    }

    pub fn load_last_snapshot(&self) -> Snapshot {
        let mut paths = fs::read_dir(&self.path).unwrap();

        let mut last_snapshot = paths.next().unwrap().unwrap().path();

        for path in paths {
            let path = path.unwrap().path();

            if path.metadata().unwrap().modified().unwrap()
                > last_snapshot.metadata().unwrap().modified().unwrap()
            {
                last_snapshot = path;
            }
        }

        let snapshot: Snapshot = Snapshot::load_snapshot(last_snapshot);

        snapshot
    }

    pub fn load_snapshot_by_index(&self, index: usize) -> Snapshot {
        let mut paths = fs::read_dir(&self.path).unwrap();

        let snapshot = paths.nth(index).unwrap().unwrap().path();

        let snapshot: Snapshot = Snapshot::load_snapshot(snapshot);

        snapshot
    }
}

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

    pub fn load_snapshot(path: PathBuf) -> Snapshot {
        let file = fs::File::open(path).unwrap();

        let mut decoder = Decoder::new(file).unwrap();
        let mut contents = Vec::new();
        decoder.read_to_end(&mut contents).unwrap();
        let snapshot: Snapshot = bson::from_slice(&contents).unwrap();

        snapshot
    }

    pub fn create_snapshot(lsm: &Lsm, path: PathBuf) -> String {
        if !path.exists() {
            std::fs::create_dir_all(path.clone()).unwrap();
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

    pub fn timestamp(&self) -> String {
        let now = chrono::Local::now();
        now.format("%Y-%m-%d-%H-%M-%S").to_string()
    }
}
