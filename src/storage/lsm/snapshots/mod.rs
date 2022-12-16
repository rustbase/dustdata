use lz4::{Decoder, EncoderBuilder};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::bloom::BloomFilter;

use super::Lsm;

#[derive(Clone, Debug)]
pub struct Snapshots {
    path: PathBuf,
}

impl Snapshots {
    pub fn new(path: String) -> Snapshots {
        let path = Path::new(&path);

        if !path.exists() {
            std::fs::create_dir_all(path).unwrap();
        }

        Snapshots {
            path: PathBuf::from(path),
        }
    }

    pub fn create_snapshot(&self, lsm: &Lsm) -> String {
        let snapshot = Snapshot::new(
            lsm.memtable.lock().unwrap().clone(),
            lsm.bloom_filter.lock().unwrap().clone(),
            lsm.dense_index.lock().unwrap().clone(),
        );

        let timestamp = snapshot.timestamp();
        let path = Path::new(&self.path);

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

    pub fn load_snapshot(&self, timestamp: String) -> Snapshot {
        let path = self.path.join(timestamp);

        let file = fs::File::open(path).unwrap();

        let mut decoder = Decoder::new(file).unwrap();
        let mut contents = Vec::new();
        decoder.read_to_end(&mut contents).unwrap();
        let snapshot: Snapshot = bson::from_slice(&contents).unwrap();

        snapshot
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

        let snapshot: Snapshot = self.load_snapshot(
            last_snapshot
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
        );

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

    pub fn timestamp(&self) -> String {
        let now = chrono::Local::now();
        now.format("%Y-%m-%d_%H-%M-%S").to_string()
    }
}
