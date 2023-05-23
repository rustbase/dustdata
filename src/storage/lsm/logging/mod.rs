use std::{
    fs,
    io::{Read, Write},
    path,
};

use serde::{Deserialize, Serialize};

use fs2::FileExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Logging {
    pub log: Vec<LogOp>,
    pub log_path: path::PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogOp {
    Insert {
        key: String,
        value: bson::Bson,
    },
    Delete {
        key: String,
        value: bson::Bson,
    },
    Update {
        key: String,
        old_value: bson::Bson,
        new_value: bson::Bson,
    },
}

impl Logging {
    pub fn new(path: path::PathBuf) -> Self {
        let log_path = path.join("DUSTDATA.logging");

        if log_path.exists() {
            Logging::read_log_file(log_path)
        } else {
            Logging {
                log: Vec::new(),
                log_path,
            }
        }
    }

    pub fn insert(&mut self, key: &str, value: bson::Bson) {
        self.log.push(LogOp::Insert {
            key: key.to_string(),
            value,
        });
    }

    pub fn delete(&mut self, key: &str, value: bson::Bson) {
        self.log.push(LogOp::Delete {
            key: key.to_string(),
            value,
        });
    }

    pub fn update(&mut self, key: &str, old_value: bson::Bson, new_value: bson::Bson) {
        self.log.push(LogOp::Update {
            key: key.to_string(),
            old_value,
            new_value,
        });
    }

    fn reverse_operation(op: LogOp) -> LogOp {
        match op {
            LogOp::Insert { key, value } => LogOp::Delete { key, value },
            LogOp::Delete { key, value } => LogOp::Insert { key, value },
            LogOp::Update {
                key,
                old_value,
                new_value,
            } => LogOp::Update {
                key,
                old_value: new_value,
                new_value: old_value,
            },
        }
    }

    pub fn rollback(&mut self, offset: u32) -> Vec<LogOp> {
        let mut ops = Vec::new();

        let log = self.log.split_off(offset as usize);

        for op in log.iter().rev() {
            let reverse_op = Logging::reverse_operation(op.clone());
            self.log.push(reverse_op.clone());
            ops.push(reverse_op);
        }

        ops
    }

    pub fn flush(&self) {
        let self_vec = bson::to_vec(self).unwrap();

        let mut file = fs::File::create(self.log_path.clone()).unwrap();

        file.write_all(&self_vec).unwrap();

        file.sync_data().unwrap();
        file.flush().unwrap();
    }

    fn read_log_file(path: path::PathBuf) -> Self {
        let mut file = fs::File::open(path).unwrap();

        file.lock_exclusive().unwrap();

        let mut content = Vec::new();
        file.read_to_end(&mut content).unwrap();

        bson::from_slice(&content).unwrap()
    }
}

impl Drop for Logging {
    fn drop(&mut self) {
        self.flush();
        let file = fs::File::open(self.log_path.clone()).unwrap();

        file.unlock().unwrap();
    }
}
