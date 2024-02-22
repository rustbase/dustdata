use crate::error::{Error, Result};

use super::{config, Operation, Transaction};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Read, Seek, SeekFrom, Write};
use std::{fs, path};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransactionLog<T> {
    pub id: usize,
    pub data: Vec<WalOperation<T>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum WalOperation<T> {
    Insert {
        key: String,
        value: T,
    },
    Update {
        key: String,
        new_value: T,
        old_value: T,
    },
    Delete {
        key: String,
        value: T,
    },
    Drop,
}

impl<T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned> WalOperation<T> {
    pub fn reverse_operation(&self) -> Operation<T> {
        match self {
            WalOperation::Insert { key, .. } => Operation::Delete(key.clone()),
            WalOperation::Update { key, old_value, .. } => {
                Operation::Update(key.clone(), old_value.clone())
            }
            WalOperation::Delete { key, value } => Operation::Insert(key.clone(), value.clone()),
            _ => Operation::Drop,
        }
    }
}

struct LogFile {
    pub id: usize,
    pub file: fs::File,
}

impl LogFile {
    pub fn new(log_path: &path::Path, max_log_size: u64) -> Self {
        let id = LogFile::log_chunk(log_path, max_log_size);

        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path.join(format!("DustDataLog_{}", id)))
            .unwrap();

        Self { id, file }
    }

    fn log_chunk(log_path: &path::Path, max_log_size: u64) -> usize {
        let mut id = 0;

        loop {
            let file_path = log_path.join(format!("DustDataLog_{}", id));

            if !file_path.exists() {
                break;
            }

            let metadata = fs::metadata(file_path).unwrap();

            if metadata.len() < max_log_size {
                break;
            }

            id += 1;
        }

        id
    }
}

pub struct Wal {
    config: config::DustDataConfig,
    current_file: LogFile,
    pub index: WALIndex,
}

impl Wal {
    pub fn new(config: config::DustDataConfig) -> Result<Self> {
        let log_path = config.data_path.join(&config.wal.log_path);

        fs::create_dir_all(&log_path).ok();

        let current_file = LogFile::new(&log_path, config.wal.max_log_size);

        let index = WALIndex::new(&log_path)?;

        Ok(Self {
            config,
            current_file,
            index,
        })
    }

    pub fn revert<T>(&mut self, tx_id: usize) -> Result<Transaction<T>>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        let log = self.read::<T>(tx_id)?;

        let mut transaction = Transaction::new();

        if let Some(log) = log {
            let mut operations = Vec::new();

            for operation in log.data {
                operations.push(operation.reverse_operation());
            }

            transaction.extend(operations);
        }

        Ok(transaction)
    }

    pub fn write<T>(&mut self, transaction: TransactionLog<T>)
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static,
    {
        let offset = self.current_file.file.metadata().unwrap().len() as usize;
        let bytes = bson::to_vec(&transaction).unwrap();

        self.index
            .write(transaction.id, self.current_file.id, offset);
        self.current_file.file.write_all(&bytes).unwrap();
    }

    pub fn read<T>(&self, tx_id: usize) -> Result<Option<TransactionLog<T>>>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        let index_tuple = self.index.get(tx_id);

        if index_tuple.is_none() {
            return Ok(None);
        }

        let (log_chunk, offset) = index_tuple.unwrap();

        self.read_by_offset_and_log_chunk(offset, log_chunk)
    }

    pub fn read_by_offset_and_log_chunk<T>(&self, offset: usize, log_chunk: usize) -> Result<T>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        let filename = format!("DustDataLog_{}", log_chunk);
        let mut file = fs::OpenOptions::new()
            .read(true)
            .open(self.config.data_path.join("log").join(&filename))
            .map_err(|r| match r.kind() {
                std::io::ErrorKind::NotFound => Error::CorruptedData(format!(
                    "WAL Log {} not found, but wal index contains it",
                    filename
                )),
                _ => Error::IoError(r),
            })?;

        let mut length = [0; 1];
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.read_exact(&mut length).unwrap();

        let mut value = vec![0; length[0] as usize];
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.read_exact(&mut value).unwrap();

        let value = bson::from_slice(&value).map_err(|e| {
            Error::CorruptedData(format!(
                "Corrupted wal log {} and offset {}. Error: {}",
                filename, offset, e
            ))
        })?;

        Ok(value)
    }
}

const WAL_INDEX_FILENAME: &str = ".wal-index-dustdata";

#[derive(Serialize, Deserialize)]
struct WALIndexEntry<T> {
    tx_id: usize,
    data: Vec<WalOperation<T>>,
}

pub struct WALIndex {
    index: BTreeMap<usize, (usize, usize)>, // tx_id -> (DustDataLog_*, offset)
    index_path: path::PathBuf,
}

impl WALIndex {
    pub fn new(path: &path::Path) -> Result<Self> {
        let index_path = path.join(WAL_INDEX_FILENAME);

        let mut file = fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(index_path.clone())
            .map_err(Error::IoError)?;

        let index = if file.metadata().unwrap().len() == 0 {
            let index = BTreeMap::new();

            let bytes = bincode::serialize(&index).unwrap();
            file.write_all(&bytes).map_err(Error::IoError)?;

            index
        } else {
            bincode::deserialize_from(&file)
                .map_err(|e| Error::CorruptedData(format!("Corrupted wal index: {}", e)))?
        };

        Ok(Self { index, index_path })
    }

    pub fn write(&mut self, id: usize, log_chunk: usize, offset: usize) {
        self.index.insert(id, (log_chunk, offset));

        let bytes = bincode::serialize(&self.index).unwrap();
        fs::write(&self.index_path, bytes).unwrap();
    }

    pub fn get_head(&self) -> Option<usize> {
        self.index.keys().next_back().copied()
    }

    pub fn diff(&self, tx_id: usize, other_tx_id: usize) -> Vec<(usize, (usize, usize))> {
        let mut diff = Vec::new();

        let iter = self.index.range(tx_id..=other_tx_id);

        for (key, value) in iter {
            diff.push((*key, *value));
        }

        diff
    }

    pub fn get(&self, key: usize) -> Option<(usize, usize)> {
        self.index.get(&key).copied()
    }
}
