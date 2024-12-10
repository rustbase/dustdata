use crate::error::{self, Error, Result};
use crate::serializer::{deserialize, serialize};
use crate::OpenOptions;

use super::{config, Operation, Transaction};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::{fs, path};

use config::dustdata_config;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransactionLog<T> {
    pub tx_id: usize,
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

#[derive(Debug)]
struct LogFile {
    pub file: fs::File,
}

impl LogFile {
    pub fn new(log_path: &path::Path) -> Self {
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path.join(format!("DustDataLog.xlog")))
            .unwrap();

        Self { file }
    }
}

#[derive(Debug)]
pub struct Wal {
    current_file: LogFile,
    pub index: WALIndex,
}

const WAL_DATA_PATH: &str = "dustdata_xlog";

impl Wal {
    pub fn new() -> Result<Self> {
        let dustdata_config = dustdata_config();

        let log_path = dustdata_config.data_path.join(WAL_DATA_PATH);

        fs::create_dir_all(&log_path).ok();

        let current_file = LogFile::new(&log_path);

        let index = WALIndex::new(&log_path)?;

        Ok(Self {
            current_file,
            index,
        })
    }

    pub fn revert_to<T>(&self, tx_id: usize) -> Result<Vec<Transaction<T>>>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        let logs = self.revert_range(tx_id..)?;

        Ok(logs)
    }

    pub fn revert_range<T, R>(&self, range: R) -> Result<Vec<Transaction<T>>>
    where
        R: RangeBounds<usize>,
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        let logs = self.read_range::<T, R>(range)?;

        let mut transactions = Vec::new();

        if let Some(logs) = logs {
            for log in logs {
                let mut transaction = Transaction::new();

                for operation in log.data {
                    transaction.push(operation.reverse_operation());
                }

                transactions.push(transaction);
            }
        }

        Ok(transactions)
    }

    pub fn revert_transaction<T>(&self, tx_id: usize) -> Result<Transaction<T>>
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

    pub fn write<T>(&mut self, transaction: TransactionLog<T>) -> Result<()>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        if self.config.open_options == OpenOptions::ReadOnly {
            return Err(error::Error::Cannot(
                "commit a transaction, due read-only mode".to_string(),
            ));
        }

        let offset = self.current_file.file.metadata().unwrap().len() as usize;
        let bytes = Self::serialize_value(&transaction);

        self.index
            .write(transaction.tx_id, self.current_file.id, offset);
        self.current_file.file.write_all(&bytes).unwrap();

        Ok(())
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

    pub fn read_range<T, R>(&self, tx_id_range: R) -> Result<Option<Vec<TransactionLog<T>>>>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
        R: RangeBounds<usize>,
    {
        let keys = self.index.diff(tx_id_range);

        let mut logs = Vec::new();

        for (_, (log_chunk, offset)) in keys {
            let log = self.read_by_offset_and_log_chunk(offset, log_chunk)?;

            if let Some(log) = log {
                logs.push(log);
            }
        }

        Ok(Some(logs))
    }

    pub fn read_head<T>(&self) -> Result<Option<TransactionLog<T>>>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        let tx_id = self.index.get_head();

        if tx_id.is_none() {
            return Ok(None);
        }

        self.read::<T>(tx_id.unwrap())
    }

    pub fn read_by_offset_and_log_chunk<T>(
        &self,
        offset: usize,
        log_chunk: usize,
    ) -> Result<Option<TransactionLog<T>>>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        let filename = format!("DustDataLog_{}", log_chunk);
        let mut file = fs::OpenOptions::new()
            .read(true)
            .open(self.config.data_path.join("log").join(&filename))
            .map_err(Error::IoError)?;

        Self::deserialize_value(&mut file, offset, &filename)
    }

    fn serialize_value<T>(value: &T) -> Vec<u8>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        let mut bytes = Vec::new();

        let serialized_value = bincode::serialize(value).unwrap();

        bytes.extend_from_slice(&serialized_value.len().to_le_bytes());
        bytes.extend_from_slice(&serialized_value);

        bytes
    }

    fn deserialize_value<T>(
        file: &mut fs::File,
        offset: usize,
        filename: &str,
    ) -> Result<Option<TransactionLog<T>>>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(Error::IoError)?;

        let mut length = [0; 8];
        file.read_exact(&mut length).unwrap();
        let length = u64::from_le_bytes(length) as usize;

        let mut value = vec![0; length];
        file.read_exact(&mut value).unwrap();

        let value = bincode::deserialize(&value).map_err(Error::SerializeError)?;

        Ok(Some(value))
    }
}

const WAL_INDEX_FILENAME: &str = ".wal-index-dustdata";

#[derive(Serialize, Deserialize)]
struct WALIndexEntry<T> {
    tx_id: usize,
    data: Vec<WalOperation<T>>,
}

#[derive(Debug)]
pub struct WALIndex {
    pub inner: BTreeMap<usize, (usize, usize)>, // tx_id -> (DustDataLog_*, offset)
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

        let inner = if file.metadata().unwrap().len() == 0 {
            let index = BTreeMap::new();

            let bytes = serialize(&index).unwrap();

            file.write_all(&bytes).map_err(Error::IoError)?;

            index
        } else {
            let mut bytes = Vec::new();

            file.read_to_end(&mut bytes).map_err(Error::IoError)?;

            deserialize(&bytes).unwrap()
        };

        Ok(Self { inner, index_path })
    }

    pub fn write(&mut self, id: usize, log_chunk: usize, offset: usize) {
        self.inner.insert(id, (log_chunk, offset));

        let bytes = serialize(&self.inner).unwrap();

        fs::write(&self.index_path, bytes).unwrap();
    }

    pub fn get_head(&self) -> Option<usize> {
        self.inner.keys().next_back().copied()
    }

    pub fn diff<R>(&self, tx_id_range: R) -> Vec<(usize, (usize, usize))>
    where
        R: RangeBounds<usize>,
    {
        let mut diff = Vec::new();

        let iter = self.inner.range(tx_id_range);

        for (key, value) in iter {
            diff.push((*key, *value));
        }

        diff
    }

    pub fn keys(&self) -> Vec<usize> {
        self.inner.keys().copied().collect()
    }

    pub fn get(&self, key: usize) -> Option<(usize, usize)> {
        self.inner.get(&key).copied()
    }
}
