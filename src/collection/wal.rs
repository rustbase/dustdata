use crate::error::{Error, Result};

use super::{config, Operation, Transaction};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
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

        let index = WALIndex::new(
            &log_path,
            config.wal.compression.is_some(),
            config.wal.compression.as_ref().map(|c| c.level),
        )?;

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
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        let offset = self.current_file.file.metadata().unwrap().len() as usize;
        let bytes = Self::serialize_value(&transaction);

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
            .map_err(|r| match r.kind() {
                std::io::ErrorKind::NotFound => Error::CorruptedData(format!(
                    "WAL Log {} not found, but wal index contains it",
                    filename
                )),
                _ => Error::IoError(r),
            })?;

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

        let value = bincode::deserialize(&value).map_err(|e| {
            Error::CorruptedData(format!(
                "Corrupted wal log {} and offset {}. Error: {}",
                filename, offset, e
            ))
        })?;

        Ok(Some(value))
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
    use_compression: bool,
    compression_lvl: Option<u32>,
}

impl WALIndex {
    pub fn new(
        path: &path::Path,
        use_compression: bool,
        compression_lvl: Option<u32>,
    ) -> Result<Self> {
        let index_path = path.join(WAL_INDEX_FILENAME);

        let mut file = fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(index_path.clone())
            .map_err(Error::IoError)?;

        let index = if file.metadata().unwrap().len() == 0 {
            let index = BTreeMap::new();

            let bytes = if use_compression {
                let mut encoder =
                    GzEncoder::new(Vec::new(), Compression::new(compression_lvl.unwrap()));
                encoder
                    .write_all(&bincode::serialize(&index).unwrap())
                    .unwrap();
                encoder.finish().unwrap()
            } else {
                bincode::serialize(&index).unwrap()
            };

            file.write_all(&bytes).map_err(Error::IoError)?;

            index
        } else {
            let mut bytes = Vec::new();

            file.read_to_end(&mut bytes).map_err(Error::IoError)?;

            let mut decoder = GzDecoder::new(&bytes[..]);

            if decoder.header().is_some() {
                let mut decoded_bytes = Vec::new();
                decoder.read_to_end(&mut decoded_bytes).unwrap();

                bincode::deserialize(&decoded_bytes).unwrap()
            } else {
                bincode::deserialize(&bytes).unwrap()
            }
        };

        Ok(Self {
            index,
            index_path,
            use_compression,
            compression_lvl,
        })
    }

    pub fn write(&mut self, id: usize, log_chunk: usize, offset: usize) {
        self.index.insert(id, (log_chunk, offset));

        let bytes = bincode::serialize(&self.index).unwrap();

        let bytes = if self.use_compression {
            let mut encoder =
                GzEncoder::new(Vec::new(), Compression::new(self.compression_lvl.unwrap()));
            encoder.write_all(&bytes).unwrap();
            encoder.finish().unwrap()
        } else {
            bytes
        };

        fs::write(&self.index_path, bytes).unwrap();
    }

    pub fn get_head(&self) -> Option<usize> {
        self.index.keys().next_back().copied()
    }

    pub fn diff<R>(&self, tx_id_range: R) -> Vec<(usize, (usize, usize))>
    where
        R: RangeBounds<usize>,
    {
        let mut diff = Vec::new();

        let iter = self.index.range(tx_id_range);

        for (key, value) in iter {
            diff.push((*key, *value));
        }

        diff
    }

    pub fn get(&self, key: usize) -> Option<(usize, usize)> {
        self.index.get(&key).copied()
    }
}
