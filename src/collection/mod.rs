mod storage;

use crate::config;
use crate::error::{self, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, RwLock},
    time,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Operation<T> {
    Insert(String, T),
    Update(String, T),
    Delete(String),
    Drop,
}

pub struct Transaction<T> {
    status: TransactionStatus,
    data: Vec<Operation<T>>,
}

impl<T> Transaction<T> {
    /// Adds an insert operation to the transaction
    /// This will insert a value into the collection
    pub fn insert(&mut self, key: &str, value: T) {
        self.data.push(Operation::Insert(key.to_string(), value))
    }

    /// Adds a delete operation to the transaction
    /// This will delete a value from the collection
    pub fn delete(&mut self, key: &str) {
        self.data.push(Operation::Delete(key.to_string()));
    }

    /// Adds an update operation to the transaction
    /// This will update a value in the collection
    pub fn update(&mut self, key: &str, value: T) {
        self.data.push(Operation::Update(key.to_string(), value));
    }

    /// Adds a clear operation to the transaction
    /// This will clear the entire collection
    pub fn clear(&mut self) {
        self.data.push(Operation::Drop);
    }
}

impl<T> Default for Transaction<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Transaction<T> {
    pub fn new() -> Self {
        Self {
            status: TransactionStatus::Active,
            data: Vec::new(),
        }
    }
}

pub enum TransactionStatus {
    Active,
    Committed,
    RolledBack,
    Aborted,
}

pub struct Collection<T: Sync + Send + Clone + Debug + Serialize + DeserializeOwned + 'static> {
    memtable: Memtable<T>,
    storage: Arc<RwLock<storage::Storage>>,
}

type Memtable<T> = Arc<RwLock<HashMap<String, T>>>;

impl<T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned> Collection<T> {
    pub fn new(config: config::DustDataConfig) -> Self {
        let storage = Arc::new(RwLock::new(storage::Storage::new(config.clone()).unwrap()));

        Self {
            memtable: Arc::new(RwLock::new(HashMap::new())),
            storage,
        }
    }

    /// Starts a new transaction
    pub fn start(&self) -> Transaction<T> {
        Transaction::new()
    }

    /// Commits a transaction
    pub fn commit(&self, transaction: &mut Transaction<T>) -> Result<()> {
        if let TransactionStatus::Committed = transaction.status {
            panic!("Transaction already committed");
        }

        // let mut wal = self.wal.write().map_err(|_| error::Error::Deadlock)?;
        let mut memtable = self.memtable.write().map_err(|_| error::Error::Deadlock)?;
        let mut storage = self.storage.write().map_err(|_| error::Error::Deadlock)?;

        for operation in &transaction.data {
            match operation {
                Operation::Insert(key, value) => {
                    memtable.insert(key.to_owned(), value.clone());

                    let tuple_entry = storage::StorageTupleEntry {
                        key: key.to_owned(),
                        value: value.clone(),
                    };

                    storage.insert_tuple(tuple_entry)?;
                }
                Operation::Delete(key) => {
                    memtable.remove(key.as_str());
                    storage.remove_tuple(key.to_owned())?;
                }
                Operation::Update(key, value) => {
                    memtable.insert(key.to_owned(), value.clone());

                    let tuple_entry = storage::StorageTupleEntry {
                        key: key.to_owned(),
                        value: value.clone(),
                    };

                    storage.update_tuple(tuple_entry)?;
                }
                Operation::Drop => {
                    memtable.clear();
                    storage.clear()?;
                }
            };
        }

        transaction.status = TransactionStatus::Committed;

        Ok(())
    }

    /// Aborts a transaction
    pub fn abort_transaction(&self, transaction: &mut Transaction<T>) {
        if let TransactionStatus::Committed = transaction.status {
            panic!("Transaction already committed");
        }

        transaction.status = TransactionStatus::Aborted;
    }

    // pub fn rollback_transaction(&self, transaction: &mut Transaction<T>) {
    //     match transaction.status {
    //         TransactionStatus::RolledBack => panic!("Transaction already rolled back"),
    //         TransactionStatus::Active => panic!("Transaction not committed"),
    //         TransactionStatus::Aborted => panic!("Transaction aborted"),
    //         _ => {}
    //     }

    //     for key in transaction.data.keys() {
    //         let mut versions = self.memtable.get_mut(key).unwrap();

    //         let versions_clone = versions.clone();
    //         let version = versions_clone
    //             .iter()
    //             .enumerate()
    //             .find(|(_, v)| v.tx_id == transaction.id)
    //             .unwrap();

    //         versions.remove(version.0);
    //     }

    //     transaction.status = TransactionStatus::RolledBack;
    // }

    /// Checks if the collection contains a key
    pub fn contains(&self, key: &str) -> Result<bool> {
        Ok(self.storage.read().unwrap().contains(key))
    }

    /// Gets a value from the collection
    pub fn get(&self, key: &str) -> Result<Option<T>> {
        if !self.contains(key)? {
            return Ok(None);
        }

        let memtable = self.memtable.read().map_err(|_| error::Error::Deadlock)?;

        if let Some(value) = memtable.get(key) {
            Ok(Some(value.clone()))
        } else {
            let storage = self.storage.read().unwrap().get_tuple(key.to_owned())?;
            if let Some(value) = storage {
                Ok(Some(value))
            } else {
                Ok(None)
            }
        }
    }
}

pub fn get_current_timestamp() -> u64 {
    (time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_micros())
    .try_into()
    .unwrap()
}
