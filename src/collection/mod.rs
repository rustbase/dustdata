mod storage;
mod wal;

use crate::config;
use crate::error::{self, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::ops::RangeBounds;
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, RwLock},
    time,
};
use wal::{TransactionLog, WalOperation};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Operation<T> {
    Insert(String, T),
    Update(String, T),
    Delete(String),
    Drop,
}

#[derive(Debug, Clone)]
pub struct Transaction<T> {
    status: TransactionStatus,
    data: Vec<Operation<T>>,
    tx_id: usize,
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

    /// Extends the transaction with a list of operations
    pub fn extend(&mut self, operations: Vec<Operation<T>>) {
        self.data.extend(operations);
    }

    /// Returns the transaction status
    /// This will return the current status of the transaction
    pub fn status(&self) -> &TransactionStatus {
        &self.status
    }

    /// Returns the transaction id
    /// This will return the transaction id
    pub fn tx_id(&self) -> usize {
        self.tx_id
    }

    pub fn push(&mut self, operation: Operation<T>) {
        self.data.push(operation);
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
            tx_id: get_current_timestamp(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransactionStatus {
    Active,
    Committed,
    RolledBack,
    Aborted,
}

#[derive(Clone, Debug)]
pub struct Collection<T: Sync + Send + Clone + Debug + Serialize + DeserializeOwned + 'static> {
    memtable: Memtable<T>,
    storage: Storage,
    pub wal: Wal,
}

type Memtable<T> = Arc<RwLock<HashMap<String, T>>>;
type Storage = Arc<RwLock<storage::Storage>>;
type Wal = Arc<RwLock<wal::Wal>>;

impl<T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned> Collection<T> {
    pub fn new(config: config::DustDataConfig) -> Self {
        let storage = Arc::new(RwLock::new(storage::Storage::new(config.clone()).unwrap()));
        let wal = Arc::new(RwLock::new(wal::Wal::new(config.clone()).unwrap()));

        Self {
            memtable: Arc::new(RwLock::new(HashMap::new())),
            wal,
            storage,
        }
    }

    /// Starts a new transaction
    pub fn start(&self) -> Transaction<T> {
        Transaction::new()
    }

    /// Starts a new transaction and executes the given closure
    /// Example:
    /// ```
    /// let transaction = collection.start_lazy(|tx| {
    ///    tx.insert("key", "value");
    /// });
    pub fn start_lazy<F>(&self, f: F) -> Result<Transaction<T>>
    where
        F: FnOnce(&mut Transaction<T>),
    {
        let mut transaction = self.start();
        f(&mut transaction);
        self.commit(&mut transaction)?;
        Ok(transaction)
    }

    /// Commits a transaction
    pub fn commit(&self, transaction: &mut Transaction<T>) -> Result<()> {
        if let TransactionStatus::Committed = transaction.status {
            panic!("Transaction already committed");
        }

        if transaction.data.is_empty() {
            return Ok(());
        }

        let mut wal = self.wal.try_write().map_err(|_| error::Error::Deadlock)?;

        let wal_operations = self.execute_operation(transaction.tx_id, &transaction.data)?;

        let transaction_log = TransactionLog {
            tx_id: transaction.tx_id(),
            data: wal_operations,
        };

        wal.write(transaction_log);

        transaction.status = TransactionStatus::Committed;

        Ok(())
    }

    /// Aborts a transaction
    pub fn abort_transaction(&self, transaction: &mut Transaction<T>) {
        if let TransactionStatus::Committed = transaction.status() {
            panic!("Transaction already committed");
        }

        transaction.status = TransactionStatus::Aborted;
    }

    /// Rolls back a transaction with a specific range
    /// This will revert all operations in the transaction
    /// Returns the reverted transaction
    pub fn rollback_transaction_range<R>(&self, range: R) -> Result<Vec<Transaction<T>>>
    where
        R: RangeBounds<usize>,
    {
        let mut rollback_transactions = {
            let wal = self.wal.read().map_err(|_| error::Error::Deadlock)?;
            wal.revert_range(range)?
        };

        for rollback_transaction in rollback_transactions.iter_mut() {
            self.commit(rollback_transaction)?;
        }

        Ok(rollback_transactions)
    }

    /// Rolls back a transaction
    /// This will revert all operations in the transaction
    /// Returns the reverted transaction
    pub fn rollback_transaction(&self, transaction: &mut Transaction<T>) -> Result<Transaction<T>> {
        match transaction.status() {
            TransactionStatus::RolledBack => panic!("Transaction already rolled back"),
            TransactionStatus::Active => panic!("Transaction not committed"),
            TransactionStatus::Aborted => panic!("Transaction aborted"),
            _ => {}
        }

        let tx_id = transaction.tx_id();

        let mut rollback_transaction = {
            let wal = self.wal.read().map_err(|_| error::Error::Deadlock)?;
            wal.revert_transaction::<T>(tx_id)?
        };

        self.commit(&mut rollback_transaction)?;

        transaction.status = TransactionStatus::RolledBack;

        Ok(rollback_transaction)
    }

    /// Resets a transaction.
    /// This will revert all operations in the transaction without committing it
    pub fn reset_transaction<R>(&self, transaction: &mut Transaction<T>) -> Result<()> {
        match transaction.status() {
            TransactionStatus::RolledBack => panic!("Transaction already rolled back"),
            TransactionStatus::Active => panic!("Transaction not committed"),
            TransactionStatus::Aborted => panic!("Transaction aborted"),
            _ => {}
        }

        let tx_id = transaction.tx_id();

        let revert_transaction = {
            let wal = self.wal.read().map_err(|_| error::Error::Deadlock)?;
            wal.revert_transaction::<T>(tx_id)?
        };

        self.execute_operation(tx_id, &revert_transaction.data)?;

        transaction.status = TransactionStatus::Active;

        Ok(())
    }

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

    fn execute_operation(
        &self,
        tx_id: usize,
        operations: &Vec<Operation<T>>,
    ) -> Result<Vec<WalOperation<T>>> {
        let mut memtable = self.memtable.write().map_err(|_| error::Error::Deadlock)?;
        let mut storage = self.storage.write().map_err(|_| error::Error::Deadlock)?;

        let mut wal_operations = Vec::new();

        for operation in operations {
            let operation = match operation {
                Operation::Insert(key, value) => {
                    memtable.insert(key.to_owned(), value.clone());

                    let tuple_entry = storage::StorageTupleEntry {
                        key: key.to_owned(),
                        value: value.clone(),
                    };

                    storage.insert_tuple(tx_id, tuple_entry)?;

                    WalOperation::Insert {
                        key: key.to_string(),
                        value: value.clone(),
                    }
                }
                Operation::Delete(key) => {
                    memtable.remove(key.as_str());
                    let old_value = storage.remove_tuple(key.to_owned())?;

                    WalOperation::Delete {
                        key: key.to_string(),
                        value: old_value,
                    }
                }
                Operation::Update(key, value) => {
                    memtable.insert(key.to_owned(), value.clone());

                    let tuple_entry = storage::StorageTupleEntry {
                        key: key.to_owned(),
                        value: value.clone(),
                    };

                    let old_value = storage.update_tuple(tx_id, tuple_entry)?;

                    WalOperation::Update {
                        key: key.to_string(),
                        new_value: value.clone(),
                        old_value,
                    }
                }
                Operation::Drop => {
                    memtable.clear();
                    storage.clear()?;

                    WalOperation::Drop
                }
            };

            wal_operations.push(operation);
        }

        Ok(wal_operations)
    }
}

pub fn get_current_timestamp() -> usize {
    (time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_micros())
    .try_into()
    .unwrap()
}
