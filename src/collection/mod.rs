pub mod strategies;
pub mod xact;
pub mod xlog;

use crate::btree::{BTree, ValueTrait};
use crate::dustdata_config;
use crate::error::{Error, Result};
use crate::page::io::BlockIO;
use std::sync::Mutex;
use std::{fmt::Debug, sync::Arc};
use xact::TransactionBuilder;
use xlog::{XLog, XLogOperation};

pub use xact::Transaction;

#[derive(Debug, Clone)]
pub enum TransactionStatus {
    Active,
    Committed,
    RolledBack,
    Aborted,
}

#[derive(Clone)]
pub struct Collection<T: ValueTrait> {
    btree: Arc<Mutex<BTree<String, T>>>,
    xlog: Arc<Mutex<XLog<T>>>,
    name: String,
}

impl<T: ValueTrait> Collection<T> {
    pub fn new(name: &str) -> Result<Self> {
        let dustdata_config = dustdata_config();

        let base_path = dustdata_config.data_path.join(name);

        let xlog = Arc::new(Mutex::new(xlog::XLog::new(&base_path)?));

        let btree_block = BlockIO::new(base_path.join("Data.db")).map_err(Error::IoError)?;
        let btree = Arc::new(Mutex::new(BTree::new(btree_block)?));

        Ok(Self {
            btree,
            xlog,
            name: name.to_string(),
        })
    }

    /// Starts a new transaction
    pub fn branch_start(&self) -> Result<impl Transaction<T> + '_> {
        let dustdata_config = dustdata_config();
        let base_path = dustdata_config.data_path.join(self.name.clone());

        TransactionBuilder::branch(base_path, self.name.clone(), self.btree.lock().unwrap())
    }

    /// Starts a new transaction with a lock
    pub fn lock_start(&self) -> Result<impl Transaction<T> + '_> {
        let dustdata_config = dustdata_config();
        let base_path = dustdata_config.data_path.join(self.name.clone());

        TransactionBuilder::lock(base_path, self.name.clone(), self.btree.try_lock().unwrap())
    }

    /// Commits a transaction
    pub fn commit<X>(&self, mut xact: X) -> Result<()>
    where
        X: Transaction<T>,
    {
        let mut page = xact.log().page()?;

        // unlock mutex lock
        drop(xact);

        let mut btree = self.btree.lock().unwrap();

        for operation in page.iter() {
            match operation {
                XLogOperation::Insert { key, value } => btree.insert(key, value)?,
                XLogOperation::Delete { key } => btree.delete(&key)?,
                XLogOperation::Update { key, new_value } => {
                    btree.delete(&key)?;
                    btree.insert(key, new_value)?
                }
                XLogOperation::Drop => {}
            }
        }

        Ok(())
    }

    /// Checks if the collection contains a key
    pub fn contains(&self, key: &str) -> Result<bool> {
        self.btree.lock().unwrap().contains(&key.to_string())
    }

    /// Gets a value from the collection
    pub fn get(&self, key: &str) -> Result<Option<T>> {
        self.btree.lock().unwrap().get(&key.to_string())
    }
}
