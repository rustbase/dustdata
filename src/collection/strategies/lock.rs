use std::{fs, sync::MutexGuard};

use crate::{
    btree::{BTree, ValueTrait},
    collection::xact::{Transaction, TransactionOperations},
    dustdata_config,
    error::Error,
};

pub struct LockTransaction<'lock, T: ValueTrait> {
    pub xid: u64,
    pub _lock: MutexGuard<'lock, BTree<String, T>>,
    pub btree: BTree<String, T>,
    pub xact_op: TransactionOperations<T>,
    pub collection_name: String,
}

impl<'lock, T: ValueTrait> Transaction<T> for LockTransaction<'lock, T> {
    fn rollback(self) {}

    fn xid(&self) -> u64 {
        self.xid
    }

    fn log(&mut self) -> &mut TransactionOperations<T> {
        &mut self.xact_op
    }

    fn btree(&mut self) -> &mut BTree<String, T> {
        &mut self.btree
    }
}

impl<'a, T: ValueTrait> Drop for LockTransaction<'a, T> {
    fn drop(&mut self) {
        let dustdata_config = dustdata_config();
        let base_path = dustdata_config.data_path.join(&self.collection_name);

        let xid = self.xid();

        let xact_file = base_path.join(format!("Data.xact.{}", xid));
        let xlog_file = base_path.join(format!("Data.xlog.{}", xid));

        fs::remove_file(xact_file).map_err(Error::IoError).unwrap();
        fs::remove_file(xlog_file).map_err(Error::IoError).unwrap();
    }
}
