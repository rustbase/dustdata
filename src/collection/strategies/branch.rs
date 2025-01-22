use std::fs;

use crate::{
    btree::{spec::BTreePair, BTree, BTreeIterator, ValueTrait},
    collection::{
        xact::{Transaction, TransactionOperations},
        xlog::XLogOperation,
    },
    dustdata_config,
    error::{Error, Result},
};

pub struct BranchTransaction<T: ValueTrait> {
    pub xid: u64,
    pub btree: BTree<String, T>,
    pub xact_op: TransactionOperations<T>,
    pub collection_name: String,
}

impl<T: ValueTrait> Transaction<T> for BranchTransaction<T> {
    fn get(&mut self, key: &str) -> Result<Option<T>> {
        self.btree.get(&key.to_string())
    }

    fn insert(&mut self, key: &str, value: T) -> Result<()> {
        self.xact_op.write(XLogOperation::Insert {
            key: key.to_string(),
            value: value.clone(),
        })?;

        self.btree.insert(key.to_string(), value)
    }

    fn delete(&mut self, key: &str) -> Result<()> {
        self.xact_op.write(XLogOperation::Delete {
            key: key.to_string(),
        })?;

        self.btree.delete(&key.to_string())
    }

    fn update(&mut self, key: &str, new_value: T) -> Result<()> {
        self.xact_op.write(XLogOperation::Update {
            key: key.to_string(),
            new_value: new_value.clone(),
        })?;

        self.btree.delete(&key.to_string())?;
        self.btree.insert(key.to_string(), new_value)?;

        Ok(())
    }

    fn iter(&mut self) -> BTreeIterator<'_, String, T> {
        self.btree.iter()
    }

    fn find_by_pattern<'a>(
        &'a mut self,
        pattern: &'a str,
    ) -> Box<(dyn Iterator<Item = BTreePair<String, T>> + 'a)> {
        Box::new(self.btree.find_pattern(pattern))
    }

    fn rollback(self) {}

    fn xid(&self) -> u64 {
        self.xid
    }

    fn log(&mut self) -> &mut TransactionOperations<T> {
        &mut self.xact_op
    }
}

impl<T: ValueTrait> Drop for BranchTransaction<T> {
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
