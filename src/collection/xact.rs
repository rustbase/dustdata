use std::{marker::PhantomData, path::Path, sync::MutexGuard, time};

use crate::{
    btree::{spec::BTreePair, BTree, BTreeIterator, ValueTrait},
    error::{Error, Result},
    page::{io::BlockIO, pager::Pager, Page},
};

use super::{
    strategies::{branch::BranchTransaction, lock::LockTransaction},
    xlog::XLogOperation,
};

pub struct TransactionOperations<T> {
    io: BlockIO,
    _t: PhantomData<T>,
}

impl<T: ValueTrait> TransactionOperations<T> {
    pub fn new(io: BlockIO) -> Self {
        Self {
            io,
            _t: PhantomData,
        }
    }

    pub fn write(&mut self, operation: XLogOperation<T>) -> Result<()> {
        let mut page: Pager<XLogOperation<T>> = self.io.page(0).map_err(Error::IoError)?;

        page.write(operation)?;

        self.io.write_page(0, &page).map_err(Error::IoError)?;

        Ok(())
    }

    pub fn page(&mut self) -> Result<Page<XLogOperation<T>>> {
        let page: Page<XLogOperation<T>> = self.io.read_page(0).map_err(Error::IoError)?;

        Ok(page)
    }
}

pub trait Transaction<T: ValueTrait> {
    fn get(&mut self, key: &str) -> Result<Option<T>> {
        self.btree().get(&key.to_string())
    }

    fn insert(&mut self, key: &str, value: T) -> Result<()> {
        self.log().write(XLogOperation::Insert {
            key: key.to_string(),
            value: value.clone(),
        })?;

        self.btree().insert(key.to_string(), value)
    }

    fn delete(&mut self, key: &str) -> Result<()> {
        self.log().write(XLogOperation::Delete {
            key: key.to_string(),
        })?;

        self.btree().delete(&key.to_string())
    }

    fn update(&mut self, key: &str, new_value: T) -> Result<()> {
        self.log().write(XLogOperation::Update {
            key: key.to_string(),
            new_value: new_value.clone(),
        })?;

        self.btree().delete(&key.to_string())?;
        self.btree().insert(key.to_string(), new_value)?;

        Ok(())
    }

    fn iter(&mut self) -> BTreeIterator<'_, String, T> {
        self.btree().iter()
    }

    fn find_by_pattern<'a>(
        &'a mut self,
        pattern: &'a str,
    ) -> Box<(dyn Iterator<Item = BTreePair<String, T>> + 'a)>
    where
        T: 'a,
    {
        Box::new(self.btree().find_pattern(pattern))
    }

    fn rollback(self);
    fn xid(&self) -> u64;
    fn log(&mut self) -> &mut TransactionOperations<T>;
    fn btree(&mut self) -> &mut BTree<String, T>;
}

pub struct TransactionBuilder;

impl TransactionBuilder {
    pub fn branch<T, P>(
        base_path: P,
        collection_name: String,
        mut data: MutexGuard<'_, BTree<String, T>>,
    ) -> Result<impl Transaction<T> + '_>
    where
        T: ValueTrait,
        P: AsRef<Path>,
    {
        let xid = gen_xid();

        let xlog_block = BlockIO::new(base_path.as_ref().join(format!("Data.xlog.{}", xid)))
            .map_err(Error::IoError)?;
        let xact_op = TransactionOperations::new(xlog_block);

        let xact_block = data
            .io
            .copy_to(base_path.as_ref().join(format!("Data.xact.{}", xid)))
            .map_err(Error::IoError)?;
        let btree = BTree::new(xact_block)?;

        Ok(BranchTransaction {
            collection_name,
            xid,
            btree,
            xact_op,
        })
    }

    pub fn lock<T, P>(
        base_path: P,
        collection_name: String,
        mut data: MutexGuard<'_, BTree<String, T>>,
    ) -> Result<impl Transaction<T> + '_>
    where
        T: ValueTrait,
        P: AsRef<Path>,
    {
        let xid = gen_xid();

        let xlog_block = BlockIO::new(base_path.as_ref().join(format!("Data.xlog.{}", xid)))
            .map_err(Error::IoError)?;
        let xact_op = TransactionOperations::new(xlog_block);

        let xact_block = data
            .io
            .copy_to(base_path.as_ref().join(format!("Data.xact.{}", xid)))
            .map_err(Error::IoError)?;
        let btree = BTree::new(xact_block)?;

        Ok(LockTransaction {
            collection_name,
            xid,
            btree,
            xact_op,
            _lock: data,
        })
    }
}

pub fn gen_xid() -> u64 {
    (time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_micros())
    .try_into()
    .unwrap()
}
