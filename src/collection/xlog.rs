use crate::{
    btree::{BTree, BTreeIterator, ValueTrait},
    error::{Error, Result},
    page::io::BlockIO,
};

use serde::{Deserialize, Serialize};
use std::{fs, ops::RangeBounds, path::Path};

pub const XLOG_FILENAME: &str = ".xlog";

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub enum XLogOperation<T> {
    Insert { key: String, value: T },
    Update { key: String, new_value: T },
    Delete { key: String },
    Drop,
}

pub struct XLog<T: ValueTrait> {
    btree: BTree<u64, Vec<XLogOperation<T>>>,
}

impl<T: ValueTrait> XLog<T> {
    pub fn new<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        fs::create_dir_all(&path).ok();
        let log_path = path.as_ref().join(XLOG_FILENAME);
        let io = BlockIO::new(log_path).map_err(Error::IoError)?;

        let btree = BTree::new(io)?;

        Ok(Self { btree })
    }

    pub fn write(&mut self, xid: u64, ops: Vec<XLogOperation<T>>) -> Result<()> {
        self.btree.insert(xid, ops)
    }

    pub fn read(&mut self, xid: u64) -> Result<Option<Vec<XLogOperation<T>>>> {
        self.btree.get(&xid)
    }

    pub fn range<R>(&mut self, range: R) -> Result<BTreeIterator<'_, u64, Vec<XLogOperation<T>>>>
    where
        R: RangeBounds<u64>,
    {
        self.btree.range(range)
    }

    pub fn head_id(&mut self) -> Option<u64> {
        self.btree.iter().last().map(|c| c.key)
    }
}
