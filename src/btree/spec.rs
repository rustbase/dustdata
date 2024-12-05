use serde::{Deserialize, Serialize};

use crate::page::spec::PageNumber;

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct BTreeCell<K, V> {
    pub left_child: Option<PageNumber>,
    pub key: K,
    pub value: Option<V>,
}

pub const BTREE_PAGE_HEADER_SIZE: u16 = 9;

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd)]
pub struct BTreePageHeader {
    pub kind: PageType,
    pub right_child: Option<PageNumber>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Copy, Clone, PartialOrd, Ord)]
pub enum PageType {
    Leaf = 0,
    Internal = 1,
    Root = 2,
}

impl BTreePageHeader {
    pub fn new(kind: PageType, right_child: Option<PageNumber>) -> Self {
        Self { kind, right_child }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}
