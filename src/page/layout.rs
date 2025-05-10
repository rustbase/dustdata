use super::spec::{LocationOffset, PageNumber, BLOCK_MAGIC_BYTES};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PageHeader {
    pub checksum: u32,
    pub lower: LocationOffset,
    pub upper: LocationOffset,
    pub special: LocationOffset,
    pub flags: u8,
}

impl PageHeader {
    pub fn set(&mut self, flag: PageFlag) {
        self.flags |= flag as u8
    }

    pub fn unset(&mut self, flag: PageFlag) {
        self.flags &= !(flag as u8)
    }

    pub fn has(&self, flag: PageFlag) -> bool {
        self.flags & (flag as u8) != 0
    }

    pub fn not_has(&self, flag: PageFlag) -> bool {
        self.flags & (flag as u8) == 0
    }
}

pub enum PageFlag {
    Full = 0b0000_0001,
    CanCompact = 0b0000_0010,
    OverflowPage = 0b0000_0100,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockHeader {
    pub magic: [u8; 14],
    pub root_page: PageNumber,
    pub free_page_overflow: PageNumber,
}

impl Default for BlockHeader {
    fn default() -> Self {
        Self {
            magic: *BLOCK_MAGIC_BYTES,
            root_page: 0,
            free_page_overflow: 0,
        }
    }
}

impl BlockHeader {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn is_valid(&self) -> bool {
        self.magic == *BLOCK_MAGIC_BYTES
    }

    pub fn to_vec(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn from_slice(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct TuplePointerMetadata {
    pub flags: u8,
}

impl TuplePointerMetadata {
    pub fn to_vec(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn from_slice(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }

    pub fn set(&mut self, flag: TuplePointerFlags) {
        self.flags |= flag as u8
    }

    pub fn unset(&mut self, flag: TuplePointerFlags) {
        self.flags &= !(flag as u8)
    }

    pub fn has(&self, flag: TuplePointerFlags) -> bool {
        self.flags & (flag as u8) != 0
    }

    pub fn not_has(&self, flag: TuplePointerFlags) -> bool {
        self.flags & (flag as u8) == 0
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub enum TuplePointerFlags {
    None = 0b0000_0000,
    Deleted = 0b0000_0001,
    OverflowItem = 0b0000_0010,
}

impl Default for TuplePointerFlags {
    fn default() -> Self {
        Self::None
    }
}
