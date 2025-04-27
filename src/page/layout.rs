use super::spec::{LocationOffset, PageNumber, BLOCK_MAGIC_BYTES};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PageHeader {
    pub checksum: u32,
    pub lower: LocationOffset,
    pub upper: LocationOffset,
    pub special: LocationOffset,
    pub next_page: Option<u64>,
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
}

pub enum PageFlag {
    Full = 0b0000_0001,
    CanCompact = 0b0000_0010,
    OverflowPage = 0b0000_0100,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockHeader {
    pub magic: [u8; 14],
    pub root_page: Option<PageNumber>,
    pub first_free_page_overflow: Option<PageNumber>,
}

impl Default for BlockHeader {
    fn default() -> Self {
        Self {
            magic: *BLOCK_MAGIC_BYTES,
            root_page: None,
            first_free_page_overflow: None,
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct CellPointerMetadata {
    pub flags: u8,
    pub has_overflow: bool,
}

impl CellPointerMetadata {
    pub fn to_vec(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn from_slice(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

#[derive(Default, PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub enum CellPointerFlags {
    #[default]
    None = 0b0000_0000,
    Deleted = 0b0000_0001,
}

impl From<u8> for CellPointerFlags {
    fn from(byte: u8) -> Self {
        match byte {
            0b0000_0000 => CellPointerFlags::None,
            0b0000_0001 => CellPointerFlags::Deleted,
            _ => panic!("Invalid cell pointer metadata: {:#02x}", byte),
        }
    }
}
