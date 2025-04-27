use super::layout::{BlockHeader, CellPointerMetadata, PageHeader};

pub const PAGE_FREE_SPACE_BYTE: u8 = 0x00;
pub const CELL_POINTER_SIZE: u16 = 4 + std::mem::size_of::<CellPointerMetadata>() as u16;
pub const PAGE_SIZE: u16 = 0x2000;
pub const PAGE_MAGIC_BYTES: &[u8; 13] = b"DUSTDATA PAGE";
pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<PageHeader>() + PAGE_MAGIC_BYTES.len();

pub const BLOCK_MAGIC_BYTES: &[u8; 14] = b"DUSTDATA BLOCK";
pub const BLOCK_HEADER_SIZE: usize = std::mem::size_of::<BlockHeader>();

pub type PageNumber = u32;
pub type LocationOffset = u16;
pub type CellId = u64;
