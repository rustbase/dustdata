use super::layout::{PageHeader, TuplePointerMetadata};

pub const PAGE_FREE_SPACE_BYTE: u8 = 0x00;
pub const TUPLE_POINTER_SIZE: u16 = 4 + size_of::<TuplePointerMetadata>() as u16;
pub const PAGE_SIZE: u16 = 0x2000;
pub const PAGE_MAGIC_BYTES: &[u8; 13] = b"DUSTDATA PAGE";
pub const PAGE_HEADER_SIZE: usize = size_of::<PageHeader>() + PAGE_MAGIC_BYTES.len();

pub const BLOCK_MAGIC_BYTES: &[u8; 14] = b"DUSTDATA BLOCK";
pub const BLOCK_HEADER_SIZE: usize = 22;

pub type PageNumber = u32;
pub type LocationOffset = u16;
pub type CellId = u64;
