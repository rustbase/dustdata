use super::layout::{CellPointerMetadata, PageHeader};

pub const PAGE_FREE_SPACE_BYTE: u8 = 0x00;
pub const CELL_POINTER_SIZE: u16 = 4 + std::mem::size_of::<CellPointerMetadata>() as u16;
pub const PAGE_SIZE: u16 = 0x1000;
pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<PageHeader>();

pub type PageNumber = u32;
pub type LocationOffset = u16;
