use super::spec::LocationOffset;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PageHeader {
    pub lower: LocationOffset,
    pub upper: LocationOffset,
    pub special: LocationOffset,
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
    Overflow = 0b0000_0010,
}

impl From<u8> for CellPointerFlags {
    fn from(byte: u8) -> Self {
        match byte {
            0b0000_0000 => CellPointerFlags::None,
            0b0000_0001 => CellPointerFlags::Deleted,
            0b0000_0010 => CellPointerFlags::Overflow,
            _ => panic!("Invalid cell pointer metadata: {:#02x}", byte),
        }
    }
}
