use serde::{Deserialize, Serialize};

use super::spec::{LocationOffset, PageNumber};

#[derive(Serialize, Deserialize, Debug)]
pub struct Overflow {
    pub page_number: PageNumber,
    pub offset: LocationOffset,
}

impl Overflow {
    pub fn new(page_number: PageNumber, offset: LocationOffset) -> Self {
        Self {
            page_number,
            offset,
        }
    }

    pub fn page_number(&self) -> PageNumber {
        self.page_number
    }

    pub fn offset(&self) -> LocationOffset {
        self.offset
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

pub const OVERFLOW_SIZE: usize = 6;
