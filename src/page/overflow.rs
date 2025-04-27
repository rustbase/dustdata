use super::{
    spec::{LocationOffset, PageNumber},
    Error, Result,
};
use serde::{Deserialize, Serialize};

pub const OVERFLOW_POINTER_SIZE: usize = 7;

#[derive(Deserialize, Serialize, Debug)]
pub struct Overflow {
    pub is_overflow: bool,
    pub page: PageNumber,
    pub location: LocationOffset,
}

impl Overflow {
    pub fn to_vec(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self).map_err(Error::SerializeError)
    }

    pub fn from_slice(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data).map_err(Error::SerializeError)
    }
}
