mod bloom;
mod cache;
mod dustdata;
mod storage;

pub use self::{dustdata::DustData, dustdata::DustDataConfig};

/// Initialize the database
/// # Arguments
/// * `config` - A configuration object
/// # Returns
/// - DustData object
pub fn initialize(config: dustdata::DustDataConfig) -> dustdata::DustData {
    dustdata::DustData::new(config)
}
