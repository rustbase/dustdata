mod bloom;
mod cache;
mod dustdata;
mod storage;

pub fn initialize(config: dustdata::DustDataConfig) -> dustdata::DustData {
    dustdata::DustData::new(config)
}
