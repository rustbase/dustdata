pub mod bloom;
pub mod collection;
pub mod config;
pub mod error;

pub use collection::Collection;
pub use config::{DustDataConfig, StorageConfig};

pub use bincode;
use error::Result;
use fs2::FileExt;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::fs;

pub struct DustData {
    config: config::DustDataConfig,
}

impl DustData {
    pub fn new(config: config::DustDataConfig) -> Result<Self> {
        fs::create_dir_all(&config.data_path).ok();

        if !config.data_path.join(".dustdata-lock").exists() {
            let file = fs::File::create(config.data_path.join(".dustdata-lock")).unwrap();

            file.lock_exclusive().unwrap();
        }

        Ok(Self { config })
    }

    pub fn collection<T>(&self, name: &str) -> collection::Collection<T>
    where
        T: Sync + Send + Clone + Debug + Serialize + DeserializeOwned + 'static,
    {
        let mut config = self.config.clone();
        config.data_path.push(name);

        collection::Collection::new(config)
    }
}

impl Drop for DustData {
    fn drop(&mut self) {
        let file = fs::File::open(self.config.data_path.join(".dustdata-lock")).unwrap();

        file.unlock().unwrap();
    }
}
