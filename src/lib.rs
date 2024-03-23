//! # DustData
//!
//! `dustdata` is a data engine written in Rust. It is designed to be fast, reliable and easy to use.
//! It is a key-value store with support for multiple data types.
//!
//! ## Usage
//! Initialize a new `DustData` instance with the default configuration:
//! ```rust
//! use dustdata::DustData;
//!
//! let mut dustdata = DustData::new(Default::default()).unwrap();
//! ```
//!
//! ## Inserting data into a collection
//!
//! ```rust
//! #[derive(Serialize, Deserialize, Clone, Debug)]
//! struct User {
//!     name: String,
//!     age: u32,
//! }
//!
//! let collection = dustdata.collection::<User>("users");
//!
//! let user = User {
//!     name: "Pedro".to_string(),
//!     age: 21,
//! };
//!
//! // Creating a new transaction.
//! let mut transaction = collection.start();
//!
//! // Inserting the user into the transaction.
//! transaction.insert("user:1", user);
//!
//! // Committing the transaction.
//! collection.commit(&mut transaction).unwrap();
//!
//! // Done!
//! ```
//! ## Reading data from a collection
//!
//! ```rust
//! let collection = dustdata.collection::<User>("users").unwrap();
//!
//! let user = collection.get("user:1").unwrap();
//! ```

pub mod bloom;
pub mod collection;
pub mod config;
pub mod error;

pub use collection::Collection;
pub use config::*;

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
