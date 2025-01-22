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

pub mod btree;
pub mod collection;
pub mod config;
pub mod error;
pub mod page;
mod serializer;

pub use collection::Collection;
pub use config::*;

pub use bincode;
use error::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::fs;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Either<L, R> {
    pub fn left(&self) -> Option<&L> {
        match self {
            Self::Left(l) => Some(l),
            _ => None,
        }
    }

    pub fn right(&self) -> Option<&R> {
        match self {
            Self::Right(r) => Some(r),
            _ => None,
        }
    }

    pub fn is_left(&self) -> bool {
        matches!(self, Self::Left(_))
    }

    pub fn is_right(&self) -> bool {
        matches!(self, Self::Right(_))
    }
}

#[derive(Debug, Clone, Default, Copy)]
pub struct DustData;

impl DustData {
    pub fn new() -> Result<Self> {
        let dustdata_config = dustdata_config();

        fs::create_dir_all(&dustdata_config.data_path).ok();

        Ok(Self)
    }

    pub fn collection<T>(&self, name: &str) -> Result<collection::Collection<T>>
    where
        T: Sync + Send + Clone + Debug + Serialize + DeserializeOwned + 'static + Ord,
    {
        collection::Collection::new(name)
    }

    pub fn drop_collection(&self, name: &str) -> Result<()> {
        let dustdata_config = dustdata_config();

        fs::remove_dir_all(dustdata_config.data_path.join(name))
            .map_err(|_| error::Error::NotFound("collection".to_owned()))?;

        Ok(())
    }
}
