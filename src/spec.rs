use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

pub trait KeyTrait: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug {}
impl<T: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug> KeyTrait for T {}

pub trait ValueTrait: Serialize + DeserializeOwned + PartialOrd + Clone + Ord + Debug {}
impl<T: Serialize + DeserializeOwned + PartialOrd + Clone + Ord + Debug> ValueTrait for T {}
