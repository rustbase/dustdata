use crate::bloom;
use crate::error::{Error, Result};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use serde::Deserialize;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{prelude::*, SeekFrom};
use std::{fs, path};

use super::config;

pub struct Storage {
    file: File,
    index: Index,
    filter: Filter,
    storage_path: path::PathBuf,
}

pub struct StorageTupleEntry<T> {
    pub key: String,
    pub value: T,
}

impl Storage {
    pub fn new(config: config::DustDataConfig) -> Result<Self> {
        let storage_path = config.data_path.join("data");

        std::fs::create_dir_all(&storage_path).ok();

        let index = Index::new(
            &storage_path,
            config.storage.compression.is_some(),
            config.storage.compression.as_ref().map(|c| c.level),
        )?;

        let keys = index.index.keys().cloned().collect::<Vec<String>>();

        let filter = Filter::new(keys);

        let (data_chunk_page, data_chunk_id) = Self::data_chunk(&storage_path, &config);
        let file = File::new(&storage_path, data_chunk_page, data_chunk_id)?;

        Ok(Self {
            file,
            filter,
            index,
            storage_path,
        })
    }

    pub fn insert_tuple<T>(&mut self, tuple: StorageTupleEntry<T>) -> Result<()>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static,
    {
        if self.filter.contains(&tuple.key) {
            return Err(Error::AlreadyExists(tuple.key));
        }

        let segment = Storage::serialize_value(&tuple.value);

        self.filter.insert(&tuple.key);
        let offset = self.file.len().unwrap();

        let index_entry = IndexEntry {
            offset,
            data_chunk: DataChunk {
                page: self.file.data_chunk_page,
                id: self.file.data_chunk_id,
            },
        };
        self.index.insert(tuple.key, index_entry);

        self.file.write_all(&segment).map_err(Error::IoError)?;

        Ok(())
    }

    pub fn update_tuple<T>(&mut self, tuple: StorageTupleEntry<T>) -> Result<T>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        if !self.contains(&tuple.key) {
            return Err(Error::NotFound(tuple.key));
        }

        let segment = Storage::serialize_value(&tuple.value);

        let offset = self.file.len().unwrap();

        let index_entry = IndexEntry {
            offset,
            data_chunk: DataChunk {
                page: self.file.data_chunk_page,
                id: self.file.data_chunk_id,
            },
        };
        let old_index_value = self.index.insert(tuple.key, index_entry).unwrap();

        self.file.write_all(&segment).map_err(Error::IoError)?;

        let old_value = self
            .get_tuple_by_offset_and_data_chunk(old_index_value.offset, old_index_value.data_chunk)?
            .unwrap();

        Ok(old_value)
    }

    pub fn remove_tuple<T>(&mut self, key: String) -> Result<T>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        if !self.contains(&key) {
            return Err(Error::NotFound(key));
        }

        self.filter.remove(&key);
        let entry = self.index.remove(key).unwrap();

        let old_value = self
            .get_tuple_by_offset_and_data_chunk(entry.offset, entry.data_chunk)?
            .unwrap();

        Ok(old_value)
    }

    pub fn get_tuple<T>(&self, key: String) -> Result<Option<T>>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        let offset = self.index.get(key);

        if offset.is_none() {
            return Ok(None);
        }

        let entry = offset.unwrap();

        self.get_tuple_by_offset_and_data_chunk(entry.offset, entry.data_chunk)
    }

    pub fn get_tuple_by_offset_and_data_chunk<T>(
        &self,
        offset: u64,
        data_chunk: DataChunk,
    ) -> Result<Option<T>>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        let filename = format!("Data_{}_{}.db", data_chunk.page, data_chunk.id);
        let mut file = fs::OpenOptions::new()
            .read(true)
            .open(self.storage_path.join(filename.clone()))
            .map_err(|r| match r.kind() {
                std::io::ErrorKind::NotFound => Error::CorruptedData(format!(
                    "Data chunk {} not found, but index contains it",
                    filename
                )),
                _ => Error::IoError(r),
            })?;

        Ok(Some(Self::deserialize_value(&mut file, offset, &filename)?))
    }

    pub fn clear(&mut self) -> Result<()> {
        self.filter.clear();
        self.index.clear();

        Ok(())
    }

    pub fn contains(&self, key: &str) -> bool {
        self.filter.contains(key)
    }

    fn serialize_value<T>(value: &T) -> Vec<u8>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static,
    {
        let mut bytes = Vec::new();
        let serialized_value = bincode::serialize(value).unwrap();
        bytes.extend(serialized_value.len().to_le_bytes().iter());
        bytes.extend_from_slice(&serialized_value);

        bytes
    }

    fn deserialize_value<T>(file: &mut fs::File, offset: u64, filename: &str) -> Result<T>
    where
        T: Sync + Send + Clone + Debug + Serialize + 'static + DeserializeOwned,
    {
        file.seek(SeekFrom::Start(offset)).map_err(Error::IoError)?;

        let mut length = [0; 8];
        file.read_exact(&mut length).map_err(Error::IoError)?;
        let length = u64::from_le_bytes(length) as usize;

        let mut value = vec![0; length];
        file.read_exact(&mut value).map_err(Error::IoError)?;

        let value = bincode::deserialize(&value).map_err(|e| {
            Error::CorruptedData(format!(
                "Corrupted data chunk {} and offset {}. Error: {}",
                filename, offset, e
            ))
        })?;

        Ok(value)
    }

    fn data_chunk(path: &path::Path, config: &config::DustDataConfig) -> (usize, usize) {
        let mut data_chunk = 0;
        let mut chunk_index = 0;

        loop {
            let filename = format!("Data_{}_{}.db", data_chunk, chunk_index);
            let file_path = path.join(filename);
            if !file_path.exists() {
                break (data_chunk, chunk_index);
            }

            let metadata = fs::metadata(file_path).unwrap();
            if metadata.len() < config.storage.max_data_chunk_size as u64 {
                break (data_chunk, chunk_index);
            }

            if chunk_index == config.storage.max_data_chunks - 1 {
                data_chunk += 1;
                chunk_index = 0;
            } else {
                chunk_index += 1;
            }
        }
    }
}

struct File {
    file: fs::File,
    data_chunk_page: usize,
    data_chunk_id: usize,
}

impl File {
    pub fn new(path: &path::Path, data_chunk_page: usize, data_chunk_id: usize) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.join(format!("Data_{}_{}.db", data_chunk_page, data_chunk_id)))
            .map_err(Error::IoError)?;

        Ok(Self {
            file,
            data_chunk_page,
            data_chunk_id,
        })
    }

    pub fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.file.write_all(bytes)?;
        Ok(())
    }

    pub fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        self.file.metadata()
    }

    pub fn len(&self) -> std::io::Result<u64> {
        self.metadata().map(|m| m.len())
    }
}

const INDEX_FILENAME: &str = ".index-dustdata";

struct Index {
    index: IndexType,
    path: path::PathBuf,
    use_compression: bool,
    compression_lvl: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
struct IndexEntry {
    offset: u64,
    data_chunk: DataChunk,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct DataChunk {
    page: usize,
    id: usize,
}

type IndexType = HashMap<String, IndexEntry>; // (Data_*_*.db, offset)

impl Index {
    pub fn new(
        data_path: &path::Path,
        use_compression: bool,
        compression_lvl: Option<u32>,
    ) -> Result<Self> {
        let index_path = data_path.join(INDEX_FILENAME);

        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(index_path.clone())
            .map_err(Error::IoError)?;

        let index = if file.metadata().unwrap().len() == 0 {
            let index = IndexType::new();

            let bytes = if use_compression {
                let mut encoder =
                    GzEncoder::new(Vec::new(), Compression::new(compression_lvl.unwrap()));
                encoder
                    .write_all(&bincode::serialize(&index).unwrap())
                    .unwrap();
                encoder.finish().unwrap()
            } else {
                bincode::serialize(&index).unwrap()
            };

            file.write_all(&bytes).map_err(Error::IoError)?;

            index
        } else {
            let mut bytes = Vec::new();

            file.read_to_end(&mut bytes).map_err(Error::IoError)?;

            let mut decoder = GzDecoder::new(&bytes[..]);

            if decoder.header().is_some() {
                let mut decoded_bytes = Vec::new();
                decoder.read_to_end(&mut decoded_bytes).unwrap();

                bincode::deserialize(&decoded_bytes).unwrap()
            } else {
                bincode::deserialize(&bytes).unwrap()
            }
        };

        Ok(Self {
            index,
            path: index_path,
            use_compression,
            compression_lvl,
        })
    }

    pub fn insert(&mut self, key: String, index_entry: IndexEntry) -> Option<IndexEntry> {
        self.index.insert(key, index_entry)
    }

    pub fn remove(&mut self, key: String) -> Option<IndexEntry> {
        self.index.remove(&key)
    }

    pub fn clear(&mut self) {
        self.index.clear();
    }

    pub fn get(&self, key: String) -> Option<IndexEntry> {
        self.index.get(&key).copied()
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        let bytes = bincode::serialize(&self.index).unwrap();

        let bytes = if self.use_compression {
            let mut encoder =
                GzEncoder::new(Vec::new(), Compression::new(self.compression_lvl.unwrap()));
            encoder.write_all(&bytes).unwrap();

            encoder.finish().unwrap()
        } else {
            bytes
        };

        fs::write(&self.path, bytes).unwrap();
    }
}

struct Filter {
    bloom: bloom::BloomFilter,
}

impl Filter {
    pub fn new(keys: Vec<String>) -> Self {
        let mut bloom = bloom::BloomFilter::new(0.01, (keys.len() + 1) * 8);

        for key in keys {
            bloom.insert(&key);
        }

        Self { bloom }
    }

    pub fn insert(&mut self, key: &str) {
        self.bloom.insert(key);
    }

    pub fn contains(&self, key: &str) -> bool {
        self.bloom.contains(key)
    }

    pub fn remove(&mut self, key: &str) {
        self.bloom.remove(key);
    }

    pub fn clear(&mut self) {
        self.bloom.clear();
    }
}
