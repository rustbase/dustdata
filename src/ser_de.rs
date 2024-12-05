use crate::{
    error::{Error, Result},
    CompressionConfig,
};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    io::{Read, Write},
    sync::OnceLock,
};

fn compression_config() -> &'static CompressionConfig {
    static COMPRESSION_CONFIG: OnceLock<CompressionConfig> = OnceLock::new();
    COMPRESSION_CONFIG.get_or_init(CompressionConfig::default)
}

pub fn serialize<T>(data: &T) -> Result<Vec<u8>>
where
    T: Serialize,
{
    let mut bytes = bincode::serialize(data).map_err(Error::SerializeError)?;

    let compression_config = compression_config();

    if compression_config.enabled {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(compression_config.level));
        encoder.write_all(&bytes).map_err(Error::IoError)?;

        bytes = encoder.finish().map_err(Error::IoError)?;
    }

    Ok(bytes)
}

pub fn deserialize<T>(bytes: &[u8]) -> Result<T>
where
    T: DeserializeOwned,
{
    let compression_config = compression_config();
    let mut decoder = GzDecoder::new(bytes);

    if compression_config.enabled && decoder.header().is_some() {
        let mut buffer = Vec::new();
        decoder.read_to_end(&mut buffer).unwrap();

        bincode::deserialize(&buffer).map_err(Error::SerializeError)
    } else {
        bincode::deserialize(bytes).map_err(Error::SerializeError)
    }
}
