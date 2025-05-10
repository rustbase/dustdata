use std::{fmt::Debug, io};

pub enum Error {
    Io(io::Error),
    Parsing(bincode::Error),
    Corrupted(CorruptedDataError),
    NotEnoughSpace,
    Other(String),
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(err) => write!(f, "{err}"),
            Error::Parsing(err) => write!(f, "{err}"),
            Error::Corrupted(msg) => write!(f, "Corrupted data: {:?}", msg),
            Error::NotEnoughSpace => write!(f, "Not enough space"),
            Error::Other(msg) => write!(f, "{}", msg),
        }
    }
}

pub struct CorruptedDataError {
    pub kind: CorruptedDataKind,
    pub message: String,
}

#[derive(Debug)]
pub enum CorruptedDataKind {
    ChecksumNotMatch,
    UnsyncWithWAL,
}

impl Debug for CorruptedDataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Corrupted data: {}. {:?}", self.message, self.kind)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
