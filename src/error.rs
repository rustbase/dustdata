use std::fmt::Debug;

pub enum Error {
    IoError(std::io::Error),
    SerializeError(bincode::Error),
    Deadlock,
    DatabaseLocked,
    AlreadyExists(String),
    NotFound(String),
    CorruptedData(CorruptedDataError),
    Other(String),
    Cannot(String),
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IoError(err) => write!(f, "IO Error: {}", err),
            Error::Deadlock => write!(f, "Deadlock"),
            Error::DatabaseLocked => {
                write!(f, "Database is locked, maybe another instance is running?")
            }
            Error::Other(err) => write!(f, "Other Error: {}", err),
            Error::CorruptedData(err) => write!(f, "{:?}", err),
            Error::AlreadyExists(message) => write!(f, "{} already exists", message),
            Error::NotFound(message) => write!(f, "{} not found", message),
            Error::Cannot(message) => write!(f, "cannot {}", message),
            Error::SerializeError(error) => write!(f, "serialize error {}", error),
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
