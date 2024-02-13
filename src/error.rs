use std::fmt::Debug;

pub enum Error {
    IoError(std::io::Error),
    Deadlock,
    DatabaseLocked,
    AlreadyExists(String),
    NotFound(String),
    CorruptedData(String),
    Other(String),
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
            Error::CorruptedData(err) => write!(f, "Corrupted data: {}", err),
            Error::AlreadyExists(message) => write!(f, "{} already exists", message),
            Error::NotFound(message) => write!(f, "{} not found", message),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
