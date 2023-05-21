#[derive(Debug)]
pub struct Error {
    pub code: ErrorKind,
}

impl Error {
    pub fn new(code: ErrorKind) -> Self {
        Self { code }
    }
}

#[derive(Debug)]
pub enum ErrorKind {
    AlreadyExists,
    KeyNotFound,
    IoError,
    Corrupted,
    Other,
}

pub type Result<T> = std::result::Result<T, Error>;
