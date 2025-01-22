use fs2::FileExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    mem,
    path::Path,
};

use super::{
    spec::{PageNumber, PAGE_SIZE},
    Page,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockMetadata {
    pub last_page_overflow: Option<PageNumber>,
}

pub const BLOCK_METADATA_SIZE: usize = mem::size_of::<BlockMetadata>();

pub struct BlockIO {
    file: File,
}

impl BlockIO {
    pub fn new<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }

        let file = open_file(path.as_ref())?;

        Self::from_file(file)
    }

    fn from_file(file: File) -> io::Result<Self> {
        let mut block = Self { file };

        if block.file.metadata()?.len() == 0 {
            let metadata_page = Page::<()>::create(BLOCK_METADATA_SIZE as u16).unwrap();

            block.write_page(0, &metadata_page)?;
        }

        Ok(block)
    }

    pub fn copy_to<P>(&mut self, path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }

        let mut new_file = open_file(path.as_ref())?;

        let mut buffer = [0u8; 8192];

        self.file.seek(io::SeekFrom::Start(0))?;

        while let Ok(n) = self.file.read(&mut buffer) {
            if n == 0 {
                break;
            }

            new_file.write_all(&buffer[..n])?;
        }

        Self::from_file(new_file)
    }

    pub fn write_new_page<T>(&mut self, page: &Page<T>) -> io::Result<u32>
    where
        T: Serialize + DeserializeOwned + PartialOrd + Ord + Clone,
    {
        let new_index = self.len()?;

        self.write_page(new_index.into(), page)?;

        Ok(new_index)
    }

    pub fn write_page<T>(&mut self, page_index: u64, page: &Page<T>) -> io::Result<()>
    where
        T: Serialize + DeserializeOwned + PartialOrd + Ord + Clone,
    {
        self.file
            .seek(io::SeekFrom::Start(page_index * PAGE_SIZE as u64))?;

        self.file.write_all(&page.to_bytes()?)
    }

    pub fn read_page<T>(&mut self, page_index: u64) -> io::Result<Page<T>>
    where
        T: Serialize + DeserializeOwned + PartialOrd + Ord + Clone,
    {
        let mut buffer = [0; PAGE_SIZE as usize];

        self.file
            .seek(io::SeekFrom::Start(page_index * PAGE_SIZE as u64))?;

        self.file.read_exact(&mut buffer)?;

        Ok(Page::open(buffer).unwrap())
    }

    pub fn read_metadata_page<T>(&mut self) -> io::Result<Page<T>>
    where
        T: Serialize + DeserializeOwned + PartialOrd + Ord + Clone,
    {
        let mut buffer = [0; PAGE_SIZE as usize];

        self.file.seek(io::SeekFrom::Start(0))?;

        self.file.read_exact(&mut buffer)?;

        Ok(Page::open(buffer).unwrap())
    }

    pub fn write_metadata_page<T>(&mut self, page: &Page<T>) -> io::Result<()>
    where
        T: Serialize + DeserializeOwned + PartialOrd + Ord + Clone,
    {
        self.file.seek(io::SeekFrom::Start(0))?;

        self.file.write_all(&page.to_bytes()?)
    }

    pub fn len(&self) -> io::Result<u32> {
        let file_size = self.file_size()? as u32;

        Ok(file_size / PAGE_SIZE as u32)
    }

    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn exists(&self) -> io::Result<bool> {
        let metadata = self.file.metadata()?;

        Ok(metadata.is_file() && metadata.len() != 0)
    }

    pub fn page_exists(&self, page_index: PageNumber) -> io::Result<bool> {
        let file_size = self.file_size()? as u32;

        Ok(file_size / PAGE_SIZE as u32 > page_index)
    }

    fn file_size(&self) -> io::Result<u64> {
        let metadata = self.file.metadata()?;

        Ok(metadata.len())
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }
}

pub fn open_file(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;

    file.lock_exclusive()?;

    Ok(file)
}

impl Drop for BlockIO {
    fn drop(&mut self) {
        self.file.unlock().unwrap();
    }
}
