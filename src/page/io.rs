use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::Path,
};

use serde::{de::DeserializeOwned, Serialize};

use super::{
    spec::{PageNumber, PAGE_SIZE},
    Page,
};

pub struct BlockIO {
    pub file: File,
}

impl BlockIO {
    pub fn new<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path.as_ref())?;

        Ok(Self { file })
    }

    pub fn write_new_page<T>(&mut self, page: &Page<T>) -> io::Result<u32>
    where
        T: Serialize + DeserializeOwned + PartialOrd + Ord + Clone,
    {
        let new_index = self.pages_in_file()?;

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

    pub fn pages_in_file(&self) -> io::Result<u32> {
        let metadata = self.file.metadata()?;
        let file_size = metadata.len() as u32;

        Ok(file_size / PAGE_SIZE as u32)
    }

    pub fn exists(&self) -> io::Result<bool> {
        let metadata = self.file.metadata()?;

        Ok(metadata.is_file() && metadata.len() != 0)
    }

    pub fn page_exists(&self, page_index: PageNumber) -> io::Result<bool> {
        let metadata = self.file.metadata()?;
        let file_size = metadata.len() as u32;

        Ok(file_size / PAGE_SIZE as u32 > page_index)
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }
}
