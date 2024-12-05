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
    data_file: File,
    cell_overflow_file: File,
    page_overflow_file: File,
    alloc_size: u16,
}

impl BlockIO {
    pub fn new<P>(path: P, alloc_size: u16) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        fs::create_dir_all(path)?;
        let block_name = path.file_name().unwrap().to_str().unwrap();

        let mut data_file = open_file(&path.join(format!("{}_data.db", block_name)))?;
        let page_overflow_file = open_file(&path.join(format!("{}_p_ovf.db", block_name)))?;
        let cell_overflow_file = open_file(&path.join(format!("{}_c_ovf.db", block_name)))?;

        if data_file.metadata()?.len() == 0 {
            data_file.write_all(&vec![0u8; alloc_size as usize])?;
        }

        Ok(Self {
            data_file,
            page_overflow_file,
            cell_overflow_file,
            alloc_size,
        })
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
        self.data_file.seek(io::SeekFrom::Start(
            page_index * PAGE_SIZE as u64 + self.alloc_size as u64,
        ))?;
        self.data_file.write_all(&page.to_bytes()?)
    }

    pub fn read_page<T>(&mut self, page_index: u64) -> io::Result<Page<T>>
    where
        T: Serialize + DeserializeOwned + PartialOrd + Ord + Clone,
    {
        let mut buffer = [0; PAGE_SIZE as usize];

        self.data_file.seek(io::SeekFrom::Start(
            page_index * PAGE_SIZE as u64 + self.alloc_size as u64,
        ))?;
        self.data_file.read_exact(&mut buffer)?;

        Ok(Page::open(buffer).unwrap())
    }

    pub fn pages_in_file(&self) -> io::Result<u32> {
        let file_size = self.file_size()? as u32;

        Ok(file_size / PAGE_SIZE as u32)
    }

    pub fn exists(&self) -> io::Result<bool> {
        let metadata = self.data_file.metadata()?;

        Ok(metadata.is_file() && metadata.len() - self.alloc_size as u64 != 0)
    }

    pub fn page_exists(&self, page_index: PageNumber) -> io::Result<bool> {
        let file_size = self.file_size()? as u32;

        Ok(file_size / PAGE_SIZE as u32 > page_index)
    }

    pub fn alloc_data(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.data_file.seek(io::SeekFrom::Start(0))?;
        self.data_file.write_all(bytes)
    }

    pub fn read_alloc_data(&mut self) -> io::Result<Vec<u8>> {
        self.data_file.seek(io::SeekFrom::Start(0))?;
        let mut buffer = vec![0u8; self.alloc_size as usize];
        self.data_file.read_exact(&mut buffer)?;

        Ok(buffer)
    }

    fn file_size(&self) -> io::Result<u64> {
        let metadata = self.data_file.metadata()?;

        Ok(metadata.len() - self.alloc_size as u64)
    }

    pub fn sync(&self) -> io::Result<()> {
        self.data_file.sync_data()
    }
}

pub fn open_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
}
