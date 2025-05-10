use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::Path,
};

use crate::spec::ValueTrait;

use super::{
    cache::{Cache, CacheBuilder},
    layout::BlockHeader,
    spec::{PageNumber, BLOCK_HEADER_SIZE, PAGE_SIZE},
    Page,
};

/// ```plaintext
/// +--------------+ -+- 0x00
/// | Block Header |  |
/// +--------------+ -+- 0x20
/// | Page 00      |
/// +--------------+
/// | Page 01      |
/// +--------------+
/// | Page 02      |
/// +--------------+
/// | Page 03      |
/// +--------------+
/// | Page 04      |
/// +--------------+
/// ```
pub struct Block {
    pub header: BlockHeader,
    file: File,
    cache: Cache,
}

impl Block {
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

    fn from_file(mut file: File) -> io::Result<Self> {
        let cache = CacheBuilder::new().build();

        let header = if is_empty(&file)? {
            let header = BlockHeader::new();

            Self::write_header(&mut file, &header)?;

            header
        } else {
            let header = Self::read_header(&mut file)?;

            if !header.is_valid() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid block header",
                ));
            }

            header
        };

        Ok(Self {
            file,
            cache,
            header,
        })
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

    pub fn write_new_page(&mut self, page: &Page) -> io::Result<u32> {
        let new_index = self.len()?;

        self.write_page(new_index.into(), page)?;

        Ok(new_index)
    }

    pub fn write_page(&mut self, page_index: u64, page: &Page) -> io::Result<()> {
        self.file
            .seek(io::SeekFrom::Start(self.index_to_offset(page_index)))?;

        self.cache.dirty(page_index as u32, page.clone());

        self.file.write_all(&page.to_bytes()?)?;
        self.file.flush()
    }

    pub fn read_page(&mut self, page_index: u64) -> io::Result<Page> {
        if let Some(page) = self.cache.get(page_index as u32) {
            return Ok(page.clone());
        }

        let mut buffer = [0; PAGE_SIZE as usize];

        self.file
            .seek(io::SeekFrom::Start(self.index_to_offset(page_index)))?;

        self.file.read_exact(&mut buffer)?;

        let page = Page::open(buffer).unwrap();

        if let Some(dirty_frame) = self.cache.put(page_index as u32, page.clone()) {
            self.write_page(dirty_frame.page_number as u64, &dirty_frame.page)?;
        }

        Ok(page)
    }

    pub fn page<T>(&mut self, page_index: u64) -> io::Result<Page>
    where
        T: ValueTrait,
    {
        let page = self.read_page(page_index)?;

        Ok(page)
    }

    pub fn len(&self) -> io::Result<u32> {
        let file_size = self.size()?;

        Ok((file_size as u32 - BLOCK_HEADER_SIZE as u32) / PAGE_SIZE as u32)
    }

    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn exists(&self, page_index: PageNumber) -> io::Result<bool> {
        Ok(self.len()? > page_index)
    }

    pub fn index_to_offset(&self, page_index: u64) -> u64 {
        (page_index * PAGE_SIZE as u64) + BLOCK_HEADER_SIZE as u64
    }

    fn size(&self) -> io::Result<u64> {
        let metadata = self.file.metadata()?;

        Ok(metadata.len())
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    pub fn persist_header(&mut self) -> io::Result<()> {
        Self::write_header(&mut self.file, &self.header)?;

        Ok(())
    }

    fn write_header(file: &mut File, header: &BlockHeader) -> io::Result<()> {
        file.seek(io::SeekFrom::Start(0))?;

        file.write_all(&header.to_vec())?;

        Ok(())
    }

    fn read_header(file: &mut File) -> io::Result<BlockHeader> {
        let mut buffer = [0; BLOCK_HEADER_SIZE];

        file.seek(io::SeekFrom::Start(0))?;

        file.read_exact(&mut buffer)?;

        Ok(BlockHeader::from_slice(&buffer))
    }
}

pub fn open_file(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;

    Ok(file)
}

pub fn is_empty(file: &File) -> io::Result<bool> {
    let metadata = file.metadata()?;

    Ok(metadata.is_file() && metadata.len() == 0)
}
