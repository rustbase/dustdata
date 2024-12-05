use std::{
    cmp::Ordering,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
};

use crate::{
    error::{CorruptedDataError, CorruptedDataKind, Error, Result},
    Either,
};
use crc32fast::Hasher;
use layout::{CellPointerFlags, CellPointerMetadata, PageHeader};
use serde::{de::DeserializeOwned, Serialize};
use spec::{LocationOffset, CELL_POINTER_SIZE, PAGE_FREE_SPACE_BYTE, PAGE_HEADER_SIZE, PAGE_SIZE};

pub mod io;
pub mod layout;
pub mod spec;

pub struct Page<T> {
    pub header: PageHeader,
    io: Cursor<[u8; PAGE_SIZE as usize]>,

    _t: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + PartialOrd + Ord + Clone> Page<T> {
    pub fn create(special_size: u16) -> Result<Self> {
        let mut io = Cursor::new([PAGE_FREE_SPACE_BYTE; PAGE_SIZE as usize]);

        let lower = PAGE_HEADER_SIZE as LocationOffset;
        let special = PAGE_SIZE - special_size as LocationOffset;
        let upper = special;

        let header = PageHeader {
            upper,
            lower,
            special,
            checksum: 0,
        };

        let header_bytes = bincode::serialize(&header).map_err(Error::SerializeError)?;
        io.seek(SeekFrom::Start(0)).map_err(Error::IoError)?;
        io.write(&header_bytes).map_err(Error::IoError)?;

        let mut page: Page<T> = Page {
            header,
            io,
            _t: PhantomData,
        };

        // update checksum
        page.write_header()?;

        Ok(page)
    }

    pub fn open(data: [u8; PAGE_SIZE as usize]) -> Result<Self> {
        let mut io = Cursor::new(data);
        let header = Self::read_header(&mut io)?;

        let page = Self {
            io,
            header: header.clone(),
            _t: PhantomData,
        };

        let checksum = page.checksum();

        if header.checksum != checksum {
            return Err(Error::CorruptedData(CorruptedDataError {
                kind: CorruptedDataKind::ChecksumNotMatch,
                message: "checksum does not match".to_string(),
            }));
        }

        Ok(page)
    }

    pub fn write(&mut self, data: T) -> Result<(LocationOffset, LocationOffset)> {
        let data = bincode::serialize(&data).map_err(Error::SerializeError)?;

        // cell_addr is the position of the cell in the page
        let cell_addr: LocationOffset = self.header.upper - data.len() as LocationOffset;

        let cell_pointer_addr = self.header.lower;

        // cell_addr to little endian bytes
        let cell_addr_binary = cell_addr.to_le_bytes();
        let cell_len_binary = (data.len() as u16).to_le_bytes();

        // serialize cell_pointer
        let mut cell_pointer: Vec<u8> = vec![0; CELL_POINTER_SIZE as usize];
        cell_pointer[0..2].copy_from_slice(&cell_addr_binary);
        cell_pointer[2..4].copy_from_slice(&cell_len_binary);
        cell_pointer[4..].copy_from_slice(&CellPointerMetadata::default().to_vec());

        // write to io
        self.io
            .seek(SeekFrom::Start(cell_addr as u64))
            .map_err(Error::IoError)?;
        self.io.write(&data).map_err(Error::IoError)?;

        self.io
            .seek(SeekFrom::Start(cell_pointer_addr as u64))
            .map_err(Error::IoError)?;
        self.io.write(&cell_pointer).map_err(Error::IoError)?;

        // update header
        self.header.upper = cell_addr;
        self.header.lower += CELL_POINTER_SIZE as LocationOffset;
        self.write_header()?;

        // sync file
        // self.io.sync().map_err(Error::IoError)?;

        Ok((cell_addr, cell_pointer_addr as LocationOffset))
    }

    pub fn write_all(&mut self, data: Vec<T>) -> Result<()> {
        for i in data {
            self.write(i)?;
        }

        Ok(())
    }

    pub fn insert(
        &mut self,
        index: LocationOffset,
        data: T,
    ) -> Result<(LocationOffset, LocationOffset)> {
        let data = bincode::serialize(&data).map_err(Error::SerializeError)?;

        let offset = self.index_to_offset(index);

        let cell_addr: LocationOffset = self.header.upper - data.len() as LocationOffset;

        let cell_addr_binary = cell_addr.to_le_bytes();
        let cell_len_binary = (data.len() as u16).to_le_bytes();

        let mut cell_pointer: Vec<u8> = vec![0; CELL_POINTER_SIZE as usize];
        cell_pointer[0..2].copy_from_slice(&cell_addr_binary);
        cell_pointer[2..4].copy_from_slice(&cell_len_binary);
        cell_pointer[4..].copy_from_slice(&CellPointerMetadata::default().to_vec());

        // shift the cells to the right
        let cells_pointers_to_shift_to_right_len =
            if offset < self.header.lower as usize - CELL_POINTER_SIZE as usize {
                self.header.lower as usize - offset
            } else {
                offset
            };

        let mut buffer = vec![0; cells_pointers_to_shift_to_right_len];
        self.io
            .seek(SeekFrom::Start(offset as u64))
            .map_err(Error::IoError)?;
        self.io.read(&mut buffer).map_err(Error::IoError)?;

        // write cells pointers to the right
        self.io
            .seek(SeekFrom::Start(offset as u64 + CELL_POINTER_SIZE as u64))
            .map_err(Error::IoError)?;
        self.io.write(&buffer).map_err(Error::IoError)?;

        let cell_pointer_offset = offset;

        // write the cell data at the cell_addr
        self.io
            .seek(SeekFrom::Start(cell_addr as u64))
            .map_err(Error::IoError)?;
        self.io.write(&data).map_err(Error::IoError)?;

        // write the cell pointer at the cell_pointer_offset
        self.io
            .seek(SeekFrom::Start(cell_pointer_offset as u64))
            .map_err(Error::IoError)?;
        self.io.write(&cell_pointer).map_err(Error::IoError)?;

        // update header
        self.header.upper = cell_addr as LocationOffset;
        self.header.lower += CELL_POINTER_SIZE as LocationOffset;

        self.write_header()?;

        Ok((cell_addr, cell_pointer_offset as LocationOffset))
    }

    pub fn replace(&mut self, index: LocationOffset, data: T) -> Result<T> {
        let data = bincode::serialize(&data).map_err(Error::SerializeError)?;

        let offset = self.index_to_offset(index);

        let old_cell = self.read_at(offset)?.unwrap();
        let (cell_addr, cell_len, _) = self.read_cell_pointer(offset)?;

        if data.len() > cell_len.into() {
            // TODO: overflow
            unimplemented!("overflow on replace")
        }

        let cell_addr_binary = cell_addr.to_le_bytes();
        let cell_len_binary = (data.len() as u16).to_le_bytes();

        let mut cell_pointer: Vec<u8> = vec![0; CELL_POINTER_SIZE as usize];
        cell_pointer[0..2].copy_from_slice(&cell_addr_binary);
        cell_pointer[2..4].copy_from_slice(&cell_len_binary);
        cell_pointer[4..].copy_from_slice(&CellPointerMetadata::default().to_vec());

        // write the cell data at the cell_addr
        self.io
            .seek(SeekFrom::Start(cell_addr as u64))
            .map_err(Error::IoError)?;
        self.io.write(&data).map_err(Error::IoError)?;

        // write the cell pointer at the offset
        self.io
            .seek(SeekFrom::Start(offset as u64))
            .map_err(Error::IoError)?;
        self.io.write(&cell_pointer).map_err(Error::IoError)?;

        // update header
        self.header.upper = cell_addr as LocationOffset;

        self.write_header()?;

        Ok(old_cell)
    }

    pub fn read(&mut self, index: LocationOffset) -> Result<Option<T>> {
        let offset = self.index_to_offset(index);

        self.read_at(offset)
    }

    pub fn read_at(&mut self, offset: usize) -> Result<Option<T>> {
        assert!(offset < self.header.lower as usize, "Index out of bounds");

        // preallocate a buffer to read the page
        let mut buffer = [0; PAGE_SIZE as usize];

        // read the page into the buffer
        self.io.seek(SeekFrom::Start(0)).map_err(Error::IoError)?;
        self.io.read(&mut buffer).map_err(Error::IoError)?;

        // create a cursor to read the buffer
        let mut buffer = Cursor::new(buffer);

        let (cell_addr, cell_len, cell_metadata) = self.read_cell_pointer(offset)?;

        if cell_metadata.flags == CellPointerFlags::Deleted as u8 {
            return Ok(None);
        }

        let mut data = vec![0; cell_len as usize];
        buffer
            .seek(SeekFrom::Start(cell_addr as u64))
            .map_err(Error::IoError)?;
        buffer.read_exact(&mut data).unwrap();

        let data = bincode::deserialize(&data).map_err(Error::SerializeError)?;

        Ok(Some(data))
    }

    fn read_cell_pointer(
        &mut self,
        offset: usize,
    ) -> Result<(LocationOffset, LocationOffset, CellPointerMetadata)> {
        let mut cell_pointer = [0; CELL_POINTER_SIZE as usize];

        self.io
            .seek(SeekFrom::Start(offset as u64))
            .map_err(Error::IoError)?;
        self.io
            .read_exact(&mut cell_pointer)
            .map_err(Error::IoError)?;

        let cell_addr = LocationOffset::from_le_bytes(cell_pointer[0..2].try_into().unwrap());
        let cell_len = LocationOffset::from_le_bytes(cell_pointer[2..4].try_into().unwrap());
        let cell_metadata = CellPointerMetadata::from_slice(&cell_pointer[4..]);

        Ok((cell_addr, cell_len, cell_metadata))
    }

    pub fn binary_search_by<F>(&mut self, mut f: F) -> Either<u16, u16>
    where
        F: FnMut(&T) -> Ordering,
    {
        let mut size = self.len();
        let mut left = 0;
        let mut right = size;

        while left < right {
            let mid: LocationOffset = left + size / 2;

            let data = self.read(mid).unwrap().unwrap();

            match f(&data) {
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid,
                Ordering::Equal => return Either::Left(mid),
            }

            size = right - left;
        }

        Either::Right(left)
    }

    pub fn binary_search(&mut self, x: &T) -> Either<u16, u16> {
        self.binary_search_by(|a| a.cmp(x))
    }

    pub fn binary_search_by_key<B, F>(&mut self, b: &B, mut f: F) -> Either<u16, u16>
    where
        F: FnMut(&T) -> B,
        B: Ord,
    {
        self.binary_search_by(|k| f(k).cmp(b))
    }

    pub fn delete(&mut self, index: LocationOffset) -> Result<()> {
        let offset = self.index_to_offset(index);

        self.delete_at(offset)
    }

    pub fn delete_at(&mut self, offset: usize) -> Result<()> {
        let cell_pointer_metadata_offset = offset + 4;

        self.io
            .seek(SeekFrom::Start(cell_pointer_metadata_offset as u64))
            .map_err(Error::IoError)?;
        self.io
            .write(&[CellPointerFlags::Deleted as u8])
            .map_err(Error::IoError)?;

        self.write_header()?;

        Ok(())
    }

    pub fn delete_range<R>(&mut self, range: R) -> Result<()>
    where
        R: Iterator<Item = u16>,
    {
        for i in range {
            let offset = self.index_to_offset(i);

            let cell_pointer_metadata_offset = offset + 4;

            self.io
                .seek(SeekFrom::Start(cell_pointer_metadata_offset as u64))
                .map_err(Error::IoError)?;
            self.io
                .write(&[CellPointerFlags::Deleted as u8])
                .map_err(Error::IoError)?;
        }

        self.write_header()?;

        Ok(())
    }

    pub fn compact(mut self) -> Result<Self> {
        let data = self.values()?;
        let mut page = Self::create(self.special_size())?;
        page.write_special(&self.read_special()?)?;

        for data in data {
            page.write(data)?;
        }

        Ok(page)
    }

    pub fn values(&mut self) -> Result<Vec<T>> {
        let mut data = Vec::new();

        for i in 0..self.len() {
            if let Some(cell) = self.read(i)? {
                data.push(cell);
            }
        }

        Ok(data)
    }

    pub fn split_at(&mut self, index: LocationOffset) -> Result<(Vec<T>, Vec<T>)> {
        let values = self.values()?;
        let split = values.split_at(index as usize);

        Ok((split.0.to_vec(), split.1.to_vec()))
    }

    pub fn split_off(&mut self, index: LocationOffset) -> Result<Vec<T>> {
        let mut values = self.values()?;
        let values = values.split_off(index.into());

        self.delete_range(index..self.len())?;

        Ok(values)
    }

    pub fn write_special(&mut self, data: &[u8]) -> Result<()> {
        assert!(data.len() as LocationOffset <= self.special_size());

        let special = self.header.special as usize;

        self.io
            .seek(SeekFrom::Start(special as u64))
            .map_err(Error::IoError)?;
        self.io.write(data).map_err(Error::IoError)?;

        Ok(())
    }

    pub fn read_special(&mut self) -> Result<Vec<u8>> {
        let mut buffer = vec![0; self.special_size() as usize];

        self.io
            .seek(SeekFrom::Start(self.header.special as u64))
            .map_err(Error::IoError)?;
        self.io.read(&mut buffer).map_err(Error::IoError)?;

        Ok(buffer)
    }

    pub fn remaining_space(&self) -> u16 {
        self.header.upper - self.header.lower
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> u16 {
        (self.header.lower - self.header_size() as LocationOffset)
            / CELL_POINTER_SIZE as LocationOffset
    }

    pub fn index_to_offset(&self, index: LocationOffset) -> usize {
        self.header_size() + (index as usize * CELL_POINTER_SIZE as usize)
    }

    pub fn to_bytes(&self) -> std::io::Result<[u8; PAGE_SIZE as usize]> {
        let buffer = self.io.clone().into_inner();

        Ok(buffer)
    }

    fn header_size(&self) -> usize {
        PAGE_HEADER_SIZE
    }

    pub fn special_size(&self) -> u16 {
        PAGE_SIZE - self.header.special
    }

    fn write_header(&mut self) -> Result<()> {
        let checksum = self.checksum();
        self.header.checksum = checksum;

        let buffer = bincode::serialize(&self.header).map_err(Error::SerializeError)?;

        self.io.seek(SeekFrom::Start(0)).map_err(Error::IoError)?;
        self.io.write(&buffer).map_err(Error::IoError)?;

        Ok(())
    }

    fn read_header(io: &mut Cursor<[u8; PAGE_SIZE as usize]>) -> Result<PageHeader> {
        let mut buffer = vec![0; PAGE_HEADER_SIZE];

        io.seek(SeekFrom::Start(0)).map_err(Error::IoError)?;
        io.read(&mut buffer).map_err(Error::IoError)?;

        bincode::deserialize(&buffer).map_err(Error::SerializeError)
    }

    fn checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.io.get_ref()[PAGE_HEADER_SIZE..]);
        hasher.finalize()
    }
}

#[cfg(test)]
mod page_tests {
    use super::*;

    #[test]
    fn create_page() {
        let mut page = Page::<u32>::create(0).unwrap();

        page.write(32).unwrap();
        page.write(54).unwrap();

        let num1 = page.read(0).unwrap().unwrap();
        let num2 = page.read(1).unwrap().unwrap();

        assert_eq!(num1, 32);
        assert_eq!(num2, 54);
    }

    #[test]
    fn delete_values_in_page() {
        let mut page = Page::<bool>::create(0).unwrap();

        page.write(true).unwrap();
        page.write(false).unwrap();
        page.write(true).unwrap();

        let value = page.read(1).unwrap().unwrap();

        assert!(!value);

        page.delete(1).unwrap();

        let value = page.read(1).unwrap();
        let len = page.len();

        assert_eq!(value, None);
        assert_eq!(len, 3);

        let page = page.compact().unwrap();
        let len = page.len();

        assert_eq!(len, 2)
    }

    #[test]
    fn insert_value_page() {
        let mut page = Page::<String>::create(0).unwrap();

        page.write("first value".to_string()).unwrap();
        page.write("second value".to_string()).unwrap();
        page.write("third value".to_string()).unwrap();

        let value = page.read(1).unwrap().unwrap();

        assert_eq!(value, "second value".to_string());

        page.insert(1, "inserted value".to_string()).unwrap();

        let value = page.read(1).unwrap().unwrap();

        assert_eq!(value, "inserted value".to_string());

        let value = page.read(2).unwrap().unwrap();

        assert_eq!(value, "second value".to_string());

        let len = page.len();

        assert_eq!(len, 4)
    }

    #[test]
    fn binary_search_page_test() {
        let mut page = Page::<(u32, String)>::create(0).unwrap();

        page.write((1, "Pedro".to_string())).unwrap();
        page.write((2, "John".to_string())).unwrap();
        page.write((5, "Ana".to_string())).unwrap();
        page.write((8, "Jane".to_string())).unwrap();
        page.write((10, "Beatriz".to_string())).unwrap();

        let found = page.binary_search_by_key(&8, |e| e.0);

        assert_eq!(found, Either::Left(3));

        let found_value = page.read(*found.left().unwrap()).unwrap().unwrap();

        assert_eq!(found_value, (8, "Jane".to_string()));

        let found = page.binary_search_by_key(&9, |e| e.0);

        assert_eq!(found, Either::Right(4));
    }

    #[test]
    fn special_size_page_test() {
        let mut page = Page::<u32>::create(4).unwrap();

        page.write(32).unwrap();
        page.write(16).unwrap();

        page.write_special(&[20, 10, 5, 2]).unwrap();

        let value = page.read(0).unwrap().unwrap();

        assert_eq!(value, 32);

        let special = page.read_special().unwrap();

        assert_eq!(special, vec![20, 10, 5, 2])
    }

    #[test]
    fn replace_page_test() {
        let mut page = Page::<u32>::create(0).unwrap();

        page.write(42).unwrap();
        page.write(15).unwrap();

        let value = page.read(0).unwrap().unwrap();

        assert_eq!(value, 42);

        page.replace(0, 90).unwrap();

        let value = page.read(0).unwrap().unwrap();

        assert_eq!(value, 90);
    }

    #[test]
    fn page_checksum() {
        let mut page = Page::<u32>::create(0).unwrap();

        page.write(99).unwrap();

        let mut page_bytes = page.to_bytes().unwrap();
        //change a random byte
        page_bytes[26] = 2u8;

        Page::<u32>::open(page_bytes).err().unwrap();
    }
}
