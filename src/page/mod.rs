use std::{
    cmp::Ordering,
    io::{Cursor, Read, Seek, SeekFrom, Write},
};

use crate::{
    error::{CorruptedDataError, CorruptedDataKind, Error, Result},
    Either,
};
use crc32fast::Hasher;
use layout::{PageFlag, PageHeader, TuplePointerFlags, TuplePointerMetadata};
use spec::{
    LocationOffset, PAGE_FREE_SPACE_BYTE, PAGE_HEADER_SIZE, PAGE_MAGIC_BYTES, PAGE_SIZE,
    TUPLE_POINTER_SIZE,
};

pub mod block;
pub mod cache;
pub mod layout;
pub mod overflow;
pub mod paging;
pub mod spec;

#[derive(Default)]
pub struct PageBuilder {
    pub special_size: u16,
    pub flags: u8,
}

impl PageBuilder {
    pub fn new() -> Self {
        Self {
            special_size: 0,
            flags: 0,
        }
    }

    pub fn special_size(mut self, size: u16) -> Self {
        self.special_size = size;
        self
    }

    pub fn set(mut self, flag: PageFlag) -> Self {
        self.flags |= flag as u8;
        self
    }

    pub fn unset(mut self, flag: PageFlag) -> Self {
        self.flags &= !(flag as u8);
        self
    }

    pub fn has(&self, flag: PageFlag) -> bool {
        self.flags & (flag as u8) != 0
    }

    pub fn build(self) -> Result<Page> {
        Page::create(self.special_size, self.flags)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct TuplePointer {
    pub addr: LocationOffset,
    pub len: LocationOffset,
    pub metadata: TuplePointerMetadata,
}

impl TuplePointer {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0; TUPLE_POINTER_SIZE as usize];
        bytes[0..2].copy_from_slice(&self.addr.to_le_bytes());
        bytes[2..4].copy_from_slice(&self.len.to_le_bytes());
        bytes[4..].copy_from_slice(&self.metadata.to_vec());

        bytes
    }
}

/// Slotted page layout
/// ```text
///             tuple_POINTER_SIZE
///                    |                           +-> header.lower
/// PAGE_HEADER_SIZE   |                           |
/// |--------|------------------|                  V                    
/// +--------+-------------------+-----------------+-----------------+ -+
/// | header |  tuple pointer 01  | tuple pointer 02 | --->            |  |
/// +--------+-------------------+-----------------+                 |  |
/// |                        (Free space)                            |  +- PAGE_SIZE (4096 bytes)
/// |          +-----------------+-----------------+-----------------+  |
/// |     <--- | tuple data 02    | tuple data 01    |  special space  |  |
/// +----------+-----------------+-----------------+-----------------+ -+
///            ^
///            |
///            +-> header.upper
/// ```
/// Header: contains metadata about the page, such as the type of the page, the lower and upper pointers.
///
/// Additional header: additional metadata about the page.
///
/// tuple pointer: contains the position of the tuple in the page, the length of the tuple, and the metadata of the tuple.
/// tuple data: the actual data of the tuple.
///
/// The lower pointer points to the end of the tuple pointers.
/// The upper pointer points to the end of the tuple data.
///
#[derive(Clone)]
pub struct Page {
    pub header: PageHeader,
    io: Cursor<[u8; PAGE_SIZE as usize]>,
}

pub type PageSplit = (Vec<Vec<u8>>, Vec<Vec<u8>>);

impl Page {
    pub fn create(special_size: u16, flags: u8) -> Result<Self> {
        let mut io = Cursor::new([PAGE_FREE_SPACE_BYTE; PAGE_SIZE as usize]);

        let lower = PAGE_HEADER_SIZE as LocationOffset;
        let special = PAGE_SIZE - special_size as LocationOffset;
        let upper = special;

        let header = PageHeader {
            upper,
            lower,
            special,
            checksum: 0,
            flags,
        };

        let header_bytes = bincode::serialize(&header).map_err(Error::Parsing)?;
        io.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
        io.write(&[PAGE_MAGIC_BYTES, header_bytes.as_slice()].concat())
            .map_err(Error::Io)?;

        let mut page: Page = Page { header, io };

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
        };

        let checksum = page.checksum();

        if header.checksum != checksum {
            return Err(Error::Corrupted(CorruptedDataError {
                kind: CorruptedDataKind::ChecksumNotMatch,
                message: "checksum does not match".to_string(),
            }));
        }

        Ok(page)
    }

    pub fn write(&mut self, data: &[u8], flags: u8) -> Result<(LocationOffset, LocationOffset)> {
        assert!(
            (data.len() as LocationOffset)
                < (self.free_space() + TUPLE_POINTER_SIZE as LocationOffset),
            "not enough space to write data"
        );

        // tuple_addr is the position of the tuple in the page
        let tuple_data_addr: LocationOffset = self.header.upper - data.len() as LocationOffset;

        let tuple_pointer_addr = self.header.lower;

        // tuple_addr to little endian bytes
        let tuple_addr_binary = tuple_data_addr.to_le_bytes();
        let tuple_len_binary = (data.len() as u16).to_le_bytes();

        // serialize tuple_pointer
        let mut tuple_pointer: Vec<u8> = vec![0; TUPLE_POINTER_SIZE as usize];
        tuple_pointer[0..2].copy_from_slice(&tuple_addr_binary);
        tuple_pointer[2..4].copy_from_slice(&tuple_len_binary);
        tuple_pointer[4..].copy_from_slice(&[flags]);

        // write to io
        self.io
            .seek(SeekFrom::Start(tuple_data_addr as u64))
            .map_err(Error::Io)?;
        self.io.write(data).map_err(Error::Io)?;

        self.io
            .seek(SeekFrom::Start(tuple_pointer_addr as u64))
            .map_err(Error::Io)?;
        self.io.write(&tuple_pointer).map_err(Error::Io)?;

        // update header
        self.header.upper = tuple_data_addr;
        self.header.lower += TUPLE_POINTER_SIZE as LocationOffset;
        self.write_header()?;

        // sync file
        // self.io.sync().map_err(Error::Io)?;

        Ok((tuple_data_addr, self.len() - 1 as LocationOffset))
    }

    pub fn write_all(&mut self, data: &[&[u8]]) -> Result<()> {
        for i in data {
            self.write(i, 0)?;
        }

        Ok(())
    }

    pub fn insert(
        &mut self,
        index: LocationOffset,
        data: &[u8],
        flags: u8,
    ) -> Result<(LocationOffset, LocationOffset)> {
        let offset = self.index_to_offset(index);

        let tuple_addr: LocationOffset = self.header.upper - data.len() as LocationOffset;

        let tuple_addr_binary = tuple_addr.to_le_bytes();
        let tuple_len_binary = (data.len() as u16).to_le_bytes();

        let mut tuple_pointer: Vec<u8> = vec![0; TUPLE_POINTER_SIZE as usize];
        tuple_pointer[0..2].copy_from_slice(&tuple_addr_binary);
        tuple_pointer[2..4].copy_from_slice(&tuple_len_binary);
        tuple_pointer[4..].copy_from_slice(&[flags]);

        // shift the tuples to the right
        let tuples_pointers_to_shift_to_right_len =
            if offset < self.header.lower as usize - TUPLE_POINTER_SIZE as usize {
                self.header.lower as usize - offset
            } else {
                offset
            };

        let mut buffer = vec![0; tuples_pointers_to_shift_to_right_len];
        self.io
            .seek(SeekFrom::Start(offset as u64))
            .map_err(Error::Io)?;
        self.io.read(&mut buffer).map_err(Error::Io)?;

        // write tuples pointers to the right
        self.io
            .seek(SeekFrom::Start(offset as u64 + TUPLE_POINTER_SIZE as u64))
            .map_err(Error::Io)?;
        self.io.write(&buffer).map_err(Error::Io)?;

        let tuple_pointer_offset = offset;

        // write the tuple data at the tuple_addr
        self.io
            .seek(SeekFrom::Start(tuple_addr as u64))
            .map_err(Error::Io)?;
        self.io.write(data).map_err(Error::Io)?;

        // write the tuple pointer at the tuple_pointer_offset
        self.io
            .seek(SeekFrom::Start(tuple_pointer_offset as u64))
            .map_err(Error::Io)?;
        self.io.write(&tuple_pointer).map_err(Error::Io)?;

        // update header
        self.header.upper = tuple_addr as LocationOffset;
        self.header.lower += TUPLE_POINTER_SIZE as LocationOffset;

        self.write_header()?;

        Ok((tuple_addr, tuple_pointer_offset as LocationOffset))
    }

    pub fn replace(
        &mut self,
        index: LocationOffset,
        data: &[u8],
    ) -> Result<(Vec<u8>, TuplePointer)> {
        let offset = self.index_to_offset(index);

        let tuple = self.read_at(offset)?.unwrap();
        let tuple_pointer = self.tuple_pointer(offset)?;

        if data.len() > tuple_pointer.len.into() {
            // TODO: overflow
            unimplemented!("overflow on replace")
        }

        let new_tuple_pointer = TuplePointer {
            addr: tuple_pointer.addr,
            len: data.len() as u16,
            metadata: tuple_pointer.metadata,
        };

        // write the tuple data at the tuple_addr
        self.io
            .seek(SeekFrom::Start(tuple_pointer.addr as u64))
            .map_err(Error::Io)?;
        self.io.write(data).map_err(Error::Io)?;

        // write the tuple pointer at the offset
        self.io
            .seek(SeekFrom::Start(offset as u64))
            .map_err(Error::Io)?;
        self.io
            .write(&new_tuple_pointer.to_bytes())
            .map_err(Error::Io)?;

        // update header
        self.header.upper = tuple_pointer.addr as LocationOffset;

        self.write_header()?;

        Ok(tuple)
    }

    pub fn read(&mut self, index: LocationOffset) -> Result<Option<(Vec<u8>, TuplePointer)>> {
        let offset = self.index_to_offset(index);

        self.read_at(offset)
    }

    pub fn read_at(&mut self, offset: usize) -> Result<Option<(Vec<u8>, TuplePointer)>> {
        if offset >= self.header.lower as usize {
            return Ok(None);
        }

        let tuple_pointer = self.tuple_pointer(offset)?;

        if tuple_pointer.metadata.has(TuplePointerFlags::Deleted) {
            return Ok(None);
        }

        let mut data = vec![0; tuple_pointer.len as usize];
        self.io
            .seek(SeekFrom::Start(tuple_pointer.addr as u64))
            .map_err(Error::Io)?;
        self.io.read_exact(&mut data).unwrap();

        Ok(Some((data, tuple_pointer)))
    }

    fn tuple_pointer(&mut self, offset: usize) -> Result<TuplePointer> {
        let mut tuple_pointer = [0; TUPLE_POINTER_SIZE as usize];

        self.io
            .seek(SeekFrom::Start(offset as u64))
            .map_err(Error::Io)?;
        self.io.read_exact(&mut tuple_pointer).map_err(Error::Io)?;

        let tuple_addr = LocationOffset::from_le_bytes(tuple_pointer[0..2].try_into().unwrap());
        let tuple_len = LocationOffset::from_le_bytes(tuple_pointer[2..4].try_into().unwrap());
        let tuple_metadata = TuplePointerMetadata::from_slice(&tuple_pointer[4..]);

        Ok(TuplePointer {
            addr: tuple_addr,
            len: tuple_len,
            metadata: tuple_metadata,
        })
    }

    pub fn binary_search_by<F>(&mut self, mut f: F) -> Either<u16, u16>
    where
        F: FnMut(&[u8]) -> Ordering,
    {
        let mut size = self.len();
        let mut left = 0;
        let mut right = size;

        while left < right {
            let mid: LocationOffset = left + size / 2;

            let (data, _) = self.read(mid).unwrap().unwrap();

            match f(&data) {
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid,
                Ordering::Equal => return Either::Left(mid),
            }

            size = right - left;
        }

        Either::Right(left)
    }

    pub fn binary_search(&mut self, x: &[u8]) -> Either<u16, u16> {
        self.binary_search_by(|a| a.cmp(x))
    }

    pub fn binary_search_by_key<B, F>(&mut self, b: &B, mut f: F) -> Either<u16, u16>
    where
        F: FnMut(&[u8]) -> B,
        B: Ord,
    {
        self.binary_search_by(|k| f(k).cmp(b))
    }

    pub fn linear_search_by<F>(&mut self, mut f: F) -> Either<u16, u16>
    where
        F: FnMut(&[u8]) -> Ordering,
    {
        let size = self.len();
        let mut pointer = 0;

        while pointer < size {
            let (data, _) = self.read(pointer).unwrap().unwrap();

            if f(&data).is_eq() {
                return Either::Left(pointer);
            }

            pointer += 1;
        }

        Either::Right(pointer)
    }

    pub fn linear_search(&mut self, x: &[u8]) -> Either<u16, u16> {
        self.linear_search_by(|a| a.cmp(x))
    }

    pub fn linear_search_by_key<B, F>(&mut self, b: &B, mut f: F) -> Either<u16, u16>
    where
        F: FnMut(&[u8]) -> B,
        B: Ord,
    {
        self.linear_search_by(|k| f(k).cmp(b))
    }

    pub fn delete(&mut self, index: LocationOffset) -> Result<()> {
        let offset = self.index_to_offset(index);

        self.delete_at(offset)
    }

    pub fn delete_at(&mut self, offset: usize) -> Result<()> {
        let tuple_pointer_metadata_offset = offset + 4;

        self.io
            .seek(SeekFrom::Start(tuple_pointer_metadata_offset as u64))
            .map_err(Error::Io)?;
        self.io
            .write(&[TuplePointerFlags::Deleted as u8])
            .map_err(Error::Io)?;

        self.write_header()?;

        Ok(())
    }

    pub fn delete_range<R>(&mut self, range: R) -> Result<()>
    where
        R: Iterator<Item = u16>,
    {
        for i in range {
            let offset = self.index_to_offset(i);

            let tuple_pointer_metadata_offset = offset + 4;

            self.io
                .seek(SeekFrom::Start(tuple_pointer_metadata_offset as u64))
                .map_err(Error::Io)?;
            self.io
                .write(&[TuplePointerFlags::Deleted as u8])
                .map_err(Error::Io)?;
        }

        self.write_header()?;

        Ok(())
    }

    pub fn compact(mut self) -> Result<Self> {
        let data = self.values_with_pointers()?;
        let mut page = Self::create(self.special_size(), self.header.flags)?;
        page.write_special(&self.read_special()?)?;

        for (data, tuple_pointer) in data {
            page.write(&data, tuple_pointer.metadata.flags)?;
        }

        Ok(page)
    }

    fn values_with_pointers(&mut self) -> Result<Vec<(Vec<u8>, TuplePointer)>> {
        let mut data = Vec::new();

        for i in 0..self.len() {
            if let Some(tuple) = self.read(i)? {
                data.push(tuple);
            }
        }

        Ok(data)
    }

    pub fn values(&mut self) -> Result<Vec<Vec<u8>>> {
        let mut data: Vec<Vec<u8>> = Vec::new();

        for i in 0..self.len() {
            if let Some((tuple_data, _)) = self.read(i)? {
                data.push(tuple_data);
            }
        }

        Ok(data)
    }

    pub fn split_at(&mut self, index: LocationOffset) -> Result<PageSplit> {
        let values = self.values()?;
        let split = values.split_at(index as usize);

        Ok((split.0.to_vec(), split.1.to_vec()))
    }

    pub fn split_off(&mut self, index: LocationOffset) -> Result<Vec<Vec<u8>>> {
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
            .map_err(Error::Io)?;
        self.io.write(data).map_err(Error::Io)?;

        self.write_header()?;

        Ok(())
    }

    pub fn read_special(&mut self) -> Result<Vec<u8>> {
        let mut buffer = vec![0; self.special_size() as usize];

        self.io
            .seek(SeekFrom::Start(self.header.special as u64))
            .map_err(Error::Io)?;
        self.io.read(&mut buffer).map_err(Error::Io)?;

        Ok(buffer)
    }

    pub fn free_space(&self) -> u16 {
        self.header.upper - self.header.lower
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> u16 {
        (self.header.lower - self.header_size() as LocationOffset)
            / TUPLE_POINTER_SIZE as LocationOffset
    }

    pub fn index_to_offset(&self, index: LocationOffset) -> usize {
        self.header_size() + (index as usize * TUPLE_POINTER_SIZE as usize)
    }

    pub fn to_bytes(&self) -> std::io::Result<[u8; PAGE_SIZE as usize]> {
        let buffer = self.io.clone().into_inner();

        Ok(buffer)
    }

    pub fn iter(&mut self) -> PageIterator<'_> {
        PageIterator::new(self)
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

        let buffer = bincode::serialize(&self.header).map_err(Error::Parsing)?;

        self.io.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
        self.io
            .write(&[PAGE_MAGIC_BYTES, buffer.as_slice()].concat())
            .map_err(Error::Io)?;

        Ok(())
    }

    fn read_header(io: &mut Cursor<[u8; PAGE_SIZE as usize]>) -> Result<PageHeader> {
        let mut buffer = vec![0; PAGE_HEADER_SIZE];

        io.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
        io.read(&mut buffer).map_err(Error::Io)?;

        bincode::deserialize(&buffer[PAGE_MAGIC_BYTES.len()..]).map_err(Error::Parsing)
    }

    fn checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.io.get_ref()[PAGE_HEADER_SIZE..]);
        hasher.finalize()
    }
}

pub struct PageIterator<'p> {
    pos: LocationOffset,
    page: &'p mut Page,
}

impl<'p> PageIterator<'p> {
    pub fn new(page: &'p mut Page) -> Self {
        Self { page, pos: 0 }
    }
}

impl Iterator for PageIterator<'_> {
    type Item = (Vec<u8>, TuplePointer);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.page.len() {
            return None;
        }

        let tuple = self.page.read(self.pos).unwrap();

        self.pos += 1;

        tuple
    }

    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.page.read(self.page.len() - 1).unwrap()
    }
}

#[cfg(test)]
mod page_tests {
    use crate::serializer::{deserialize, serialize};

    use super::*;

    #[test]
    fn create_page() {
        let mut page = Page::create(0, 0).unwrap();

        page.write(&[12, 32], 0).unwrap();
        page.write(&[65, 23], 0).unwrap();

        let (num1, _) = page.read(0).unwrap().unwrap();
        let (num2, _) = page.read(1).unwrap().unwrap();

        assert_eq!(num1, &[12, 32]);
        assert_eq!(num2, &[65, 23]);
    }

    #[test]
    fn delete_values_in_page() {
        let mut page = Page::create(0, 0).unwrap();

        page.write(&[1], 0).unwrap();
        page.write(&[0], 0).unwrap();
        page.write(&[1], 0).unwrap();

        let (value, _) = page.read(1).unwrap().unwrap();

        assert_eq!(value, &[0]);

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
        let mut page = Page::create(0, 0).unwrap();

        page.write(&[1], 0).unwrap();
        page.write(&[2], 0).unwrap();
        page.write(&[3], 0).unwrap();

        let (value, _) = page.read(1).unwrap().unwrap();

        assert_eq!(value, &[2]);

        page.insert(1, &[4], 0).unwrap();

        let (value, _) = page.read(1).unwrap().unwrap();

        assert_eq!(value, &[4]);

        let (value, _) = page.read(2).unwrap().unwrap();

        assert_eq!(value, &[2]);

        let len = page.len();

        assert_eq!(len, 4)
    }

    #[test]
    fn binary_search_page_test() {
        let mut page = Page::create(0, 0).unwrap();

        page.write(&serialize(&(1, "Pedro".to_string())).unwrap(), 0)
            .unwrap();

        page.write(&serialize(&(2, "John".to_string())).unwrap(), 0)
            .unwrap();

        page.write(&serialize(&(5, "Ana".to_string())).unwrap(), 0)
            .unwrap();

        page.write(&serialize(&(8, "Jane".to_string())).unwrap(), 0)
            .unwrap();

        page.write(&serialize(&(10, "Beatriz".to_string())).unwrap(), 0)
            .unwrap();

        let found = page.binary_search_by_key(&8, |e| {
            let value: (i32, String) = deserialize(e).unwrap();

            value.0
        });

        assert_eq!(found, Either::Left(3));

        let (found_value, _) = page.read(*found.left().unwrap()).unwrap().unwrap();

        assert_eq!(found_value, serialize(&(8, "Jane".to_string())).unwrap());

        let found = page.binary_search_by_key(&9, |e| {
            let value: (i32, String) = deserialize(e).unwrap();

            value.0
        });

        assert_eq!(found, Either::Right(4));
    }

    #[test]
    fn special_size_page_test() {
        let mut page = Page::create(4, 0).unwrap();

        page.write(&[32], 0).unwrap();
        page.write(&[16], 0).unwrap();

        page.write_special(&[20, 10, 5, 2]).unwrap();

        let (value, _) = page.read(0).unwrap().unwrap();

        assert_eq!(value, &[32]);

        let special = page.read_special().unwrap();

        assert_eq!(special, vec![20, 10, 5, 2])
    }

    #[test]
    fn replace_page_test() {
        let mut page = Page::create(0, 0).unwrap();

        page.write(&[42], 0).unwrap();
        page.write(&[15], 0).unwrap();

        let (value, _) = page.read(0).unwrap().unwrap();

        assert_eq!(value, &[42]);

        page.replace(0, &[90]).unwrap();

        let (value, _) = page.read(0).unwrap().unwrap();

        assert_eq!(value, &[90]);
    }

    #[test]
    fn page_checksum() {
        let mut page = Page::create(0, 0).unwrap();

        page.write(&[99], 0).unwrap();

        let mut page_bytes = page.to_bytes().unwrap();
        //change a random byte
        page_bytes[128] = 2u8;

        Page::open(page_bytes).err().unwrap();
    }
}
