use std::{fmt::Debug, marker::PhantomData};

use log::debug;
use serde::{de::DeserializeOwned, Serialize};

use super::{
    io::BlockIO,
    layout::PageFlag,
    overflow::{Overflow, OVERFLOW_POINTER_SIZE},
    spec::{CellId, LocationOffset, PageNumber, CELL_POINTER_SIZE},
    Page,
};
use crate::{
    error::{Error, Result},
    serializer::{deserialize, serialize},
    spec::ValueTrait,
    Either,
};

pub struct Pager<'b, T> {
    block: &'b mut BlockIO,
    page: Page,
    head_page_number: PageNumber,
    page_number: u64,

    _t: PhantomData<T>,
}

const MIN_SPACE_TO_WRITE: u16 = CELL_POINTER_SIZE + OVERFLOW_POINTER_SIZE as u16 + 3;

#[derive(Debug)]
pub struct Search {
    pub page: PageNumber,
    pub page_index: Either<LocationOffset, LocationOffset>,

    pub index: Either<u64, u64>,
}

impl<'b, T: ValueTrait> Pager<'b, T> {
    pub fn new(io: &'b mut BlockIO, page: Page, page_number: PageNumber) -> Self {
        Self {
            block: io,
            page,
            page_number,
            head_page_number: page_number,
            _t: PhantomData,
        }
    }

    pub fn page(&self) -> &Page {
        &self.page
    }

    pub fn write_all(&mut self, data: Vec<T>) -> Result<()> {
        for data in data {
            self.write(data)?;
        }

        Ok(())
    }

    pub fn write(&mut self, data: T) -> Result<(PageNumber, LocationOffset)> {
        if self.page.header.has(PageFlag::Full) {
            self.navigate_to_next_available_page()?;
        }

        let bytes = serialize(&data)?;

        self.write_raw(&bytes, None)
    }

    fn write_raw(
        &mut self,
        bytes: &[u8],
        latest_overflow_page: Option<PageNumber>,
    ) -> Result<(PageNumber, LocationOffset)> {
        let remaining_space =
            self.page.remaining_space() - CELL_POINTER_SIZE - OVERFLOW_POINTER_SIZE as u16;
        let required_space = bytes.len();

        // if data size is greather than page available space
        if required_space >= remaining_space as usize {
            let (data, remaining_data) = bytes.split_at(remaining_space as usize);

            let current_page_number = self.page_number;

            self.navigate_to_next_available_overflow_page()?;

            if Some(self.page_number) == latest_overflow_page {
                self.page.header.set(PageFlag::Full);
                self.create_new_overflow_page()?;
            }

            // write to next available page
            let (page, cell_addr) = self.write_raw(remaining_data, Some(self.page_number))?;

            self.set_page(current_page_number)?;

            let overflow = Overflow {
                is_overflow: true,
                page,
                location: cell_addr,
            };

            let overflow = overflow.to_vec()?;

            self.page.header.set(PageFlag::Full);
            self.page.write_header()?;

            let data = &[overflow.as_slice(), data].concat();

            // write to current page
            let (_, cell_addr) = self.page.write(data)?;
            self.block
                .write_page(self.page_number as u64, &self.page)
                .map_err(Error::IoError)?;

            return Ok((current_page_number, cell_addr));
        }

        self.write_raw_non_full(bytes)
    }

    fn write_raw_non_full(&mut self, bytes: &[u8]) -> Result<(PageNumber, LocationOffset)> {
        let overflow = Overflow {
            is_overflow: false,
            page: 0,
            location: 0,
        };

        let overflow = overflow.to_vec()?;

        let (_, cell_addr) = self.page.write(&[overflow.as_slice(), bytes].concat())?;

        if self.page.remaining_space() <= MIN_SPACE_TO_WRITE {
            self.page.header.set(PageFlag::Full);
            self.page.write_header()?;
        }

        self.block
            .write_page(self.page_number as u64, &self.page)
            .map_err(Error::IoError)?;

        Ok((self.page_number, cell_addr))
    }

    pub fn read(&mut self, index: CellId) -> Result<Option<T>> {
        self.set_page_to_head()?;

        let mut cells = self.page.len() as CellId;

        while self.navigate_to_next_page().is_some() {
            cells += self.page.len() as CellId;

            if index < cells {
                break;
            }
        }

        let cells_count = cells - self.page.len() as CellId;

        let data = self.reassemble_data((index - cells_count) as LocationOffset)?;

        Ok(data.map(|d| deserialize(&d).unwrap()))
    }

    // pub fn insert(&mut self, data: T, index: CellId) -> Result<()> {}

    pub fn binary_search_by_key<B, F>(&mut self, b: &B, mut f: F) -> Result<Search>
    where
        F: FnMut(&T) -> &B,
        B: Ord + DeserializeOwned + Serialize + Debug,
    {
        self.set_page_to_head()?;

        let mut last_cell: T =
            deserialize(&self.reassemble_data(self.page.len() - 1).unwrap().unwrap())?;
        let mut page_cells = 0;

        while b > f(&last_cell) {
            if self.page.header.next_page.is_none() {
                return Ok(Search {
                    page: self.page_number,
                    page_index: Either::Right(0),
                    index: Either::Right(page_cells),
                });
            }

            page_cells += self.page.len() as u64;

            self.navigate_to_next_page().unwrap()?;

            last_cell = deserialize(&self.reassemble_data(self.page.len() - 1).unwrap().unwrap())?;
        }

        let page_index = self.page.binary_search_by(|a| {
            f(&deserialize::<T>(&a[OVERFLOW_POINTER_SIZE..]).unwrap()).cmp(b)
        });

        let index: Either<u64, u64> = match page_index {
            Either::Left(index) => Either::Left(index as u64 + page_cells),
            Either::Right(index) => Either::Right(index as u64 + page_cells),
        };

        Ok(Search {
            page: self.page_number,
            page_index,
            index,
        })
    }

    pub fn binary_search(&mut self, x: &T) -> Result<Search> {
        self.binary_search_by_key(x, |d| d)
    }

    pub fn len(&mut self) -> Result<usize> {
        self.set_page_to_head()?;

        let mut cells = self.page.len();

        while self.navigate_to_next_page().is_some() {
            cells += self.page.len();
        }

        Ok(cells as usize)
    }

    pub fn is_empty(&mut self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    fn navigate_to_next_available_page(&mut self) -> Result<()> {
        while self.page.header.next_page.is_some() {
            self.navigate_to_next_page().unwrap()?;
        }

        if !self.page.header.has(PageFlag::Full) {
            return Ok(());
        }

        self.create_new_page()?;

        Ok(())
    }

    fn navigate_to_next_available_overflow_page(&mut self) -> Result<()> {
        if self.block.header.first_free_page_overflow.is_none() {
            return self.create_new_overflow_page();
        }

        self.navigate_to_overflow_page()?;

        if !self.page.header.has(PageFlag::Full) {
            return Ok(());
        }

        self.create_new_overflow_page()?;

        Ok(())
    }

    fn create_new_page(&mut self) -> Result<()> {
        let new_page = Page::create(self.page.special_size())?;

        let new_page_number = self
            .block
            .write_new_page(&new_page)
            .map_err(Error::IoError)?;

        self.page.header.next_page = Some(new_page_number);
        self.page.write_header()?;

        self.block
            .write_page(self.page_number as u64, &self.page)
            .map_err(Error::IoError)?;

        println!("{} -> {}", self.page_number, new_page_number);
        println!("{:?}", self.page.header);

        self.page = new_page;
        self.page_number = new_page_number;

        Ok(())
    }

    fn create_new_overflow_page(&mut self) -> Result<()> {
        let mut new_page = Page::create(0)?;

        new_page.header.set(PageFlag::OverflowPage);

        let new_page_number = self
            .block
            .write_new_page(&new_page)
            .map_err(Error::IoError)?;

        self.block.header.first_free_page_overflow = Some(new_page_number);
        self.block.write_header().map_err(Error::IoError)?;

        self.page = new_page;
        self.page_number = new_page_number;

        Ok(())
    }

    fn navigate_to_next_page(&mut self) -> Option<Result<()>> {
        let next_page_number = self.page.header.next_page?;

        self.set_page(next_page_number).unwrap();

        Some(Ok(()))
    }

    fn navigate_to_overflow_page(&mut self) -> Result<()> {
        let overflow_page_number = self.block.header.first_free_page_overflow.unwrap();

        self.set_page(overflow_page_number)?;

        Ok(())
    }

    fn reassemble_data(&mut self, index: LocationOffset) -> Result<Option<Vec<u8>>> {
        let data = self.page.read(index)?;

        if data.is_none() {
            return Ok(None);
        }

        let mut data = data.unwrap();

        let overflow = Overflow::from_slice(&data[..OVERFLOW_POINTER_SIZE])?;

        if overflow.is_overflow {
            self.set_page(overflow.page)?;
            let next_data = self.reassemble_data(overflow.location)?;

            data.extend_from_slice(&next_data.unwrap());
        }

        Ok(Some(data[OVERFLOW_POINTER_SIZE..].to_vec()))
    }

    fn set_page(&mut self, page_number: PageNumber) -> Result<()> {
        let page = self
            .block
            .read_page(page_number as u64)
            .map_err(Error::IoError)?;

        self.page = page;
        self.page_number = page_number;

        Ok(())
    }

    fn set_page_to_head(&mut self) -> Result<()> {
        self.set_page(self.head_page_number)?;

        Ok(())
    }
}

#[cfg(test)]
mod pager_tests {
    use super::*;

    #[test]
    fn write_test() {
        let mut block = BlockIO::new("test_data/b.db").unwrap();
        let mut head_page = Page::create(0).unwrap();

        let mut pager = Pager::<u8>::new(&mut block, head_page, 0);

        for i in 0..2000u16 {
            pager.write((i % 255).try_into().unwrap()).unwrap();
        }
    }

    // #[test]
    // fn insert_test() {
    //     let mut block = BlockIO::new("test_data/pager_insert_test.db").unwrap();
    //     let head_page = Page::create(0).unwrap();
    //     block.write_new_page(&head_page).unwrap();

    //     let mut pager = Pager::<String>::new(&mut block, head_page, 0);

    //     for i in 0..100_000 {
    //         pager.write(i.to_string().repeat(i)).unwrap();
    //     }

    //     println!("{:?}", pager.read(90_000));
    // }
}
