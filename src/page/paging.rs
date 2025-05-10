use std::marker::PhantomData;

use crate::{
    error::Error,
    serializer::{deserialize, serialize},
    spec::ValueTrait,
};

use super::{
    block::Block,
    layout::{PageFlag, TuplePointerFlags},
    overflow::{Overflow, OVERFLOW_SIZE},
    spec::{LocationOffset, PageNumber, TUPLE_POINTER_SIZE},
    Page, PageBuilder, Result,
};

const REQUIRED_SPACE: usize = TUPLE_POINTER_SIZE as usize + OVERFLOW_SIZE;

/// A abstraction layer to handle overflow pages and tuples
/// in a block. It provides methods to read and write
/// tuples to the block, and handles the allocation
/// of overflow pages when necessary.
pub struct Paging<'b, T: ValueTrait> {
    block: &'b mut Block,
    page_number: PageNumber,
    page: Page,

    _t: PhantomData<T>,
}

impl<'b, T: ValueTrait> Paging<'b, T> {
    pub fn new(block: &'b mut Block, page_number: PageNumber, page: Page) -> Self {
        Self {
            block,
            page_number,
            page,
            _t: PhantomData,
        }
    }

    pub fn get(&mut self, index: usize) -> Result<Option<T>> {
        let tuple = self.page.read(index as LocationOffset).unwrap();

        if tuple.is_none() {
            return Ok(None);
        }

        let (mut data, tuple_pointer) = tuple.unwrap();

        // if is not overflow item, we can deserialize it directly
        if tuple_pointer
            .metadata
            .not_has(TuplePointerFlags::OverflowItem)
        {
            let value = deserialize(&data)?;
            return Ok(Some(value));
        }

        let mut raw_data = data.get(OVERFLOW_SIZE..).unwrap().to_vec();

        loop {
            // if is overflow item, we need to read the overflow page
            let (overflow, _) = data.split_at(OVERFLOW_SIZE);

            let overflow = Overflow::from_bytes(overflow);

            let mut page = self.block.read_page(overflow.page_number as u64).unwrap();

            let offset = overflow.offset as usize;

            // read the overflow data
            let overflow_data = page.read(offset as LocationOffset).unwrap();
            if overflow_data.is_none() {
                return Ok(None);
            }
            let (overflow_data, tuple_pointer) = overflow_data.unwrap();

            // extend the raw data with the overflow data
            raw_data.extend_from_slice(&overflow_data);

            if tuple_pointer
                .metadata
                .not_has(TuplePointerFlags::OverflowItem)
            {
                break;
            }

            data = overflow_data;
        }

        let value = deserialize(&raw_data)?;

        Ok(Some(value))
    }

    pub fn write(&mut self, value: T) -> Result<()> {
        let data = serialize(&value)?;

        let required_space = REQUIRED_SPACE as u16;
        let free_space = self.page.free_space();

        if required_space > free_space {
            return Err(Error::NotEnoughSpace);
        }

        self.write_recursively(&data, self.page.clone(), self.page_number)?;

        self.page = self.block.read_page(self.page_number as u64).unwrap();

        Ok(())
    }

    fn write_recursively(
        &mut self,
        data: &[u8],
        mut page: Page,
        page_number: PageNumber,
    ) -> Result<(LocationOffset, PageNumber)> {
        let free_space = page.free_space();

        if data.len() + (TUPLE_POINTER_SIZE as usize) < free_space as usize {
            // write the data to the current page
            let (_, tuple_pointer_offset) = page.write(data, 0)?;

            if REQUIRED_SPACE as u16 >= page.free_space() {
                page.header.set(PageFlag::Full);
            }

            self.block.write_page(page_number as u64, &page).unwrap();

            return Ok((tuple_pointer_offset, page_number));
        }

        // split data to fit into the page
        let (data, overflow_data) =
            data.split_at(page.free_space() as usize - TUPLE_POINTER_SIZE as usize - OVERFLOW_SIZE);

        let (overflow_page, overflow_page_number) = self.allocate_overflow_page(
            overflow_data.len() as u16 + TUPLE_POINTER_SIZE + OVERFLOW_SIZE as u16,
        )?;

        let (offset, overflow_page_number) =
            self.write_recursively(overflow_data, overflow_page, overflow_page_number)?;

        let overflow = Overflow::new(overflow_page_number, offset);

        let data_to_write = [&overflow.to_bytes(), data].concat();

        // write the overflow data to the current page
        let (_, tuple_pointer_offset) =
            page.write(&data_to_write, TuplePointerFlags::OverflowItem as u8)?;

        page.header.set(PageFlag::Full);

        self.block.write_page(page_number as u64, &page).unwrap();

        Ok((tuple_pointer_offset, page_number))
    }

    pub fn len(&self) -> u16 {
        self.page.len()
    }

    pub fn is_empty(&self) -> bool {
        self.page.is_empty()
    }

    fn allocate_overflow_page(&mut self, space_to_allocate: u16) -> Result<(Page, PageNumber)> {
        // check if there is an available overflow page
        let available_overflow_page = self.block.header.free_page_overflow;

        let mut overflow_page = self
            .block
            .read_page(available_overflow_page as u64)
            .unwrap();

        if space_to_allocate >= overflow_page.free_space() {
            overflow_page.header.set(PageFlag::Full);
        }

        if overflow_page.header.not_has(PageFlag::OverflowPage)
            || overflow_page.header.has(PageFlag::Full)
        {
            return self.create_new_overflow_page();
        }

        Ok((overflow_page, available_overflow_page))
    }

    fn create_new_overflow_page(&mut self) -> Result<(Page, PageNumber)> {
        let overflow_page = PageBuilder::new().set(PageFlag::OverflowPage).build()?;

        // write the overflow page to the block
        let overflow_page_number = self.block.write_new_page(&overflow_page).unwrap();

        // update the block header
        self.block.header.free_page_overflow = overflow_page_number;
        self.block.persist_header().unwrap();

        Ok((overflow_page, overflow_page_number))
    }

    pub fn page(&self) -> &Page {
        &self.page
    }

    pub fn page_number(&self) -> PageNumber {
        self.page_number
    }
}

#[cfg(test)]
mod paging_tests {
    use crate::page::PageBuilder;

    use super::*;

    #[test]
    fn write_non_overflow_data() {
        let mut block = Block::new("test_data/paging/write_non_overflow_data.db").unwrap();
        let page = PageBuilder::new().build().unwrap();
        block.write_new_page(&page).unwrap();

        let mut paging = Paging::<String>::new(&mut block, 0, page);

        let value = "Hello, world!".to_string();
        paging.write(value.clone()).unwrap();

        let read_value = paging.get(0).unwrap().unwrap();
        assert_eq!(read_value, value);
        assert_eq!(paging.len(), 1);
    }

    #[test]
    fn write_overflow_data() {
        let mut block = Block::new("test_data/paging/write_overflow_data.db").unwrap();
        let page = PageBuilder::new().build().unwrap();
        block.write_new_page(&page).unwrap();

        let mut paging = Paging::<String>::new(&mut block, 0, page);

        let value = "Hello, world!".repeat(1000);
        paging.write(value.clone()).unwrap();

        let read_value = paging.get(0).unwrap().unwrap();
        assert_eq!(read_value, value);
        assert_eq!(paging.len(), 1);
    }

    #[test]
    fn write_multiple_overflow_data() {
        let mut block = Block::new("test_data/paging/write_multiple_overflow_data.db").unwrap();
        let page1 = PageBuilder::new().build().unwrap();
        let page2 = PageBuilder::new().build().unwrap();
        block.write_new_page(&page1).unwrap();
        block.write_new_page(&page2).unwrap();

        let mut paging1 = Paging::<String>::new(&mut block, 0, page1);

        let value1 = "Hello, world!".repeat(1000);
        paging1.write(value1.clone()).unwrap();

        let read_value1 = paging1.get(0).unwrap().unwrap();
        assert_eq!(read_value1, value1);

        let mut paging2 = Paging::<String>::new(&mut block, 1, page2);

        let value2 = "Goodbye, world!".repeat(1000);
        paging2.write(value2.clone()).unwrap();

        let read_value2 = paging2.get(0).unwrap().unwrap();
        assert_eq!(read_value2, value2);
    }
}
