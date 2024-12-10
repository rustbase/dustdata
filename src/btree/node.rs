use crate::{
    error::{Error, Result},
    page::{spec::PageNumber, Page},
};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

use super::spec::{BTreeCell, BTreePageHeader, PageType, BTREE_PAGE_HEADER_SIZE};

pub struct BTreeNode<K, V> {
    pub page: Page<BTreeCell<K, V>>,
    pub header: BTreePageHeader,
}

pub struct BTreeNodeSplited<K, V> {
    pub node: BTreeNode<K, V>,
    pub median_cell: BTreeCell<K, V>,
    pub sibling_node: BTreeNode<K, V>,
}

impl<
        K: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug,
        V: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug,
    > BTreeNode<K, V>
{
    pub fn is_full(&self, b: u16) -> bool {
        match self.header.kind {
            PageType::Internal | PageType::Root => self.page.len() == (b * 2 - 1),
            PageType::Leaf => self.page.len() == (b * 2),
        }
    }

    pub fn is_underflow(&self, b: u16) -> bool {
        match self.header.kind {
            PageType::Root => false,
            PageType::Internal | PageType::Leaf => self.page.len() < b - 1,
        }
    }

    pub fn is_root(&self) -> bool {
        self.header.kind == PageType::Root
    }

    pub fn is_leaf(&self) -> bool {
        self.header.kind == PageType::Leaf
    }

    pub fn len(&mut self) -> u16 {
        self.page.len()
    }

    pub fn is_empty(&mut self) -> bool {
        self.page.is_empty()
    }

    pub fn split(mut self, b: u16) -> Result<BTreeNodeSplited<K, V>> {
        // split page cells at the middle
        let mut sibling_cells = self.page.split_off(b - 1)?;
        // get the median cell and remove from sibling_cells
        let median_cell = sibling_cells.remove(0);

        // garbage collect page
        self.page = self.page.compact()?;

        // create sibling page
        let mut sibling = Page::<BTreeCell<K, V>>::create(BTREE_PAGE_HEADER_SIZE)?;

        let (page_type, sibling_page_right_child) = match self.header.kind {
            PageType::Root | PageType::Internal => (PageType::Internal, self.header.right_child),
            PageType::Leaf => (PageType::Leaf, None),
        };

        // setting sibling page metadata
        let sibling_metadata = BTreePageHeader::new(page_type, sibling_page_right_child);
        sibling.write_special(&sibling_metadata.to_bytes())?;

        // write all sibling cells
        sibling.write_all(sibling_cells)?;

        let page_metadata = BTreePageHeader::new(page_type, median_cell.left_child);
        self.page.write_special(&page_metadata.to_bytes())?;

        Ok(BTreeNodeSplited {
            node: self,
            median_cell,
            sibling_node: sibling.try_into()?,
        })
    }

    /// Read child index by cell index
    pub fn child(&mut self, index: u16) -> Result<Option<u32>> {
        if self.is_leaf() {
            return Ok(None);
        }

        if index >= self.page.len() {
            Ok(self.header.right_child)
        } else {
            Ok(self.page.read(index)?.unwrap().left_child)
        }
    }

    /// Set child cell index with index
    pub fn set_child(&mut self, index: u16, child_index: PageNumber) -> Result<()> {
        if index >= self.page.len() {
            self.header.right_child = Some(child_index);
            self.page.write_special(&self.header.to_bytes())?;
        } else {
            let mut cell = self.page.read(index)?.unwrap();
            cell.left_child = Some(child_index);
            self.page.replace(index, cell)?;
        }

        Ok(())
    }

    pub fn max_key(&mut self) -> Result<Option<K>> {
        if self.page.is_empty() {
            return Ok(None);
        }

        Ok(self.page.read(self.page.len() - 1)?.map(|e| e.key))
    }

    pub fn min_key(&mut self) -> Result<Option<K>> {
        if self.page.is_empty() {
            return Ok(None);
        }

        Ok(self.page.read(0)?.map(|e| e.key))
    }

    pub fn merge(mut self, mut other: Self, mut cell: BTreeCell<K, V>) -> Result<Self> {
        let new_page_header = BTreePageHeader::new(self.header.kind, None);
        match self.min_key()? >= other.min_key()? {
            // merge with left sibling node
            true => {
                cell.left_child = other.header.right_child;
            }
            // merge with right sibling node
            false => {
                cell.left_child = self.header.right_child;
                self.header.right_child = other.header.right_child
            }
        };

        let node_values = self.page.values()?.into_iter();
        let other_values = other.page.values()?.into_iter();

        let mut merged_values = node_values
            .chain(other_values)
            .collect::<Vec<BTreeCell<K, V>>>();

        merged_values.push(cell);

        merged_values.sort_by(|a, b| a.key.cmp(&b.key));

        let mut page: Page<BTreeCell<K, V>> = Page::create(self.page.special_size())?;
        page.write_all(merged_values)?;
        page.write_special(&new_page_header.to_bytes())?;

        page.try_into()
    }

    pub fn iter_children(&mut self) -> impl DoubleEndedIterator<Item = PageNumber> + '_ {
        let len = if self.is_leaf() { 0 } else { self.len() + 1 };

        (0..len).map(|i| self.child(i).unwrap().unwrap())
    }
}

impl<
        K: Serialize + DeserializeOwned + PartialOrd + Ord + Clone,
        V: Serialize + DeserializeOwned + PartialOrd + Ord + Clone,
    > TryInto<BTreeNode<K, V>> for Page<BTreeCell<K, V>>
{
    type Error = Error;
    fn try_into(mut self) -> Result<BTreeNode<K, V>> {
        let special_bytes = &mut self.read_special()?;
        let header = BTreePageHeader::from_bytes(special_bytes);

        Ok(BTreeNode { header, page: self })
    }
}
