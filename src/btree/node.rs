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

    pub fn merge(mut self, mut node: Self) -> Result<Self> {
        let node_values = node.page.values()?;
        self.page.write_all(node_values)?;

        self.header.right_child = node.header.right_child;

        self.page.write_special(&self.header.to_bytes())?;

        Ok(self)
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
