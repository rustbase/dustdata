use crate::{
    error::{Error, Result},
    page::{io::BlockIO, spec::PageNumber, Page},
    Either,
};
use node::BTreeNode;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cmp::{self, Ordering},
    fmt::Debug,
    marker::PhantomData,
    path::Path,
};

pub mod node;
pub mod spec;
use spec::{
    BTreeBlockHeader, BTreeCell, BTreePageHeader, PageType, BTREE_BLOCK_ALLOC_SIZE,
    BTREE_PAGE_HEADER_SIZE,
};

pub const MAX_BRANCHING_FACTOR: u16 = 100;

#[derive(Debug)]
pub struct Search {
    /// The page number where the search ended
    pub page: PageNumber,

    /// The index where the search ended
    pub index: Either<u16, u16>,
}

pub struct BTree<K, V> {
    root: PageNumber,
    b: u16,
    io: BlockIO,

    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<
        K: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug,
        V: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug,
    > BTree<K, V>
{
    pub fn new<P>(block_path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let io: BlockIO =
            BlockIO::new(block_path, BTREE_BLOCK_ALLOC_SIZE as u16).map_err(Error::IoError)?;

        if io.exists().map_err(Error::IoError)? {
            Self::open_block(io)
        } else {
            Self::create_block(io)
        }
    }

    fn open_block(mut io: BlockIO) -> Result<Self> {
        let bytes = io.read_alloc_data().map_err(Error::IoError)?;
        let block_header: BTreeBlockHeader =
            bincode::deserialize(&bytes).map_err(Error::SerializeError)?;

        Ok(Self {
            io,
            root: block_header.root,
            b: block_header.b,
            _k: PhantomData,
            _v: PhantomData,
        })
    }

    fn create_block(mut io: BlockIO) -> Result<Self> {
        let mut root: Page<BTreeCell<K, V>> = Page::create(BTREE_PAGE_HEADER_SIZE).unwrap();
        let metadata = BTreePageHeader::new(PageType::Leaf, None);
        root.write_special(&metadata.to_bytes()).unwrap();

        io.write_page(0, &root).unwrap();

        let block_header = BTreeBlockHeader {
            b: MAX_BRANCHING_FACTOR,
            root: 0,
        };

        io.alloc_data(&bincode::serialize(&block_header).map_err(Error::SerializeError)?)
            .map_err(Error::IoError)?;

        Ok(Self {
            io,
            root: 0,
            b: MAX_BRANCHING_FACTOR,
            _k: PhantomData,
            _v: PhantomData,
        })
    }

    pub fn search(&mut self, key: &K) -> Result<Search> {
        self.search_from_subtree(key, self.root, &mut Vec::new())
    }

    fn search_from_subtree(
        &mut self,
        key: &K,
        page_number: PageNumber,
        parents: &mut Vec<PageNumber>,
    ) -> Result<Search> {
        let mut node: BTreeNode<K, V> = self
            .io
            .read_page(page_number.into())
            .map_err(Error::IoError)?
            .try_into()?;

        match node.header.kind {
            PageType::Internal | PageType::Root => {
                parents.push(page_number);
                let index = node.page.binary_search_by_key(key, |e| e.key.clone());

                if index.is_left() {
                    return Ok(Search {
                        index,
                        page: page_number,
                    });
                }

                let next_cell = *index.right().unwrap();

                let next_page = node.child(next_cell)?.unwrap();

                self.search_from_subtree(key, next_page, parents)
            }

            PageType::Leaf => {
                let index = node.page.binary_search_by_key(key, |e| e.key.clone());

                Ok(Search {
                    index,
                    page: page_number,
                })
            }
        }
    }

    pub fn get(&mut self, key: &K) -> Result<Option<V>> {
        let search = self.search_from_subtree(key, self.root, &mut Vec::new())?;

        if let Either::Left(index) = search.index {
            let mut page: Page<BTreeCell<K, V>> = self
                .io
                .read_page(search.page.into())
                .map_err(Error::IoError)?;

            let cell = page.read(index)?;

            Ok(cell.map(|c| c.value.unwrap()))
        } else {
            Ok(None)
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        let root: BTreeNode<K, V> = self
            .io
            .read_page(self.root.into())
            .map_err(Error::IoError)?
            .try_into()?;

        if root.is_full(self.b) {
            let mut splited = root.split(self.b)?;

            let sibling_page_number = self
                .io
                .write_new_page(&splited.sibling_node.page)
                .map_err(Error::IoError)?;

            // create new root
            let mut new_root = Page::<BTreeCell<K, V>>::create(BTREE_PAGE_HEADER_SIZE)?;
            // set sibling page index to new root right child
            let new_root_metadata = BTreePageHeader::new(PageType::Root, Some(sibling_page_number));
            new_root.write_special(&new_root_metadata.to_bytes())?;

            splited.median_cell.left_child = Some(self.root);
            // write median cell to new root page
            new_root.write(splited.median_cell)?;

            // write old root to disk
            self.io
                .write_page(self.root.into(), &splited.node.page)
                .map_err(Error::IoError)?;

            // write new root to disk and set new root index
            let root_page = self.io.write_new_page(&new_root).map_err(Error::IoError)?;
            self.set_root(root_page)?;
        }

        self.insert_non_full(self.root, key, value)
    }

    fn insert_non_full(&mut self, page_number: PageNumber, key: K, value: V) -> Result<()> {
        let mut node: BTreeNode<K, V> = self
            .io
            .read_page(page_number.into())
            .map_err(Error::IoError)?
            .try_into()?;

        let index = node.page.binary_search_by_key(&key, |e| e.key.clone());

        let index = match index {
            Either::Left(index) => index,
            Either::Right(index) => index,
        };

        match node.header.kind {
            PageType::Internal | PageType::Root => {
                let page_child_page_number = node.child(index)?.unwrap();

                let page_child: BTreeNode<K, V> = self
                    .io
                    .read_page(page_child_page_number.into())
                    .map_err(Error::IoError)?
                    .try_into()?;

                if !page_child.is_full(self.b) {
                    return self.insert_non_full(page_child_page_number, key, value);
                }

                let mut splited = page_child.split(self.b)?;

                let sibling_page_number = self
                    .io
                    .write_new_page(&splited.sibling_node.page)
                    .map_err(Error::IoError)?;

                node.set_child(index + 1, sibling_page_number)?;

                splited.median_cell.left_child = Some(page_child_page_number);

                let median_cell_key = splited.median_cell.key.clone();
                node.page.insert(index, splited.median_cell)?;

                self.io
                    .write_page(page_child_page_number.into(), &splited.node.page)
                    .map_err(Error::IoError)?;

                self.io
                    .write_page(page_number.into(), &node.page)
                    .map_err(Error::IoError)?;

                let insert_page_offset = match key.cmp(&median_cell_key) {
                    Ordering::Less | Ordering::Equal => page_child_page_number,
                    Ordering::Greater => sibling_page_number,
                };

                self.insert_non_full(insert_page_offset, key, value)
            }

            PageType::Leaf => {
                let cell = BTreeCell {
                    left_child: None,
                    key,
                    value: Some(value),
                };

                node.page.insert(index, cell)?;

                self.io
                    .write_page(page_number.into(), &node.page)
                    .map_err(Error::IoError)?;

                Ok(())
            }
        }
    }

    pub fn values(&mut self) -> Result<Vec<V>> {
        let values = self.cells_from_subtree(self.root)?.into_iter();
        let values = values.map(|e| e.value.unwrap()).collect::<Vec<V>>();

        Ok(values)
    }

    pub fn keys(&mut self) -> Result<Vec<K>> {
        let keys = self.cells_from_subtree(self.root)?.into_iter();
        let keys = keys.map(|e| e.key).collect::<Vec<K>>();

        Ok(keys)
    }

    pub fn cells_from_subtree(&mut self, page_number: PageNumber) -> Result<Vec<BTreeCell<K, V>>> {
        let mut node: BTreeNode<K, V> = self
            .io
            .read_page(page_number.into())
            .map_err(Error::IoError)?
            .try_into()?;

        let mut results = Vec::new();

        for index in 0..node.page.len() {
            let child = node.child(index)?;

            if let Some(child) = child {
                results.extend(self.cells_from_subtree(child)?)
            }

            results.extend(node.page.read(index)?);
        }

        let right = node.child(node.page.len())?;
        if let Some(right) = right {
            results.extend(self.cells_from_subtree(right)?)
        }

        Ok(results)
    }

    pub fn delete(&mut self, key: &K) -> Result<()> {
        self.delete_from_subtree(self.root, key, &mut Vec::new())
    }

    fn delete_from_subtree(
        &mut self,
        page_number: PageNumber,
        key: &K,
        parents: &mut Vec<PageNumber>,
    ) -> Result<()> {
        // search key
        let search = self.search_from_subtree(key, page_number, parents)?;

        // get searched page node
        let mut node: BTreeNode<K, V> = self
            .io
            .read_page(search.page.into())
            .map_err(Error::IoError)?
            .try_into()?;

        // return if not found
        if search.index.is_right() {
            return Err(Error::NotFound(format!("key {:?}", key)));
        }

        // delete cell
        let index = *search.index.left().unwrap();
        node.page.delete(index)?;
        let page = node.page.compact()?;

        self.io
            .write_page(search.page.into(), &page)
            .map_err(Error::IoError)?;

        // check for underflow
        self.borrow_if_needed(search.page, parents, key)
    }

    fn borrow_if_needed(
        &mut self,
        page_number: PageNumber,
        parents: &mut Vec<PageNumber>,
        key: &K,
    ) -> Result<()> {
        let node: BTreeNode<K, V> = self
            .io
            .read_page(page_number.into())
            .map_err(Error::IoError)?
            .try_into()?;

        if !node.is_underflow(self.b) || parents.is_empty() {
            return Ok(());
        }

        let parent_index = parents.pop().unwrap();

        let mut parent: BTreeNode<K, V> = self
            .io
            .read_page(parent_index.into())
            .map_err(Error::IoError)?
            .try_into()?;

        let index = parent.page.binary_search_by_key(key, |e| e.key.clone());

        let index = match index {
            Either::Left(index) => index,
            Either::Right(index) => index,
        };
        let sibling_index = match index > 0 {
            false => index + 1,
            true => index - 1,
        };

        let sibling_page_number = parent.child(sibling_index)?.unwrap();

        let sibling: BTreeNode<K, V> = self
            .io
            .read_page(sibling_page_number.into())
            .map_err(Error::IoError)?
            .try_into()?;

        let merged_node_idx = cmp::min(index, sibling_index);

        let cell = parent.page.read(merged_node_idx)?.unwrap();
        parent.page.delete(merged_node_idx)?;
        let mut parent: BTreeNode<K, V> = parent.page.compact()?.try_into()?;

        let mut merged_node = node.merge(sibling, cell)?;

        let merged_page_number = self
            .io
            .write_new_page(&merged_node.page)
            .map_err(Error::IoError)?;

        if parent.is_root() && parent.page.is_empty() {
            merged_node.header.kind = PageType::Root;
            merged_node
                .page
                .write_special(&merged_node.header.to_bytes())?;

            self.io
                .write_page(merged_page_number.into(), &merged_node.page)
                .map_err(Error::IoError)?;

            self.set_root(merged_page_number)?;
        } else {
            parent.set_child(merged_node_idx, merged_page_number)?;
        }

        self.io
            .write_page(parent_index.into(), &parent.page)
            .map_err(Error::IoError)?;

        if let Some(parent) = parents.pop() {
            self.borrow_if_needed(parent, parents, key)?;
        }

        Ok(())
    }

    fn set_root(&mut self, page_number: PageNumber) -> Result<()> {
        self.root = page_number;

        let block_header = BTreeBlockHeader {
            b: MAX_BRANCHING_FACTOR,
            root: self.root,
        };

        self.io
            .alloc_data(&bincode::serialize(&block_header).map_err(Error::SerializeError)?)
            .map_err(Error::IoError)?;
        Ok(())
    }
}

// #[cfg(test)]
// mod btree_tests {
//     use super::*;

//     #[test]
//     fn create_btree() {
//         let mut btree = BTree::<u32, String>::new("test_data/btree").unwrap();
//     }
// }
