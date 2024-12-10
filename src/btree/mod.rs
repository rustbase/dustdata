use crate::{
    error::{Error, Result},
    page::{
        io::BlockIO,
        spec::{LocationOffset, PageNumber},
        Page,
    },
    Either,
};
use glob::Pattern;
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
    BTreeBlockHeader, BTreeCell, BTreePageHeader, BTreePair, PageType, BTREE_PAGE_HEADER_SIZE,
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
        let mut io: BlockIO = BlockIO::new(block_path).map_err(Error::IoError)?;

        let mut metadata_page: Page<BTreeBlockHeader> =
            io.read_metadata_page().map_err(Error::IoError)?;

        match metadata_page.is_empty() {
            false => {
                let block_header = metadata_page.read(0)?.unwrap();

                Ok(Self {
                    io,
                    root: block_header.root,
                    b: MAX_BRANCHING_FACTOR,
                    _k: PhantomData,
                    _v: PhantomData,
                })
            }

            true => Self::create_block(io),
        }
    }

    fn create_block(mut io: BlockIO) -> Result<Self> {
        let mut root: Page<BTreeCell<K, V>> = Page::create(BTREE_PAGE_HEADER_SIZE).unwrap();
        let metadata = BTreePageHeader::new(PageType::Leaf, None);
        root.write_special(&metadata.to_bytes()).unwrap();

        let root_page = io.write_new_page(&root).unwrap();

        let block_header = BTreeBlockHeader { root: root_page };

        let mut metadata_page: Page<BTreeBlockHeader> =
            io.read_metadata_page().map_err(Error::IoError)?;

        metadata_page.insert(0, block_header)?;

        io.write_metadata_page(&metadata_page).unwrap();

        Ok(Self {
            io,
            root: root_page,
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
        self.search_from_subtree_by(
            |page| page.binary_search_by_key(key, |e| e.key.clone()),
            page_number,
            parents,
        )
    }

    pub fn search_from_subtree_by<F>(
        &mut self,
        mut f: F,
        page_number: PageNumber,
        parents: &mut Vec<PageNumber>,
    ) -> Result<Search>
    where
        F: FnMut(&mut Page<BTreeCell<K, V>>) -> Either<u16, u16>,
    {
        let mut node: BTreeNode<K, V> = self
            .io
            .read_page(page_number.into())
            .map_err(Error::IoError)?
            .try_into()?;

        match node.header.kind {
            PageType::Internal | PageType::Root => {
                parents.push(page_number);
                let index = f(&mut node.page);

                if index.is_left() {
                    return Ok(Search {
                        index,
                        page: page_number,
                    });
                }

                let next_cell = *index.right().unwrap();

                let next_page = node.child(next_cell)?.unwrap();

                self.search_from_subtree_by(f, next_page, parents)
            }

            PageType::Leaf => {
                let index = f(&mut node.page);

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

    pub fn len(&mut self) -> Result<usize> {
        Ok(self.cells_from_subtree(self.root)?.len())
    }

    pub fn is_empty(&mut self) -> Result<bool> {
        Ok(self.len()? == 0)
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

        let block_header = BTreeBlockHeader { root: self.root };

        let mut metadata_page: Page<BTreeBlockHeader> =
            self.io.read_metadata_page().map_err(Error::IoError)?;

        metadata_page.insert(0, block_header)?;

        self.io.write_metadata_page(&metadata_page).unwrap();

        Ok(())
    }
}

impl<
        K: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug,
        V: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug,
    > IntoIterator for BTree<K, V>
{
    type Item = BTreePair<K, V>;
    type IntoIter = BTreeIterator<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        BTreeIterator::new(self)
    }
}

pub struct BTreeIterator<K, V> {
    btree: BTree<K, V>,
    page: PageNumber,
    cell_index: LocationOffset,
    parents: Vec<PageNumber>,
}

impl<
        K: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug,
        V: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug,
    > BTreeIterator<K, V>
{
    pub fn new(btree: BTree<K, V>) -> Self {
        let mut iter = Self {
            page: btree.root,
            btree,
            cell_index: 0,
            parents: Vec::new(),
        };

        iter.move_to_leftmost().unwrap();

        iter
    }

    fn move_to_leftmost(&mut self) -> Result<()> {
        let mut node: BTreeNode<K, V> = self
            .btree
            .io
            .read_page(self.page.into())
            .map_err(Error::IoError)?
            .try_into()?;

        while !node.is_leaf() {
            self.parents.push(self.page);

            self.page = node.child(0)?.unwrap();

            let next_node: BTreeNode<K, V> = self
                .btree
                .io
                .read_page(self.page.into())
                .map_err(Error::IoError)?
                .try_into()?;

            node = next_node
        }

        self.cell_index = 0;

        Ok(())
    }

    pub fn into_btree(self) -> BTree<K, V> {
        self.btree
    }
}

impl<
        K: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug,
        V: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug,
    > Iterator for BTreeIterator<K, V>
{
    type Item = BTreePair<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut node: BTreeNode<K, V> = self
            .btree
            .io
            .read_page(self.page.into())
            .unwrap()
            .try_into()
            .unwrap();

        if node.is_empty() && node.is_leaf() {
            return None;
        }

        let cell = node.page.read(self.cell_index).unwrap();

        if node.is_leaf() && self.cell_index + 1 < node.len() {
            self.cell_index += 1;
            return Some(cell.unwrap().to_pair());
        }

        if !node.is_leaf() && self.cell_index < node.len() {
            self.parents.push(self.page);
            self.page = node.child(self.cell_index + 1).unwrap().unwrap();
            self.move_to_leftmost().unwrap();

            return Some(cell.unwrap().to_pair());
        }

        let mut found_branch = false;

        while !self.parents.is_empty() && !found_branch {
            let parent_page = self.parents.pop().unwrap();
            let mut parent: BTreeNode<K, V> = self
                .btree
                .io
                .read_page(parent_page.into())
                .unwrap()
                .try_into()
                .unwrap();

            let index = parent.iter_children().position(|c| c == self.page).unwrap() as u16;

            self.page = parent_page;

            if index < parent.len() {
                self.cell_index = index;
                found_branch = true;
            }
        }

        if self.parents.is_empty() && !found_branch {
            return None;
        }

        Some(cell.unwrap().to_pair())
    }
}

impl<V: Serialize + DeserializeOwned + PartialOrd + Ord + Clone + Debug> BTree<String, V> {
    /// O(n) worst-case complexity
    pub fn find_pattern(&mut self, key_pattern: &str) -> Result<Vec<BTreePair<String, V>>> {
        let results = self
            .cells_from_subtree(self.root)?
            .into_iter()
            .filter_map(|c| {
                if Pattern::new(key_pattern).unwrap().matches(&c.key) {
                    Some(c.to_pair())
                } else {
                    None
                }
            })
            .collect::<Vec<BTreePair<String, V>>>();

        Ok(results)
    }
}

#[cfg(test)]
mod btree_tests {
    use super::*;

    #[test]
    fn create_btree() {
        let mut btree = BTree::<u32, u32>::new("test_data/btree.db").unwrap();

        for i in 0u32..100 {
            btree.insert(i, i * 2).unwrap();
        }

        let value = btree.get(&2).unwrap().unwrap();
        assert_eq!(value, 4);

        let value = btree.get(&10).unwrap().unwrap();
        assert_eq!(value, 20);

        let value = btree.get(&50).unwrap().unwrap();
        assert_eq!(value, 100);

        let value = btree.get(&75).unwrap().unwrap();
        assert_eq!(value, 150);

        let value = btree.get(&99).unwrap().unwrap();
        assert_eq!(value, 198);

        let length = btree.len().unwrap();
        assert_eq!(length, 100);
    }
}
