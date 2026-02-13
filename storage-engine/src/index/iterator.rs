//! B+ tree iterator for range scans.
//!
//! Provides efficient range scanning by following the leaf node chain.

use std::sync::Arc;
use std::ops::DerefMut;
use buffer_pool_manager::api::{BufferPoolManager, PageId, BpmError, INVALID_PAGE_ID};
use crate::table::RowId;
use super::key::{IndexKey, KeyType};
use super::node::BPlusTreeNode;

/// An iterator over a range of keys in a B+ tree.
///
/// The iterator follows the leaf node chain, returning key-value pairs
/// until the end key is reached or the end of the tree is encountered.
pub struct BPlusTreeIterator {
    bpm: Arc<dyn BufferPoolManager>,
    current_page_id: PageId,
    current_index: usize,
    end_key: Option<IndexKey>,
    key_type: KeyType,
}

impl BPlusTreeIterator {
    /// Creates a new B+ tree iterator.
    ///
    /// # Arguments
    /// * `bpm` - The buffer pool manager
    /// * `start_page_id` - The leaf page to start from
    /// * `start_index` - The index within the start page
    /// * `end_key` - Optional end key (exclusive)
    /// * `key_type` - The type of keys in the tree
    pub fn new(
        bpm: Arc<dyn BufferPoolManager>,
        start_page_id: PageId,
        start_index: usize,
        end_key: Option<IndexKey>,
        key_type: KeyType,
    ) -> Self {
        Self {
            bpm,
            current_page_id: start_page_id,
            current_index: start_index,
            end_key,
            key_type,
        }
    }

    /// Creates an iterator that scans the entire tree.
    pub fn full_scan(
        bpm: Arc<dyn BufferPoolManager>,
        start_page_id: PageId,
        key_type: KeyType,
    ) -> Self {
        Self::new(bpm, start_page_id, 0, None, key_type)
    }
}

impl Iterator for BPlusTreeIterator {
    type Item = Result<(IndexKey, RowId), BpmError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_page_id == INVALID_PAGE_ID {
                return None;
            }

            // Fetch current page
            let mut page_guard = match self.bpm.fetch_page(self.current_page_id) {
                Ok(guard) => guard,
                Err(e) => return Some(Err(e)),
            };

            let node = BPlusTreeNode::new(
                page_guard.deref_mut(),
                self.key_type.clone(),
            );

            let key_count = node.key_count() as usize;

            // Check if we've exhausted the current page
            if self.current_index >= key_count {
                // Move to next leaf
                self.current_page_id = node.next_leaf();
                self.current_index = 0;
                continue;
            }

            // Get current key-value pair
            let key = node.get_key(self.current_index);

            // Check if we've reached the end key
            if let Some(ref end_key) = self.end_key {
                if key.compare(end_key) != std::cmp::Ordering::Less {
                    return None;
                }
            }

            let value = node.get_value(self.current_index);
            self.current_index += 1;

            return Some(Ok((key, value)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::bptree::BPlusTree;
    use buffer_pool_manager::actor::ActorBufferPoolManager;
    use buffer_pool_manager::disk_manager::DiskManager;
    use std::fs;

    #[test]
    fn test_iterator_empty_tree() {
        let db_file = "test_iterator_empty.db";
        let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
        let bpm = Arc::new(ActorBufferPoolManager::new(10, disk_manager));

        let tree = BPlusTree::new(bpm.clone(), KeyType::Integer).unwrap();

        // Get leftmost leaf for iteration
        let leftmost = tree.find_leftmost_leaf().unwrap();
        let mut iter = BPlusTreeIterator::full_scan(bpm, leftmost, KeyType::Integer);

        assert!(iter.next().is_none());

        fs::remove_file(db_file).unwrap();
    }
}
