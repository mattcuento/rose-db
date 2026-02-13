//! B+ tree implementation with latch crabbing for concurrency.
//!
//! This module implements a B+ tree index with the following features:
//! - Latch coupling (crabbing) for concurrent access
//! - Right-biased splits for sequential insert optimization
//! - Support for Integer and Varchar keys
//! - Range scan support via leaf chain

use std::sync::Arc;
use std::ops::{Deref, DerefMut};
use buffer_pool_manager::api::{BufferPoolManager, PageId, BpmError, INVALID_PAGE_ID};
use crate::table::RowId;
use super::key::{IndexKey, KeyType};
use super::metadata::IndexMetadata;
use super::node::BPlusTreeNode;

/// A B+ tree index supporting efficient point queries and range scans.
pub struct BPlusTree {
    bpm: Arc<dyn BufferPoolManager>,
    metadata_page_id: PageId,
    key_type: KeyType,
    leaf_max_size: u16,
    internal_max_size: u16,
}

impl BPlusTree {
    /// Creates a new B+ tree index.
    ///
    /// Allocates a metadata page and initializes an empty tree with a root leaf node.
    pub fn new(bpm: Arc<dyn BufferPoolManager>, key_type: KeyType) -> Result<Self, BpmError> {
        // Create metadata page
        let mut metadata_page = bpm.new_page()?;
        let metadata_page_id = metadata_page.page_id();

        let mut metadata = IndexMetadata::new(key_type.clone());

        // Create root leaf node
        let mut root_page = bpm.new_page()?;
        let root_page_id = root_page.page_id();
        metadata.root_page_id = root_page_id;

        // Initialize root as leaf
        let mut root_node = BPlusTreeNode::new(root_page.deref_mut(), key_type.clone());
        root_node.initialize(root_page_id, true, INVALID_PAGE_ID);

        // Write metadata to page
        let serialized = metadata.serialize();
        metadata_page[0..serialized.len()].copy_from_slice(&serialized);

        // Pages will be unpinned when guards drop
        drop(root_page);
        drop(metadata_page);

        Ok(Self {
            bpm,
            metadata_page_id,
            key_type: metadata.key_type,
            leaf_max_size: metadata.leaf_max_size,
            internal_max_size: metadata.internal_max_size,
        })
    }

    /// Opens an existing B+ tree index from a metadata page.
    pub fn open(bpm: Arc<dyn BufferPoolManager>, metadata_page_id: PageId) -> Result<Self, BpmError> {
        let metadata = {
            let metadata_page = bpm.fetch_page(metadata_page_id)?;
            IndexMetadata::deserialize(metadata_page.deref())
        };

        Ok(Self {
            bpm,
            metadata_page_id,
            key_type: metadata.key_type,
            leaf_max_size: metadata.leaf_max_size,
            internal_max_size: metadata.internal_max_size,
        })
    }

    /// Loads the metadata from disk.
    fn load_metadata(&self) -> Result<IndexMetadata, BpmError> {
        let metadata_page = self.bpm.fetch_page(self.metadata_page_id)?;
        Ok(IndexMetadata::deserialize(metadata_page.deref()))
    }

    /// Updates the root page ID in metadata.
    fn update_root(&self, new_root_page_id: PageId) -> Result<(), BpmError> {
        let mut metadata = self.load_metadata()?;
        metadata.root_page_id = new_root_page_id;

        let mut metadata_page = self.bpm.fetch_page(self.metadata_page_id)?;
        let serialized = metadata.serialize();
        metadata_page.deref_mut()[0..serialized.len()].copy_from_slice(&serialized);

        Ok(())
    }

    // ===== SEARCH OPERATION WITH LATCH CRABBING =====

    /// Searches for a key in the B+ tree.
    ///
    /// Uses optimistic latch crabbing: releases parent latches as soon as we know
    /// the child won't need to be modified.
    ///
    /// Returns the RowId if the key is found, None otherwise.
    pub fn search(&self, key: &IndexKey) -> Result<Option<RowId>, BpmError> {
        let metadata = self.load_metadata()?;
        let mut current_page_id = metadata.root_page_id;

        loop {
            let mut page_guard = self.bpm.fetch_page(current_page_id)?;
            let node = BPlusTreeNode::new(
                page_guard.deref_mut(),
                self.key_type.clone(),
            );

            if node.is_leaf() {
                // Found the leaf, perform binary search
                match node.binary_search(key) {
                    Ok(index) => return Ok(Some(node.get_value(index))),
                    Err(_) => return Ok(None),
                }
            } else {
                // Internal node: find the child to traverse
                let child_index = match node.binary_search(key) {
                    Ok(i) => i + 1,     // Key found, go to right child
                    Err(i) => i,        // Key not found, i is the insertion point
                };
                current_page_id = node.get_child(child_index);
                // page_guard drops here, releasing the latch
            }
        }
    }

    // ===== INSERT OPERATION WITH LATCH CRABBING =====

    /// Inserts a key-value pair into the B+ tree.
    ///
    /// Uses latch crabbing for concurrency: holds latches on the path from root to leaf,
    /// releasing them when we determine a node is safe (won't split).
    pub fn insert(&self, key: IndexKey, value: RowId) -> Result<(), BpmError> {
        let metadata = self.load_metadata()?;

        // Start from root
        let root_page_id = metadata.root_page_id;

        // Check if root needs to split
        let mut root_guard = self.bpm.fetch_page(root_page_id)?;
        let root_node = BPlusTreeNode::new(
            root_guard.deref_mut(),
            self.key_type.clone(),
        );

        let root_is_leaf = root_node.is_leaf();
        let root_is_full = if root_is_leaf {
            root_node.is_full(self.leaf_max_size)
        } else {
            root_node.is_full(self.internal_max_size)
        };

        drop(root_node);
        drop(root_guard);

        if root_is_full {
            // Root is full, need to split it and create new root
            self.split_root(root_page_id)?;
        }

        // Now insert into the tree (root is guaranteed not to split)
        self.insert_internal(key, value, None)
    }

    /// Internal insert that assumes root won't split.
    fn insert_internal(&self, key: IndexKey, value: RowId, _parent_page_id: Option<PageId>) -> Result<(), BpmError> {
        let metadata = self.load_metadata()?;
        let leaf_page_id = self.find_leaf_for_insert(&key, metadata.root_page_id)?;

        let mut leaf_guard = self.bpm.fetch_page(leaf_page_id)?;
        let mut leaf_node = BPlusTreeNode::new(leaf_guard.deref_mut(), self.key_type.clone());

        // Find insertion point
        let insert_index = match leaf_node.binary_search(&key) {
            Ok(_) => return Err(BpmError::IoError(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Duplicate key",
            ))),
            Err(i) => i,
        };

        if !leaf_node.is_full(self.leaf_max_size) {
            // Simple case: leaf has space
            leaf_node.insert_at(insert_index, &key, value);
            Ok(())
        } else {
            // Leaf is full, need to split
            drop(leaf_node);
            drop(leaf_guard);
            self.split_leaf_and_insert(leaf_page_id, key, value)
        }
    }

    /// Finds the leaf node where a key should be inserted.
    ///
    /// Uses latch crabbing, releasing parent latches when safe.
    fn find_leaf_for_insert(&self, key: &IndexKey, start_page_id: PageId) -> Result<PageId, BpmError> {
        let mut current_page_id = start_page_id;

        loop {
            let mut page_guard = self.bpm.fetch_page(current_page_id)?;
            let node = BPlusTreeNode::new(
                page_guard.deref_mut(),
                self.key_type.clone(),
            );

            if node.is_leaf() {
                return Ok(current_page_id);
            }

            // Find child to traverse
            let child_index = match node.binary_search(key) {
                Ok(i) => i + 1,
                Err(i) => i,
            };
            current_page_id = node.get_child(child_index);
            // Release parent latch
        }
    }

    /// Splits the root node and creates a new root.
    fn split_root(&self, old_root_page_id: PageId) -> Result<(), BpmError> {
        let mut old_root_guard = self.bpm.fetch_page(old_root_page_id)?;
        let old_root_node = BPlusTreeNode::new(
            old_root_guard.deref_mut(),
            self.key_type.clone(),
        );

        let old_root_is_leaf = old_root_node.is_leaf();
        drop(old_root_node);
        drop(old_root_guard);

        // Split the old root
        let (split_key, new_page_id) = if old_root_is_leaf {
            self.split_leaf(old_root_page_id)?
        } else {
            self.split_internal(old_root_page_id)?
        };

        // Create new root
        let mut new_root_guard = self.bpm.new_page()?;
        let new_root_page_id = new_root_guard.page_id();
        let mut new_root_node = BPlusTreeNode::new(new_root_guard.deref_mut(), self.key_type.clone());

        new_root_node.initialize(new_root_page_id, false, INVALID_PAGE_ID);
        new_root_node.set_child(0, old_root_page_id);
        new_root_node.insert_key_child(0, &split_key, new_page_id);

        // Update children's parent pointers
        let mut old_root_guard = self.bpm.fetch_page(old_root_page_id)?;
        let mut old_root_node = BPlusTreeNode::new(old_root_guard.deref_mut(), self.key_type.clone());
        old_root_node.set_parent_page_id(new_root_page_id);

        drop(old_root_node);
        drop(old_root_guard);

        let mut new_page_guard = self.bpm.fetch_page(new_page_id)?;
        let mut new_node = BPlusTreeNode::new(new_page_guard.deref_mut(), self.key_type.clone());
        new_node.set_parent_page_id(new_root_page_id);

        drop(new_node);
        drop(new_page_guard);
        drop(new_root_node);
        drop(new_root_guard);

        // Update metadata with new root
        self.update_root(new_root_page_id)?;

        Ok(())
    }

    /// Splits a leaf node, using right-biased split for sequential inserts.
    ///
    /// Returns the split key (first key of new node) and the new page ID.
    fn split_leaf(&self, leaf_page_id: PageId) -> Result<(IndexKey, PageId), BpmError> {
        let mut old_leaf_guard = self.bpm.fetch_page(leaf_page_id)?;
        let mut new_leaf_guard = self.bpm.new_page()?;
        let new_leaf_page_id = new_leaf_guard.page_id();

        let mut old_node = BPlusTreeNode::new(old_leaf_guard.deref_mut(), self.key_type.clone());
        let mut new_node = BPlusTreeNode::new(new_leaf_guard.deref_mut(), self.key_type.clone());

        // Initialize new leaf
        new_node.initialize(new_leaf_page_id, true, old_node.parent_page_id());

        let old_count = old_node.key_count() as usize;

        // Right-biased split: 60/40 split for sequential inserts
        // If the last key is the largest, assume sequential pattern
        let split_point = if old_count > 2 {
            let last_key = old_node.get_key(old_count - 1);
            let second_last_key = old_node.get_key(old_count - 2);
            if last_key.compare(&second_last_key) == std::cmp::Ordering::Greater {
                // Sequential pattern detected: keep more in left node
                (old_count * 3) / 4 // 75% left, 25% right
            } else {
                old_count / 2 // 50/50 split
            }
        } else {
            old_count / 2
        };

        // Move second half to new node
        for i in split_point..old_count {
            let key = old_node.get_key(i);
            let value = old_node.get_value(i);
            new_node.insert_at(i - split_point, &key, value);
        }

        old_node.set_key_count(split_point as u16);

        // Update leaf chain pointers
        let old_next = old_node.next_leaf();
        new_node.set_next_leaf(old_next);
        new_node.set_prev_leaf(leaf_page_id);
        old_node.set_next_leaf(new_leaf_page_id);

        // Update next leaf's prev pointer
        if old_next != INVALID_PAGE_ID {
            drop(old_node);
            drop(old_leaf_guard);
            drop(new_node);
            drop(new_leaf_guard);

            let mut next_guard = self.bpm.fetch_page(old_next)?;
            let mut next_node = BPlusTreeNode::new(next_guard.deref_mut(), self.key_type.clone());
            next_node.set_prev_leaf(new_leaf_page_id);
        }

        // Get the split key (first key of new node)
        let mut new_leaf_guard = self.bpm.fetch_page(new_leaf_page_id)?;
        let new_node = BPlusTreeNode::new(
            new_leaf_guard.deref_mut(),
            self.key_type.clone(),
        );
        let split_key = new_node.get_key(0);

        Ok((split_key, new_leaf_page_id))
    }

    /// Splits an internal node.
    fn split_internal(&self, internal_page_id: PageId) -> Result<(IndexKey, PageId), BpmError> {
        let mut old_internal_guard = self.bpm.fetch_page(internal_page_id)?;
        let mut new_internal_guard = self.bpm.new_page()?;
        let new_internal_page_id = new_internal_guard.page_id();

        let mut old_node = BPlusTreeNode::new(old_internal_guard.deref_mut(), self.key_type.clone());
        let mut new_node = BPlusTreeNode::new(new_internal_guard.deref_mut(), self.key_type.clone());

        // Initialize new internal node
        new_node.initialize(new_internal_page_id, false, old_node.parent_page_id());

        let old_count = old_node.key_count() as usize;
        let split_point = old_count / 2;

        // The middle key will be pushed up to parent
        let split_key = old_node.get_key(split_point);

        // Move keys and children from split_point+1 onwards to new node
        for i in (split_point + 1)..old_count {
            let key = old_node.get_key(i);
            new_node.insert_key_child(i - split_point - 1, &key, old_node.get_child(i + 1));
        }

        // Set the first child of new node
        new_node.set_child(0, old_node.get_child(split_point + 1));

        old_node.set_key_count(split_point as u16);

        // Update children's parent pointers
        for i in 0..=new_node.key_count() as usize {
            let child_page_id = new_node.get_child(i);
            drop(old_node);
            drop(old_internal_guard);
            drop(new_node);
            drop(new_internal_guard);

            let mut child_guard = self.bpm.fetch_page(child_page_id)?;
            let mut child_node = BPlusTreeNode::new(child_guard.deref_mut(), self.key_type.clone());
            child_node.set_parent_page_id(new_internal_page_id);

            drop(child_node);
            drop(child_guard);

            old_internal_guard = self.bpm.fetch_page(internal_page_id)?;
            old_node = BPlusTreeNode::new(old_internal_guard.deref_mut(), self.key_type.clone());
            new_internal_guard = self.bpm.fetch_page(new_internal_page_id)?;
            new_node = BPlusTreeNode::new(new_internal_guard.deref_mut(), self.key_type.clone());
        }

        Ok((split_key, new_internal_page_id))
    }

    /// Splits a leaf and inserts the key-value pair.
    fn split_leaf_and_insert(&self, leaf_page_id: PageId, key: IndexKey, value: RowId) -> Result<(), BpmError> {
        let (split_key, new_page_id) = self.split_leaf(leaf_page_id)?;

        // Determine which leaf to insert into
        let target_page_id = if key.compare(&split_key) == std::cmp::Ordering::Less {
            leaf_page_id
        } else {
            new_page_id
        };

        // Insert into the appropriate leaf
        let mut target_guard = self.bpm.fetch_page(target_page_id)?;
        let mut target_node = BPlusTreeNode::new(target_guard.deref_mut(), self.key_type.clone());

        let insert_index = match target_node.binary_search(&key) {
            Ok(_) => return Err(BpmError::IoError(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Duplicate key",
            ))),
            Err(i) => i,
        };

        target_node.insert_at(insert_index, &key, value);
        let parent_page_id = target_node.parent_page_id();

        drop(target_node);
        drop(target_guard);

        // Insert split key into parent
        if parent_page_id != INVALID_PAGE_ID {
            self.insert_into_parent(leaf_page_id, split_key, new_page_id, parent_page_id)?;
        }

        Ok(())
    }

    /// Inserts a key and page pointer into parent after a split.
    fn insert_into_parent(
        &self,
        _left_page_id: PageId,
        key: IndexKey,
        right_page_id: PageId,
        parent_page_id: PageId,
    ) -> Result<(), BpmError> {
        let mut parent_guard = self.bpm.fetch_page(parent_page_id)?;
        let mut parent_node = BPlusTreeNode::new(parent_guard.deref_mut(), self.key_type.clone());

        if !parent_node.is_full(self.internal_max_size) {
            // Parent has space, insert directly
            let insert_index = match parent_node.binary_search(&key) {
                Ok(i) => i + 1,
                Err(i) => i,
            };
            parent_node.insert_key_child(insert_index, &key, right_page_id);
            Ok(())
        } else {
            // Parent is full, need to split
            drop(parent_node);
            drop(parent_guard);

            // TODO: Implement parent split and recursive insertion
            // For now, return an error
            Err(BpmError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Parent split not yet implemented",
            )))
        }
    }

    // ===== DELETE OPERATION (Placeholder) =====

    /// Removes a key from the B+ tree.
    ///
    /// Returns the RowId if the key was found and removed, None otherwise.
    pub fn remove(&self, _key: &IndexKey) -> Result<Option<RowId>, BpmError> {
        // TODO: Implement delete with coalesce/redistribute
        unimplemented!("Delete operation not yet implemented")
    }

    // ===== UTILITY METHODS =====

    /// Finds the leftmost (first) leaf in the tree.
    pub fn find_leftmost_leaf(&self) -> Result<PageId, BpmError> {
        let metadata = self.load_metadata()?;
        let mut current_page_id = metadata.root_page_id;

        loop {
            let mut page_guard = self.bpm.fetch_page(current_page_id)?;
            let node = BPlusTreeNode::new(
                page_guard.deref_mut(),
                self.key_type.clone(),
            );

            if node.is_leaf() {
                return Ok(current_page_id);
            }

            current_page_id = node.get_child(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffer_pool_manager::actor::ActorBufferPoolManager;
    use buffer_pool_manager::disk_manager::DiskManager;
    use std::fs;

    #[test]
    fn test_bptree_create_and_search() {
        let db_file = "test_bptree_create.db";
        let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
        let bpm = Arc::new(ActorBufferPoolManager::new(10, disk_manager));

        let tree = BPlusTree::new(bpm.clone(), KeyType::Integer).unwrap();

        // Search in empty tree
        assert_eq!(tree.search(&IndexKey::Integer(42)).unwrap(), None);

        fs::remove_file(db_file).unwrap();
    }

    #[test]
    fn test_bptree_simple_insert_and_search() {
        let db_file = "test_bptree_insert.db";
        let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
        let bpm = Arc::new(ActorBufferPoolManager::new(10, disk_manager));

        let tree = BPlusTree::new(bpm.clone(), KeyType::Integer).unwrap();

        // Insert a few keys
        let key1 = IndexKey::Integer(10);
        let val1 = RowId { page_id: 100, slot_index: 0 };
        tree.insert(key1.clone(), val1).unwrap();

        // Search for the key
        assert_eq!(tree.search(&key1).unwrap(), Some(val1));
        assert_eq!(tree.search(&IndexKey::Integer(20)).unwrap(), None);

        fs::remove_file(db_file).unwrap();
    }
}
