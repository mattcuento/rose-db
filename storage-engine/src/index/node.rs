//! B+ tree node implementation.
//!
//! This module defines the layout and operations for B+ tree nodes (both leaf and internal).
//! Uses a custom page format optimized for index operations.

use buffer_pool_manager::api::{PageId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::table::RowId;
use super::key::{IndexKey, KeyType};

/// Memory layout for leaf nodes:
/// - Bytes 0-7: page_id (usize, little-endian)
/// - Byte 8: is_leaf (bool, 1 for leaf, 0 for internal)
/// - Bytes 9-10: key_count (u16, little-endian)
/// - Bytes 11-18: parent_page_id (usize, little-endian)
/// - Bytes 19-26: next_leaf_page_id (usize, little-endian)
/// - Bytes 27-34: prev_leaf_page_id (usize, little-endian)
/// - Bytes 35+: Key-value pairs (variable size)
///
/// Memory layout for internal nodes:
/// - Bytes 0-7: page_id (usize, little-endian)
/// - Byte 8: is_leaf (bool, 1 for leaf, 0 for internal)
/// - Bytes 9-10: key_count (u16, little-endian)
/// - Bytes 11-18: parent_page_id (usize, little-endian)
/// - Bytes 19-26: (unused padding for alignment)
/// - Bytes 27+: Keys and child pointers (variable size)

const PAGE_ID_OFFSET: usize = 0;
const IS_LEAF_OFFSET: usize = 8;
const KEY_COUNT_OFFSET: usize = 9;
const PARENT_PAGE_ID_OFFSET: usize = 11;
const NEXT_LEAF_OFFSET: usize = 19;
const PREV_LEAF_OFFSET: usize = 27;
const LEAF_DATA_OFFSET: usize = 35;
const INTERNAL_DATA_OFFSET: usize = 27;

/// A B+ tree node that provides access to page data.
///
/// This structure wraps a mutable byte slice and provides typed access
/// to the node's fields and data.
pub struct BPlusTreeNode<'a> {
    data: &'a mut [u8],
    key_type: KeyType,
}

impl<'a> BPlusTreeNode<'a> {
    /// Creates a new B+ tree node from a byte slice.
    pub fn new(data: &'a mut [u8], key_type: KeyType) -> Self {
        assert!(data.len() >= PAGE_SIZE, "Buffer too small for B+ tree node");
        Self { data, key_type }
    }

    /// Initializes a new node (leaf or internal).
    pub fn initialize(&mut self, page_id: PageId, is_leaf: bool, parent_page_id: PageId) {
        self.set_page_id(page_id);
        self.set_is_leaf(is_leaf);
        self.set_key_count(0);
        self.set_parent_page_id(parent_page_id);

        if is_leaf {
            self.set_next_leaf(INVALID_PAGE_ID);
            self.set_prev_leaf(INVALID_PAGE_ID);
        }
    }

    // ===== Header Accessors =====

    /// Returns the page ID of this node.
    pub fn page_id(&self) -> PageId {
        usize::from_le_bytes(self.data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 8].try_into().unwrap())
    }

    /// Sets the page ID of this node.
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 8].copy_from_slice(&page_id.to_le_bytes());
    }

    /// Returns whether this node is a leaf node.
    pub fn is_leaf(&self) -> bool {
        self.data[IS_LEAF_OFFSET] != 0
    }

    /// Sets whether this node is a leaf node.
    pub fn set_is_leaf(&mut self, is_leaf: bool) {
        self.data[IS_LEAF_OFFSET] = if is_leaf { 1 } else { 0 };
    }

    /// Returns the number of keys in this node.
    pub fn key_count(&self) -> u16 {
        u16::from_le_bytes(self.data[KEY_COUNT_OFFSET..KEY_COUNT_OFFSET + 2].try_into().unwrap())
    }

    /// Sets the number of keys in this node.
    pub fn set_key_count(&mut self, count: u16) {
        self.data[KEY_COUNT_OFFSET..KEY_COUNT_OFFSET + 2].copy_from_slice(&count.to_le_bytes());
    }

    /// Returns the parent page ID.
    pub fn parent_page_id(&self) -> PageId {
        usize::from_le_bytes(
            self.data[PARENT_PAGE_ID_OFFSET..PARENT_PAGE_ID_OFFSET + 8]
                .try_into()
                .unwrap(),
        )
    }

    /// Sets the parent page ID.
    pub fn set_parent_page_id(&mut self, parent_page_id: PageId) {
        self.data[PARENT_PAGE_ID_OFFSET..PARENT_PAGE_ID_OFFSET + 8]
            .copy_from_slice(&parent_page_id.to_le_bytes());
    }

    // ===== Leaf-specific Accessors =====

    /// Returns the next leaf page ID (only valid for leaf nodes).
    pub fn next_leaf(&self) -> PageId {
        assert!(self.is_leaf(), "next_leaf() called on internal node");
        usize::from_le_bytes(
            self.data[NEXT_LEAF_OFFSET..NEXT_LEAF_OFFSET + 8]
                .try_into()
                .unwrap(),
        )
    }

    /// Sets the next leaf page ID (only valid for leaf nodes).
    pub fn set_next_leaf(&mut self, page_id: PageId) {
        assert!(self.is_leaf(), "set_next_leaf() called on internal node");
        self.data[NEXT_LEAF_OFFSET..NEXT_LEAF_OFFSET + 8]
            .copy_from_slice(&page_id.to_le_bytes());
    }

    /// Returns the previous leaf page ID (only valid for leaf nodes).
    pub fn prev_leaf(&self) -> PageId {
        assert!(self.is_leaf(), "prev_leaf() called on internal node");
        usize::from_le_bytes(
            self.data[PREV_LEAF_OFFSET..PREV_LEAF_OFFSET + 8]
                .try_into()
                .unwrap(),
        )
    }

    /// Sets the previous leaf page ID (only valid for leaf nodes).
    pub fn set_prev_leaf(&mut self, page_id: PageId) {
        assert!(self.is_leaf(), "set_prev_leaf() called on internal node");
        self.data[PREV_LEAF_OFFSET..PREV_LEAF_OFFSET + 8]
            .copy_from_slice(&page_id.to_le_bytes());
    }

    // ===== Key Operations =====

    /// Returns the key at the specified index.
    pub fn get_key(&self, index: usize) -> IndexKey {
        assert!(index < self.key_count() as usize, "Key index out of bounds");
        let offset = self.key_offset(index);
        IndexKey::deserialize(&self.data[offset..], &self.key_type)
    }

    /// Sets the key at the specified index.
    pub fn set_key(&mut self, index: usize, key: &IndexKey) {
        assert!(index < self.key_count() as usize, "Key index out of bounds");
        let offset = self.key_offset(index);
        let serialized = key.serialize();
        let max_size = self.key_type.max_size();

        // Write the key and pad with zeros if necessary
        self.data[offset..offset + serialized.len()].copy_from_slice(&serialized);
        if serialized.len() < max_size {
            self.data[offset + serialized.len()..offset + max_size].fill(0);
        }
    }

    /// Calculates the offset for a key at the given index.
    fn key_offset(&self, index: usize) -> usize {
        let base_offset = if self.is_leaf() {
            LEAF_DATA_OFFSET
        } else {
            INTERNAL_DATA_OFFSET
        };

        let max_key_size = self.key_type.max_size();

        if self.is_leaf() {
            // Leaf: each entry is (key + RowId)
            base_offset + index * (max_key_size + 12)
        } else {
            // Internal: keys array, then children array
            base_offset + index * max_key_size
        }
    }

    // ===== Value Operations (Leaf Nodes Only) =====

    /// Returns the RowId at the specified index (leaf nodes only).
    pub fn get_value(&self, index: usize) -> RowId {
        assert!(self.is_leaf(), "get_value() called on internal node");
        assert!(index < self.key_count() as usize, "Value index out of bounds");

        let offset = self.value_offset(index);
        let page_id = usize::from_le_bytes(self.data[offset..offset + 8].try_into().unwrap());
        let slot_index = u16::from_le_bytes(self.data[offset + 8..offset + 10].try_into().unwrap());

        RowId { page_id, slot_index }
    }

    /// Sets the RowId at the specified index (leaf nodes only).
    pub fn set_value(&mut self, index: usize, value: RowId) {
        assert!(self.is_leaf(), "set_value() called on internal node");
        assert!(index < self.key_count() as usize, "Value index out of bounds");

        let offset = self.value_offset(index);
        self.data[offset..offset + 8].copy_from_slice(&value.page_id.to_le_bytes());
        self.data[offset + 8..offset + 10].copy_from_slice(&value.slot_index.to_le_bytes());
        // Padding bytes 10-12 remain as is
    }

    /// Calculates the offset for a value (RowId) at the given index.
    fn value_offset(&self, index: usize) -> usize {
        let max_key_size = self.key_type.max_size();
        LEAF_DATA_OFFSET + index * (max_key_size + 12) + max_key_size
    }

    // ===== Child Operations (Internal Nodes Only) =====

    /// Returns the child page ID at the specified index (internal nodes only).
    pub fn get_child(&self, index: usize) -> PageId {
        assert!(!self.is_leaf(), "get_child() called on leaf node");
        assert!(index <= self.key_count() as usize, "Child index out of bounds");

        let offset = self.child_offset(index);
        usize::from_le_bytes(self.data[offset..offset + 8].try_into().unwrap())
    }

    /// Sets the child page ID at the specified index (internal nodes only).
    pub fn set_child(&mut self, index: usize, child_page_id: PageId) {
        assert!(!self.is_leaf(), "set_child() called on leaf node");
        assert!(index <= self.key_count() as usize, "Child index out of bounds");

        let offset = self.child_offset(index);
        self.data[offset..offset + 8].copy_from_slice(&child_page_id.to_le_bytes());
    }

    /// Calculates the offset for a child pointer at the given index.
    fn child_offset(&self, index: usize) -> usize {
        let max_key_size = self.key_type.max_size();
        let key_count = self.key_count() as usize;

        // Children array starts after all keys
        let children_base = INTERNAL_DATA_OFFSET + key_count * max_key_size;
        children_base + index * 8
    }

    // ===== Utility Methods =====

    /// Checks if the node is full (reached maximum capacity).
    pub fn is_full(&self, max_size: u16) -> bool {
        self.key_count() >= max_size
    }

    /// Checks if the node has underflowed (below minimum capacity).
    pub fn is_underflow(&self, max_size: u16) -> bool {
        let min_size = (max_size + 1) / 2; // Ceiling division
        self.key_count() < min_size
    }

    /// Performs binary search for a key.
    ///
    /// Returns Ok(index) if the key is found, or Err(index) indicating where
    /// the key should be inserted to maintain sorted order.
    pub fn binary_search(&self, key: &IndexKey) -> Result<usize, usize> {
        let count = self.key_count() as usize;
        let mut left = 0;
        let mut right = count;

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.get_key(mid);

            match key.compare(&mid_key) {
                std::cmp::Ordering::Less => right = mid,
                std::cmp::Ordering::Greater => left = mid + 1,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }

        Err(left)
    }

    /// Inserts a key-value pair at the specified index (leaf nodes only).
    ///
    /// Shifts existing entries to the right to make space.
    pub fn insert_at(&mut self, index: usize, key: &IndexKey, value: RowId) {
        assert!(self.is_leaf(), "insert_at() called on internal node");
        let count = self.key_count() as usize;
        assert!(index <= count, "Insert index out of bounds");

        // Update count first so that set_key and set_value work correctly
        self.set_key_count((count + 1) as u16);

        // Shift entries to the right
        if index < count {
            let max_key_size = self.key_type.max_size();
            let entry_size = max_key_size + 12;
            let src_offset = self.key_offset(index);
            let dst_offset = src_offset + entry_size;
            let bytes_to_move = (count - index) * entry_size;

            // Use copy_within for safe overlapping copy
            self.data.copy_within(src_offset..src_offset + bytes_to_move, dst_offset);
        }

        // Insert the new key-value pair
        self.set_key(index, key);
        self.set_value(index, value);
    }

    /// Inserts a key and child pointer at the specified index (internal nodes only).
    pub fn insert_key_child(&mut self, index: usize, key: &IndexKey, right_child: PageId) {
        assert!(!self.is_leaf(), "insert_key_child() called on leaf node");
        let count = self.key_count() as usize;
        assert!(index <= count, "Insert index out of bounds");

        // Update count first
        self.set_key_count((count + 1) as u16);

        // Shift keys to the right
        if index < count {
            let max_key_size = self.key_type.max_size();
            let src_offset = self.key_offset(index);
            let dst_offset = src_offset + max_key_size;
            let bytes_to_move = (count - index) * max_key_size;
            self.data.copy_within(src_offset..src_offset + bytes_to_move, dst_offset);
        }

        // Shift children to the right (n+1 children for n keys)
        // Need to recalculate offsets after count update
        let child_offset_src = INTERNAL_DATA_OFFSET + count * self.key_type.max_size() + (index + 1) * 8;
        let child_offset_dst = child_offset_src + 8;
        let children_to_move = (count - index) * 8;
        self.data.copy_within(child_offset_src..child_offset_src + children_to_move, child_offset_dst);

        // Insert the new key and child
        self.set_key(index, key);
        self.set_child(index + 1, right_child);
    }

    /// Removes a key-value pair at the specified index (leaf nodes only).
    pub fn remove_at(&mut self, index: usize) {
        assert!(self.is_leaf(), "remove_at() called on internal node");
        let count = self.key_count() as usize;
        assert!(index < count, "Remove index out of bounds");

        // Shift entries to the left
        if index < count - 1 {
            let max_key_size = self.key_type.max_size();
            let entry_size = max_key_size + 12;
            let src_offset = self.key_offset(index + 1);
            let dst_offset = self.key_offset(index);
            let bytes_to_move = (count - index - 1) * entry_size;

            self.data.copy_within(src_offset..src_offset + bytes_to_move, dst_offset);
        }

        self.set_key_count((count - 1) as u16);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node(_is_leaf: bool) -> Vec<u8> {
        vec![0; PAGE_SIZE]
    }

    #[test]
    fn test_node_initialization() {
        let mut data = create_test_node(true);
        let mut node = BPlusTreeNode::new(&mut data, KeyType::Integer);

        node.initialize(42, true, 10);

        assert_eq!(node.page_id(), 42);
        assert!(node.is_leaf());
        assert_eq!(node.key_count(), 0);
        assert_eq!(node.parent_page_id(), 10);
        assert_eq!(node.next_leaf(), INVALID_PAGE_ID);
        assert_eq!(node.prev_leaf(), INVALID_PAGE_ID);
    }

    #[test]
    fn test_leaf_insert_and_search() {
        let mut data = create_test_node(true);
        let mut node = BPlusTreeNode::new(&mut data, KeyType::Integer);
        node.initialize(1, true, INVALID_PAGE_ID);

        let key1 = IndexKey::Integer(10);
        let val1 = RowId { page_id: 100, slot_index: 1 };

        node.insert_at(0, &key1, val1);
        assert_eq!(node.key_count(), 1);
        assert_eq!(node.get_key(0), key1);
        assert_eq!(node.get_value(0), val1);

        assert_eq!(node.binary_search(&key1), Ok(0));
        assert_eq!(node.binary_search(&IndexKey::Integer(5)), Err(0));
        assert_eq!(node.binary_search(&IndexKey::Integer(15)), Err(1));
    }

    #[test]
    fn test_internal_node_operations() {
        let mut data = create_test_node(false);
        let mut node = BPlusTreeNode::new(&mut data, KeyType::Integer);
        node.initialize(1, false, INVALID_PAGE_ID);

        node.set_child(0, 100); // Leftmost child
        node.insert_key_child(0, &IndexKey::Integer(50), 200);

        assert_eq!(node.key_count(), 1);
        assert_eq!(node.get_key(0), IndexKey::Integer(50));
        assert_eq!(node.get_child(0), 100);
        assert_eq!(node.get_child(1), 200);
    }
}
