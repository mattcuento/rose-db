//! Index metadata management.
//!
//! This module defines how index metadata is stored and retrieved from disk.
//! The metadata includes the root page ID, key type, and computed fanout values.

use buffer_pool_manager::api::{PageId, INVALID_PAGE_ID, PAGE_SIZE};
use super::key::KeyType;

/// Index metadata stored in a dedicated page.
///
/// Memory layout:
/// - Bytes 0-7: root_page_id (usize, little-endian)
/// - Byte 8: key_type discriminant (u8)
/// - Bytes 9-12: max_key_length for Varchar (u32, little-endian, 0 for Integer)
/// - Bytes 13-14: leaf_max_size (u16, little-endian)
/// - Bytes 15-16: internal_max_size (u16, little-endian)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexMetadata {
    /// The page ID of the root node of the B+ tree.
    pub root_page_id: PageId,
    /// The type of keys stored in this index.
    pub key_type: KeyType,
    /// Maximum number of entries in a leaf node.
    pub leaf_max_size: u16,
    /// Maximum number of keys (and children) in an internal node.
    pub internal_max_size: u16,
}

impl IndexMetadata {
    /// Header size for the metadata page.
    const HEADER_SIZE: usize = 17;

    /// Creates new index metadata with computed fanout based on key type.
    pub fn new(key_type: KeyType) -> Self {
        let (leaf_max_size, internal_max_size) = Self::compute_fanout(&key_type);
        Self {
            root_page_id: INVALID_PAGE_ID,
            key_type,
            leaf_max_size,
            internal_max_size,
        }
    }

    /// Computes the maximum fanout for leaf and internal nodes based on key type.
    ///
    /// Leaf node calculation:
    /// - Header: 32 bytes (page_id, is_leaf, key_count, parent, next, prev)
    /// - Per entry: key_size + 12 bytes (RowId: page_id + slot_index + padding)
    ///
    /// Internal node calculation:
    /// - Header: 24 bytes (page_id, is_leaf, key_count, parent, padding)
    /// - Per key: key_size
    /// - Per child: 8 bytes (PageId)
    fn compute_fanout(key_type: &KeyType) -> (u16, u16) {
        const LEAF_HEADER_SIZE: usize = 32;
        const INTERNAL_HEADER_SIZE: usize = 24;
        const ROW_ID_SIZE: usize = 12; // PageId (8) + slot_index (2) + padding (2)
        const PAGE_ID_SIZE: usize = 8;

        let max_key_size = key_type.max_size();

        // For leaf nodes: each entry is (key + RowId)
        let leaf_entry_size = max_key_size + ROW_ID_SIZE;
        let leaf_max_size = (PAGE_SIZE - LEAF_HEADER_SIZE) / leaf_entry_size;

        // For internal nodes: keys array + children array (n keys, n+1 children)
        // Approximate: (max_key_size + PAGE_ID_SIZE) per key
        let internal_entry_size = max_key_size + PAGE_ID_SIZE;
        let internal_max_size = (PAGE_SIZE - INTERNAL_HEADER_SIZE) / internal_entry_size;

        (
            leaf_max_size as u16,
            internal_max_size as u16,
        )
    }

    /// Serializes the metadata to bytes for storage in a page.
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::HEADER_SIZE);

        // root_page_id (8 bytes)
        bytes.extend_from_slice(&self.root_page_id.to_le_bytes());

        // key_type discriminant (1 byte) + max_key_length (4 bytes)
        match &self.key_type {
            KeyType::Integer => {
                bytes.push(0);
                bytes.extend_from_slice(&0u32.to_le_bytes());
            }
            KeyType::Varchar { max_length } => {
                bytes.push(1);
                bytes.extend_from_slice(&max_length.to_le_bytes());
            }
        }

        // leaf_max_size (2 bytes)
        bytes.extend_from_slice(&self.leaf_max_size.to_le_bytes());

        // internal_max_size (2 bytes)
        bytes.extend_from_slice(&self.internal_max_size.to_le_bytes());

        bytes
    }

    /// Deserializes metadata from bytes.
    ///
    /// # Panics
    /// Panics if the bytes are invalid.
    pub fn deserialize(bytes: &[u8]) -> Self {
        assert!(
            bytes.len() >= Self::HEADER_SIZE,
            "Invalid metadata bytes: too short"
        );

        let root_page_id = usize::from_le_bytes(bytes[0..8].try_into().unwrap());

        let key_type_discriminant = bytes[8];
        let max_key_length = u32::from_le_bytes(bytes[9..13].try_into().unwrap());
        let key_type = match key_type_discriminant {
            0 => KeyType::Integer,
            1 => KeyType::Varchar {
                max_length: max_key_length,
            },
            _ => panic!("Invalid key type discriminant: {}", key_type_discriminant),
        };

        let leaf_max_size = u16::from_le_bytes(bytes[13..15].try_into().unwrap());
        let internal_max_size = u16::from_le_bytes(bytes[15..17].try_into().unwrap());

        Self {
            root_page_id,
            key_type,
            leaf_max_size,
            internal_max_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_metadata_serialization() {
        let metadata = IndexMetadata::new(KeyType::Integer);
        let bytes = metadata.serialize();
        assert_eq!(bytes.len(), IndexMetadata::HEADER_SIZE);

        let deserialized = IndexMetadata::deserialize(&bytes);
        assert_eq!(metadata, deserialized);
    }

    #[test]
    fn test_varchar_metadata_serialization() {
        let metadata = IndexMetadata::new(KeyType::Varchar { max_length: 100 });
        let bytes = metadata.serialize();
        assert_eq!(bytes.len(), IndexMetadata::HEADER_SIZE);

        let deserialized = IndexMetadata::deserialize(&bytes);
        assert_eq!(metadata, deserialized);
    }

    #[test]
    fn test_integer_fanout_computation() {
        let metadata = IndexMetadata::new(KeyType::Integer);
        // Integer keys: 4 bytes
        // Leaf: (4096 - 32) / (4 + 12) = 254
        // Internal: (4096 - 24) / (4 + 8) = 339
        assert_eq!(metadata.leaf_max_size, 254);
        assert_eq!(metadata.internal_max_size, 339);
    }

    #[test]
    fn test_varchar_fanout_computation() {
        let metadata = IndexMetadata::new(KeyType::Varchar { max_length: 100 });
        // Varchar keys: 4 + 100 = 104 bytes
        // Leaf: (4096 - 32) / (104 + 12) = 35
        // Internal: (4096 - 24) / (104 + 8) = 36
        assert_eq!(metadata.leaf_max_size, 35);
        assert_eq!(metadata.internal_max_size, 36);
    }
}
