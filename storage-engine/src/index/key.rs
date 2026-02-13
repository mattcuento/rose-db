//! Key abstraction for B+ tree indexes.
//!
//! This module defines the key types that can be used in B+ tree indexes,
//! providing comparison, serialization, and deserialization capabilities.

use std::cmp::Ordering;

/// The type of key stored in an index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyType {
    /// Integer key type (4 bytes).
    Integer,
    /// Variable-length character key type with maximum length.
    Varchar { max_length: u32 },
}

impl KeyType {
    /// Returns the maximum serialized size for this key type in bytes.
    pub fn max_size(&self) -> usize {
        match self {
            KeyType::Integer => 4,
            KeyType::Varchar { max_length } => 4 + (*max_length as usize),
        }
    }
}

/// A key value that can be stored in a B+ tree index.
///
/// Supports Integer and Varchar types, matching the storage engine's Value types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexKey {
    /// An integer key value.
    Integer(i32),
    /// A variable-length string key value.
    Varchar(String),
}

impl IndexKey {
    /// Compares this key with another key.
    ///
    /// # Panics
    /// Panics if comparing keys of different types.
    pub fn compare(&self, other: &Self) -> Ordering {
        match (self, other) {
            (IndexKey::Integer(a), IndexKey::Integer(b)) => a.cmp(b),
            (IndexKey::Varchar(a), IndexKey::Varchar(b)) => a.cmp(b),
            _ => panic!("Cannot compare keys of different types"),
        }
    }

    /// Serializes the key to bytes.
    ///
    /// Format:
    /// - Integer: 4 bytes (i32 in native endian)
    /// - Varchar: 4 bytes (length as u32) + UTF-8 bytes
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            IndexKey::Integer(val) => val.to_ne_bytes().to_vec(),
            IndexKey::Varchar(val) => {
                let len = val.len() as u32;
                let mut bytes = Vec::with_capacity(4 + val.len());
                bytes.extend_from_slice(&len.to_ne_bytes());
                bytes.extend_from_slice(val.as_bytes());
                bytes
            }
        }
    }

    /// Deserializes a key from bytes based on the key type.
    ///
    /// # Panics
    /// Panics if the bytes are invalid for the given key type.
    pub fn deserialize(bytes: &[u8], key_type: &KeyType) -> Self {
        match key_type {
            KeyType::Integer => {
                assert!(bytes.len() >= 4, "Invalid integer key bytes");
                let val = i32::from_ne_bytes(bytes[0..4].try_into().unwrap());
                IndexKey::Integer(val)
            }
            KeyType::Varchar { .. } => {
                assert!(bytes.len() >= 4, "Invalid varchar key bytes");
                let len = u32::from_ne_bytes(bytes[0..4].try_into().unwrap()) as usize;
                assert!(
                    bytes.len() >= 4 + len,
                    "Invalid varchar key bytes: length mismatch"
                );
                let val = String::from_utf8(bytes[4..4 + len].to_vec())
                    .expect("Invalid UTF-8 in varchar key");
                IndexKey::Varchar(val)
            }
        }
    }

    /// Returns the serialized size of this key in bytes.
    pub fn serialized_size(&self) -> usize {
        match self {
            IndexKey::Integer(_) => 4,
            IndexKey::Varchar(val) => 4 + val.len(),
        }
    }

    /// Returns the key type of this key.
    pub fn key_type(&self) -> KeyType {
        match self {
            IndexKey::Integer(_) => KeyType::Integer,
            IndexKey::Varchar(val) => KeyType::Varchar {
                max_length: val.len() as u32,
            },
        }
    }
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.compare(other))
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_key_serialization() {
        let key = IndexKey::Integer(42);
        let bytes = key.serialize();
        assert_eq!(bytes.len(), 4);

        let deserialized = IndexKey::deserialize(&bytes, &KeyType::Integer);
        assert_eq!(key, deserialized);
    }

    #[test]
    fn test_varchar_key_serialization() {
        let key = IndexKey::Varchar("hello".to_string());
        let bytes = key.serialize();
        assert_eq!(bytes.len(), 4 + 5); // 4 bytes for length + 5 bytes for "hello"

        let deserialized = IndexKey::deserialize(&bytes, &KeyType::Varchar { max_length: 100 });
        assert_eq!(key, deserialized);
    }

    #[test]
    fn test_integer_key_comparison() {
        let key1 = IndexKey::Integer(10);
        let key2 = IndexKey::Integer(20);
        let key3 = IndexKey::Integer(10);

        assert_eq!(key1.compare(&key2), Ordering::Less);
        assert_eq!(key2.compare(&key1), Ordering::Greater);
        assert_eq!(key1.compare(&key3), Ordering::Equal);
    }

    #[test]
    fn test_varchar_key_comparison() {
        let key1 = IndexKey::Varchar("apple".to_string());
        let key2 = IndexKey::Varchar("banana".to_string());
        let key3 = IndexKey::Varchar("apple".to_string());

        assert_eq!(key1.compare(&key2), Ordering::Less);
        assert_eq!(key2.compare(&key1), Ordering::Greater);
        assert_eq!(key1.compare(&key3), Ordering::Equal);
    }

    #[test]
    fn test_key_type_max_size() {
        let int_type = KeyType::Integer;
        assert_eq!(int_type.max_size(), 4);

        let varchar_type = KeyType::Varchar { max_length: 100 };
        assert_eq!(varchar_type.max_size(), 104); // 4 + 100
    }
}
