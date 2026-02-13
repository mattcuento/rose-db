//! B+ tree index implementation for rose-db.
//!
//! This module provides a complete B+ tree index with:
//! - Latch coupling (crabbing) for concurrent access
//! - Support for Integer and Varchar keys
//! - Efficient range scans via leaf chain
//! - Right-biased splits for sequential workloads

pub mod key;
pub mod metadata;
pub mod node;
pub mod bptree;
pub mod iterator;

// Re-export main types
pub use key::{IndexKey, KeyType};
pub use metadata::IndexMetadata;
pub use bptree::BPlusTree;
pub use iterator::BPlusTreeIterator;
