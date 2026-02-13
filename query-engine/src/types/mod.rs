//! Type system for query engine.
//!
//! Extends storage_engine types with NULL support and comparison operations.

use std::cmp::Ordering;
use storage_engine::tuple::Value as StorageValue;

/// A value that can be stored in a tuple, including NULL.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i32),
    Varchar(String),
    Null,
}

impl Value {
    /// Converts from storage_engine Value to query_engine Value.
    pub fn from_storage(value: StorageValue) -> Self {
        match value {
            StorageValue::Integer(i) => Value::Integer(i),
            StorageValue::Varchar(s) => Value::Varchar(s),
        }
    }

    /// Converts to storage_engine Value.
    ///
    /// Returns None if the value is NULL (storage engine doesn't support NULL yet).
    pub fn to_storage(&self) -> Option<StorageValue> {
        match self {
            Value::Integer(i) => Some(StorageValue::Integer(*i)),
            Value::Varchar(s) => Some(StorageValue::Varchar(s.clone())),
            Value::Null => None,
        }
    }

    /// Returns true if this value is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Compares two values using SQL semantics.
    ///
    /// NULL comparisons always return None (unknown).
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
            (Value::Varchar(a), Value::Varchar(b)) => Some(a.cmp(b)),
            _ => None, // Type mismatch
        }
    }

    /// Adds two values (for arithmetic expressions).
    pub fn add(&self, other: &Value) -> Option<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a + b)),
            (Value::Null, _) | (_, Value::Null) => Some(Value::Null),
            _ => None, // Type mismatch
        }
    }

    /// Subtracts two values.
    pub fn subtract(&self, other: &Value) -> Option<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a - b)),
            (Value::Null, _) | (_, Value::Null) => Some(Value::Null),
            _ => None,
        }
    }

    /// Multiplies two values.
    pub fn multiply(&self, other: &Value) -> Option<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a * b)),
            (Value::Null, _) | (_, Value::Null) => Some(Value::Null),
            _ => None,
        }
    }

    /// Divides two values.
    pub fn divide(&self, other: &Value) -> Option<Value> {
        match (self, other) {
            (Value::Integer(_), Value::Integer(0)) => None, // Division by zero
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a / b)),
            (Value::Null, _) | (_, Value::Null) => Some(Value::Null),
            _ => None,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Integer(i) => write!(f, "{}", i),
            Value::Varchar(s) => write!(f, "{}", s),
            Value::Null => write!(f, "NULL"),
        }
    }
}
