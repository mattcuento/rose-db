//! Query engine for rose-db with DataFrame-style API.
//!
//! This crate provides a programmatic query interface inspired by Polars and DataFusion.
//! Users can build queries using method chaining instead of SQL strings.
//!
//! # Example
//!
//! ```no_run
//! use query_engine::Database;
//!
//! let db = Database::open("mydb.db")?;
//!
//! // Create table
//! db.create_table("users", schema! {
//!     "id" => Type::Integer,
//!     "name" => Type::Varchar(50),
//! })?;
//!
//! // Query with DataFrame API
//! let results = db.table("users")
//!     .filter(col("id").gt(10))
//!     .select(&["name"])
//!     .collect()?;
//! ```

pub mod catalog;
pub mod types;
pub mod expression;
pub mod executor;
mod database;
mod dataframe;

pub use database::Database;
pub use dataframe::DataFrame;
pub use expression::{col, lit, Expression};
pub use types::Value;

// Re-export commonly used types from storage_engine
pub use storage_engine::tuple::{Schema, Column, Type};

// Helper function for creating columns
pub fn column(name: &str, column_type: Type, length: u32) -> Column {
    Column {
        name: name.to_string(),
        column_type,
        length,
    }
}

// Convenience helpers for specific types
pub fn int_column(name: &str) -> Column {
    column(name, Type::Integer, 4)
}

pub fn varchar_column(name: &str, length: u32) -> Column {
    column(name, Type::Varchar, length)
}

/// A specialized error type for query engine operations.
#[derive(Debug)]
pub enum QueryError {
    /// Table not found in catalog
    TableNotFound(String),
    /// Column not found in schema
    ColumnNotFound(String),
    /// Type mismatch in expression
    TypeMismatch(String),
    /// Execution error
    ExecutionError(String),
    /// Buffer pool manager error
    BpmError(buffer_pool_manager::api::BpmError),
    /// I/O error
    IoError(std::io::Error),
}

impl From<buffer_pool_manager::api::BpmError> for QueryError {
    fn from(err: buffer_pool_manager::api::BpmError) -> Self {
        QueryError::BpmError(err)
    }
}

impl From<std::io::Error> for QueryError {
    fn from(err: std::io::Error) -> Self {
        QueryError::IoError(err)
    }
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::TableNotFound(name) => write!(f, "Table not found: {}", name),
            QueryError::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            QueryError::TypeMismatch(msg) => write!(f, "Type mismatch: {}", msg),
            QueryError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            QueryError::BpmError(err) => write!(f, "Buffer pool error: {:?}", err),
            QueryError::IoError(err) => write!(f, "I/O error: {}", err),
        }
    }
}

impl std::error::Error for QueryError {}

pub type Result<T> = std::result::Result<T, QueryError>;
