//! Catalog for storing and managing table metadata.
//!
//! The catalog keeps track of all tables, their schemas, and associated TableHeap instances.

use crate::{QueryError, Result};
use buffer_pool_manager::api::BufferPoolManager;
use storage_engine::table::TableHeap;
use storage_engine::tuple::Schema;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Information about a table in the database.
#[derive(Clone)]
pub struct TableInfo {
    pub table_id: u32,
    pub name: String,
    pub schema: Schema,
    pub table_heap: Arc<TableHeap>,
}

impl TableInfo {
    pub fn new(table_id: u32, name: String, schema: Schema, table_heap: Arc<TableHeap>) -> Self {
        Self {
            table_id,
            name,
            schema,
            table_heap,
        }
    }
}

/// The database catalog.
///
/// Stores metadata about all tables in the database and provides lookup APIs.
/// Uses RwLock for concurrent reads (queries) and exclusive writes (DDL).
pub struct Catalog {
    tables: RwLock<HashMap<String, Arc<TableInfo>>>,
    next_table_id: RwLock<u32>,
    bpm: Arc<dyn BufferPoolManager>,
}

impl Catalog {
    /// Creates a new empty catalog.
    pub fn new(bpm: Arc<dyn BufferPoolManager>) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            next_table_id: RwLock::new(1),
            bpm,
        }
    }

    /// Creates a new table in the catalog.
    pub fn create_table(&self, name: String, schema: Schema) -> Result<Arc<TableInfo>> {
        let mut tables = self.tables.write().unwrap();

        // Check if table already exists
        if tables.contains_key(&name) {
            return Err(QueryError::ExecutionError(format!(
                "Table '{}' already exists",
                name
            )));
        }

        // Allocate table ID
        let mut next_id = self.next_table_id.write().unwrap();
        let table_id = *next_id;
        *next_id += 1;
        drop(next_id);

        // Create TableHeap (new() doesn't return Result)
        let table_heap = Arc::new(TableHeap::new(self.bpm.clone(), schema.clone()));

        // Create TableInfo
        let table_info = Arc::new(TableInfo::new(table_id, name.clone(), schema, table_heap));

        // Add to catalog
        tables.insert(name, table_info.clone());

        Ok(table_info)
    }

    /// Looks up a table by name.
    pub fn get_table(&self, name: &str) -> Result<Arc<TableInfo>> {
        let tables = self.tables.read().unwrap();
        tables
            .get(name)
            .cloned()
            .ok_or_else(|| QueryError::TableNotFound(name.to_string()))
    }

    /// Returns all table names in the catalog.
    pub fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect()
    }

    /// Drops a table from the catalog.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut tables = self.tables.write().unwrap();
        tables
            .remove(name)
            .ok_or_else(|| QueryError::TableNotFound(name.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffer_pool_manager::actor::ActorBufferPoolManager;
    use buffer_pool_manager::disk_manager::DiskManager;
    use storage_engine::tuple::{Column, Type};

    #[test]
    fn test_catalog_create_and_get_table() {
        let disk_manager = Arc::new(DiskManager::new("test_catalog.db", false).unwrap());
        let bpm = Arc::new(ActorBufferPoolManager::new(10, disk_manager));
        let catalog = Catalog::new(bpm);

        let schema = Schema {
            columns: vec![
                Column::new("id".to_string(), Type::Integer),
                Column::new("name".to_string(), Type::Varchar(50)),
            ],
        };

        // Create table
        let table_info = catalog.create_table("users".to_string(), schema.clone()).unwrap();
        assert_eq!(table_info.name, "users");
        assert_eq!(table_info.table_id, 1);

        // Get table
        let retrieved = catalog.get_table("users").unwrap();
        assert_eq!(retrieved.name, "users");

        // List tables
        let tables = catalog.list_tables();
        assert_eq!(tables, vec!["users"]);

        std::fs::remove_file("test_catalog.db").unwrap();
    }

    #[test]
    fn test_catalog_table_not_found() {
        let disk_manager = Arc::new(DiskManager::new("test_catalog2.db", false).unwrap());
        let bpm = Arc::new(ActorBufferPoolManager::new(10, disk_manager));
        let catalog = Catalog::new(bpm);

        let result = catalog.get_table("nonexistent");
        assert!(matches!(result, Err(QueryError::TableNotFound(_))));

        std::fs::remove_file("test_catalog2.db").unwrap();
    }
}
