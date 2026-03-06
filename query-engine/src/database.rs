//! Database struct - the main entry point for query execution.

use crate::catalog::Catalog;
use crate::dataframe::DataFrame;
use crate::Result;
use buffer_pool_manager::actor::ActorBufferPoolManager;
use buffer_pool_manager::disk_manager::DiskManager;
use std::sync::Arc;
use storage_engine::tuple::Schema;

/// The main database interface.
///
/// Provides methods to create tables, execute queries, and manage the database.
pub struct Database {
    catalog: Arc<Catalog>,
}

impl Database {
    /// Opens or creates a database at the specified path.
    pub fn open(path: &str) -> Result<Self> {
        let catalog_path = format!("{}.catalog", path);
        let disk_manager = Arc::new(DiskManager::new(path, false)?);
        let bpm = Arc::new(ActorBufferPoolManager::new(100, disk_manager));
        let catalog = Arc::new(Catalog::open(bpm, catalog_path)?);

        Ok(Self { catalog })
    }

    /// Creates a new table in the database.
    pub fn create_table(&self, name: &str, schema: Schema) -> Result<()> {
        self.catalog.create_table(name.to_string(), schema)?;
        Ok(())
    }

    /// Returns a DataFrame for querying the specified table.
    pub fn table(&self, name: &str) -> Result<DataFrame> {
        let table_info = self.catalog.get_table(name)?;
        Ok(DataFrame::new(table_info, None, None, None))
    }

    /// Lists all tables in the database.
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog.list_tables()
    }

    /// Drops a table from the database.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        self.catalog.drop_table(name)
    }

    /// Flushes all dirty pages to disk.
    ///
    /// This is useful in tests to ensure all writes are persisted before reading.
    pub fn flush(&self) -> Result<()> {
        self.catalog.flush_all_pages()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_engine::tuple::{Column, Type, Tuple, Value};

    #[test]
    fn test_database_create_and_list_tables() {
        let db = Database::open("test_database.db").unwrap();

        let schema = Schema {
            columns: vec![
                crate::int_column("id"),
                crate::varchar_column("name", 50),
            ],
        };

        // Create table
        db.create_table("users", schema).unwrap();

        // List tables
        let tables = db.list_tables();
        assert!(tables.contains(&"users".to_string()));

        // Drop table
        db.drop_table("users").unwrap();

        std::fs::remove_file("test_database.db").unwrap();
        let _ = std::fs::remove_file("test_database.db.catalog");
    }

    #[test]
    fn test_catalog_persists_across_reopen() {
        let db_path = "test_persistence.db";
        let catalog_path = "test_persistence.db.catalog";

        // Clean up any leftover files from previous runs
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(catalog_path);

        let schema = Schema {
            columns: vec![
                crate::int_column("id"),
                crate::varchar_column("name", 50),
            ],
        };

        // Phase 1: create table, insert a row, close database
        {
            let db = Database::open(db_path).unwrap();
            db.create_table("users", schema).unwrap();
            let table_info = db.catalog.get_table("users").unwrap();
            table_info.table_heap.insert_tuple(&Tuple {
                values: vec![Value::Integer(42), Value::Varchar("Alice".to_string())],
            });
            db.flush().unwrap();
        }

        // Phase 2: reopen and assert table + data survive
        {
            let db = Database::open(db_path).unwrap();

            let tables = db.list_tables();
            assert!(tables.contains(&"users".to_string()), "table should persist after reopen");

            let table_info = db.catalog.get_table("users").unwrap();
            let tuple = table_info.table_heap.get_tuple(storage_engine::table::RowId {
                page_id: table_info.table_heap.first_page_id(),
                slot_index: 0,
            });
            assert!(tuple.is_some(), "row should persist after reopen");
            let tuple = tuple.unwrap();
            assert_eq!(tuple.values[0], Value::Integer(42));
            assert_eq!(tuple.values[1], Value::Varchar("Alice".to_string()));
        }

        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(catalog_path);
    }
}
