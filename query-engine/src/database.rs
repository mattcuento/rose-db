//! Database struct - the main entry point for query execution.

use crate::catalog::Catalog;
use crate::dataframe::DataFrame;
use crate::Result;
use buffer_pool_manager::api::BufferPoolManager;
use buffer_pool_manager::concurrent::ConcurrentBufferPoolManager;
use buffer_pool_manager::disk_manager::DiskManager;
use std::path::Path;
use std::sync::Arc;
use storage_engine::tuple::Schema;

/// The main database interface.
///
/// Provides methods to create tables, execute queries, and manage the database.
pub struct Database {
    catalog: Arc<Catalog>,
}

impl Database {
    /// Opens or creates a database at the specified directory path.
    ///
    /// The directory is created if it does not exist. Inside it, the catalog is
    /// stored as `catalog` and each table gets its own subdirectory with segment files.
    pub fn open(path: &str) -> Result<Self> {
        let db_dir = Path::new(path);
        std::fs::create_dir_all(db_dir)?;

        let disk_manager = Arc::new(DiskManager::new(db_dir, false)?);
        let bpm: Arc<dyn BufferPoolManager> =
            Arc::new(ConcurrentBufferPoolManager::new(1000, disk_manager.clone()));
        let catalog_path = db_dir.join("catalog").to_string_lossy().into_owned();
        let catalog = Arc::new(Catalog::open(bpm, disk_manager, catalog_path)?);

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
    pub fn flush(&self) -> Result<()> {
        self.catalog.flush_all_pages()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_engine::tuple::{Tuple, Value};

    #[test]
    fn test_database_create_and_list_tables() {
        let path = "test_database_dir";
        let _ = std::fs::remove_dir_all(path);
        let db = Database::open(path).unwrap();

        let schema = crate::Schema {
            columns: vec![
                crate::int_column("id"),
                crate::varchar_column("name", 50),
            ],
        };

        db.create_table("users", schema).unwrap();

        let tables = db.list_tables();
        assert!(tables.contains(&"users".to_string()));

        db.drop_table("users").unwrap();

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_catalog_persists_across_reopen() {
        let path = "test_persistence_dir";
        let _ = std::fs::remove_dir_all(path);

        let schema = crate::Schema {
            columns: vec![
                crate::int_column("id"),
                crate::varchar_column("name", 50),
            ],
        };

        // Phase 1: create table, insert a row, close database
        {
            let db = Database::open(path).unwrap();
            db.create_table("users", schema).unwrap();
            let table_info = db.catalog.get_table("users").unwrap();
            table_info.table_heap.insert_tuple(&Tuple {
                values: vec![Value::Integer(42), Value::Varchar("Alice".to_string())],
            });
            db.flush().unwrap();
        }

        // Phase 2: reopen and assert table + data survive
        {
            let db = Database::open(path).unwrap();

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

        std::fs::remove_dir_all(path).unwrap();
    }
}
