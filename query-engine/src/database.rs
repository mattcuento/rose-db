//! Database struct - the main entry point for query execution.

use crate::catalog::Catalog;
use crate::dataframe::DataFrame;
use crate::{QueryError, Result};
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
        let disk_manager = Arc::new(DiskManager::new(path, false)?);
        let bpm = Arc::new(ActorBufferPoolManager::new(100, disk_manager));
        let catalog = Arc::new(Catalog::new(bpm));

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_engine::tuple::{Column, Type};

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
    }
}
