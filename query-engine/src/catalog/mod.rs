//! Catalog for storing and managing table metadata.
//!
//! The catalog keeps track of all tables, their schemas, and associated TableHeap instances.
//! Metadata is persisted to a sidecar file (`<db_path>.catalog`) using binary serialization.

use crate::{QueryError, Result};
use buffer_pool_manager::api::BufferPoolManager;
use storage_engine::table::TableHeap;
use storage_engine::tuple::{Column, Schema, Type};
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
/// Stores metadata about all tables and persists it to a binary file on every DDL change.
/// Uses RwLock for concurrent reads (queries) and exclusive writes (DDL).
pub struct Catalog {
    tables: RwLock<HashMap<String, Arc<TableInfo>>>,
    next_table_id: RwLock<u32>,
    bpm: Arc<dyn BufferPoolManager>,
    catalog_path: String,
}

impl Catalog {
    /// Opens (or creates) the catalog at the given path.
    ///
    /// If the catalog file exists its contents are deserialized and each table's
    /// `TableHeap` is reconstructed via `TableHeap::open` so that no new pages are allocated.
    /// If the file does not exist the catalog starts empty (fresh database).
    pub fn open(bpm: Arc<dyn BufferPoolManager>, catalog_path: String) -> Result<Self> {
        let (next_id, tables_map) = if std::path::Path::new(&catalog_path).exists() {
            let bytes = std::fs::read(&catalog_path)?;
            let (next_id, entries) = deserialize_catalog(&bytes);
            let mut map: HashMap<String, Arc<TableInfo>> = HashMap::new();
            for entry in entries {
                let table_heap = Arc::new(TableHeap::open(bpm.clone(), entry.schema.clone(), entry.first_page_id));
                let table_info = Arc::new(TableInfo::new(entry.table_id, entry.name.clone(), entry.schema, table_heap));
                map.insert(entry.name, table_info);
            }
            (next_id, map)
        } else {
            (1, HashMap::new())
        };

        Ok(Self {
            tables: RwLock::new(tables_map),
            next_table_id: RwLock::new(next_id),
            bpm,
            catalog_path,
        })
    }

    /// Creates a new table in the catalog.
    pub fn create_table(&self, name: String, schema: Schema) -> Result<Arc<TableInfo>> {
        let mut tables = self.tables.write().unwrap();

        if tables.contains_key(&name) {
            return Err(QueryError::ExecutionError(format!(
                "Table '{}' already exists",
                name
            )));
        }

        let mut next_id = self.next_table_id.write().unwrap();
        let table_id = *next_id;
        *next_id += 1;
        drop(next_id);

        let table_heap = Arc::new(TableHeap::new(self.bpm.clone(), schema.clone()));
        let table_info = Arc::new(TableInfo::new(table_id, name.clone(), schema, table_heap));
        tables.insert(name, table_info.clone());

        self.save_locked(&tables)?;
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
        self.save_locked(&tables)?;
        Ok(())
    }

    /// Flushes all dirty pages to disk.
    pub fn flush_all_pages(&self) -> Result<()> {
        self.bpm.flush_all_pages()?;
        Ok(())
    }

    /// Atomically writes the catalog to disk.
    ///
    /// Writes to a `.tmp` file first, then renames — on POSIX systems, rename is atomic so
    /// the on-disk catalog is never partially replaced.
    fn save_locked(&self, tables: &HashMap<String, Arc<TableInfo>>) -> Result<()> {
        let next_id = *self.next_table_id.read().unwrap();
        let bytes = serialize_catalog(next_id, tables);
        let tmp_path = format!("{}.tmp", self.catalog_path);
        std::fs::write(&tmp_path, &bytes)?;
        std::fs::rename(&tmp_path, &self.catalog_path)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Binary serialization
// ---------------------------------------------------------------------------
//
// Format:
//   [4] next_table_id (u32)
//   [4] entry_count   (u32)
//   For each entry:
//     [4]          table_id
//     [4]          name_len
//     [name_len]   name (UTF-8)
//     [4]          first_page_id
//     [4]          column_count
//     For each column:
//       [4]          col_name_len
//       [col_name_len] col_name (UTF-8)
//       [1]          type_tag  (0 = Integer, 1 = Varchar)
//       [4]          length

fn serialize_catalog(next_id: u32, tables: &HashMap<String, Arc<TableInfo>>) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.extend_from_slice(&next_id.to_ne_bytes());
    buf.extend_from_slice(&(tables.len() as u32).to_ne_bytes());

    for info in tables.values() {
        buf.extend_from_slice(&info.table_id.to_ne_bytes());

        let name_bytes = info.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u32).to_ne_bytes());
        buf.extend_from_slice(name_bytes);

        buf.extend_from_slice(&(info.table_heap.first_page_id() as u32).to_ne_bytes());

        buf.extend_from_slice(&(info.schema.columns.len() as u32).to_ne_bytes());
        for col in &info.schema.columns {
            let col_name_bytes = col.name.as_bytes();
            buf.extend_from_slice(&(col_name_bytes.len() as u32).to_ne_bytes());
            buf.extend_from_slice(col_name_bytes);

            let type_tag: u8 = match col.column_type {
                Type::Integer => 0,
                Type::Varchar => 1,
            };
            buf.push(type_tag);
            buf.extend_from_slice(&col.length.to_ne_bytes());
        }
    }

    buf
}

struct RestoredEntry {
    table_id: u32,
    name: String,
    schema: Schema,
    first_page_id: usize,
}

fn deserialize_catalog(bytes: &[u8]) -> (u32, Vec<RestoredEntry>) {
    let mut offset = 0;

    let next_id = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;

    let entry_count = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut entries = Vec::with_capacity(entry_count);

    for _ in 0..entry_count {
        let table_id = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let name_len = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let name = String::from_utf8(bytes[offset..offset + name_len].to_vec()).unwrap();
        offset += name_len;

        let first_page_id = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let column_count = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let col_name_len = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let col_name = String::from_utf8(bytes[offset..offset + col_name_len].to_vec()).unwrap();
            offset += col_name_len;

            let type_tag = bytes[offset];
            offset += 1;
            let length = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;

            let column_type = match type_tag {
                0 => Type::Integer,
                1 => Type::Varchar,
                _ => panic!("Unknown type tag: {}", type_tag),
            };
            columns.push(Column { name: col_name, column_type, length });
        }

        entries.push(RestoredEntry {
            table_id,
            name,
            schema: Schema { columns },
            first_page_id,
        });
    }

    (next_id, entries)
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
        let catalog = Catalog::open(bpm, "test_catalog.catalog".to_string()).unwrap();

        let schema = Schema {
            columns: vec![
                crate::int_column("id"),
                crate::varchar_column("name", 50),
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
        std::fs::remove_file("test_catalog.catalog").unwrap();
    }

    #[test]
    fn test_catalog_table_not_found() {
        let disk_manager = Arc::new(DiskManager::new("test_catalog2.db", false).unwrap());
        let bpm = Arc::new(ActorBufferPoolManager::new(10, disk_manager));
        let catalog = Catalog::open(bpm, "test_catalog2.catalog".to_string()).unwrap();

        let result = catalog.get_table("nonexistent");
        assert!(matches!(result, Err(QueryError::TableNotFound(_))));

        std::fs::remove_file("test_catalog2.db").unwrap();
        let _ = std::fs::remove_file("test_catalog2.catalog");
    }
}
