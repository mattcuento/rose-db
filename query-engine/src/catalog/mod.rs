//! Catalog for storing and managing table metadata.
//!
//! The catalog keeps track of all tables, their schemas, and associated TableHeap instances.
//! Metadata is persisted to a binary file (`<db_dir>/catalog`) using binary serialization.

use crate::{QueryError, Result};
use buffer_pool_manager::api::{make_page_id, BufferPoolManager};
use buffer_pool_manager::disk_manager::DiskManager;
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
        Self { table_id, name, schema, table_heap }
    }
}

/// The database catalog.
///
/// Stores metadata about all tables and persists it to a binary file on every DDL change.
/// Uses RwLock for concurrent reads (queries) and exclusive writes (DDL).
///
/// A single global `BufferPoolManager` and `DiskManager` are shared across all tables.
/// The disk manager routes page I/O to per-table segment files using composite page IDs.
pub struct Catalog {
    tables: RwLock<HashMap<String, Arc<TableInfo>>>,
    next_table_id: RwLock<u32>,
    bpm: Arc<dyn BufferPoolManager>,
    disk_manager: Arc<DiskManager>,
    catalog_path: String,
}

impl Catalog {
    /// Opens (or creates) the catalog.
    ///
    /// If the catalog file exists, deserializes it and reconstructs each table's `TableHeap`
    /// by registering it with the disk manager and opening the existing segment files.
    /// If the file does not exist, starts empty (fresh database).
    pub fn open(
        bpm: Arc<dyn BufferPoolManager>,
        disk_manager: Arc<DiskManager>,
        catalog_path: String,
    ) -> Result<Self> {
        let (next_id, tables_map) = if std::path::Path::new(&catalog_path).exists() {
            let bytes = std::fs::read(&catalog_path)?;
            let (next_id, entries) = deserialize_catalog(&bytes);
            let mut map: HashMap<String, Arc<TableInfo>> = HashMap::new();
            for entry in entries {
                disk_manager.register_table(entry.table_id, &entry.name)?;
                // Each table's first page is always local page 1 (local page 0 is reserved).
                let first_page_id = make_page_id(entry.table_id, 1);
                let table_heap = Arc::new(TableHeap::open(
                    bpm.clone(),
                    entry.schema.clone(),
                    first_page_id,
                    entry.table_id,
                ));
                let table_info = Arc::new(TableInfo::new(
                    entry.table_id,
                    entry.name.clone(),
                    entry.schema,
                    table_heap,
                ));
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
            disk_manager,
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

        self.disk_manager.register_table(table_id, &name)?;
        let table_heap = Arc::new(TableHeap::new(self.bpm.clone(), schema.clone(), table_id));
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

    /// Drops a table from the catalog and removes its data directory.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut tables = self.tables.write().unwrap();
        let table_info = tables
            .remove(name)
            .ok_or_else(|| QueryError::TableNotFound(name.to_string()))?;
        self.disk_manager.drop_table(table_info.table_id)?;
        self.save_locked(&tables)?;
        Ok(())
    }

    /// Flushes all dirty pages in the global buffer pool to disk.
    pub fn flush_all_pages(&self) -> Result<()> {
        self.bpm.flush_all_pages()?;
        Ok(())
    }

    /// Atomically writes the catalog to disk.
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
// Format (first_page_id removed — always derived as make_page_id(table_id, 1)):
//   [4] next_table_id (u32)
//   [4] entry_count   (u32)
//   For each entry:
//     [4]          table_id
//     [4]          name_len
//     [name_len]   name (UTF-8)
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

        entries.push(RestoredEntry { table_id, name, schema: Schema { columns } });
    }

    (next_id, entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffer_pool_manager::concurrent::ConcurrentBufferPoolManager;
    use storage_engine::tuple::{Column, Type};
    use std::path::Path;

    fn make_catalog(dir: &str) -> (Arc<DiskManager>, Catalog) {
        let path = Path::new(dir);
        std::fs::create_dir_all(path).unwrap();
        let disk_manager = Arc::new(DiskManager::new(path, false).unwrap());
        let bpm: Arc<dyn BufferPoolManager> =
            Arc::new(ConcurrentBufferPoolManager::new(10, disk_manager.clone()));
        let catalog_path = path.join("catalog").to_string_lossy().into_owned();
        let catalog = Catalog::open(bpm, disk_manager.clone(), catalog_path).unwrap();
        (disk_manager, catalog)
    }

    #[test]
    fn test_catalog_create_and_get_table() {
        let dir = "test_catalog_dir";
        let (_dm, catalog) = make_catalog(dir);

        let schema = Schema {
            columns: vec![
                crate::int_column("id"),
                crate::varchar_column("name", 50),
            ],
        };

        let table_info = catalog.create_table("users".to_string(), schema.clone()).unwrap();
        assert_eq!(table_info.name, "users");
        assert_eq!(table_info.table_id, 1);

        let retrieved = catalog.get_table("users").unwrap();
        assert_eq!(retrieved.name, "users");

        let tables = catalog.list_tables();
        assert_eq!(tables, vec!["users"]);

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_catalog_table_not_found() {
        let dir = "test_catalog2_dir";
        let (_dm, catalog) = make_catalog(dir);

        let result = catalog.get_table("nonexistent");
        assert!(matches!(result, Err(QueryError::TableNotFound(_))));

        std::fs::remove_dir_all(dir).unwrap();
    }
}
