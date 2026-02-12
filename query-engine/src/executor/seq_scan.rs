//! Sequential scan executor.
//!
//! Iterates through all tuples in a table by scanning pages sequentially.

use super::Executor;
use crate::catalog::TableInfo;
use crate::{QueryError, Result};
use buffer_pool_manager::api::{PageId, INVALID_PAGE_ID};
use buffer_pool_manager::page::SlottedPage;
use std::ops::DerefMut;
use std::sync::Arc;
use storage_engine::tuple::{Schema, Tuple};

/// Sequential scan executor.
///
/// Scans all pages and slots in a table heap, returning tuples one at a time.
pub struct SeqScanExecutor {
    table_info: Arc<TableInfo>,
    current_page_id: PageId,
    current_slot: u16,
}

impl SeqScanExecutor {
    /// Creates a new sequential scan executor.
    pub fn new(table_info: Arc<TableInfo>) -> Self {
        Self {
            table_info,
            current_page_id: INVALID_PAGE_ID,
            current_slot: 0,
        }
    }
}

impl Executor for SeqScanExecutor {
    fn schema(&self) -> &Schema {
        &self.table_info.schema
    }

    fn init(&mut self) -> Result<()> {
        self.current_page_id = self.table_info.table_heap.first_page_id();
        self.current_slot = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        loop {
            if self.current_page_id == INVALID_PAGE_ID {
                return Ok(None);
            }

            // Fetch the current page
            let mut page_guard = self.table_info.table_heap.bpm().fetch_page(self.current_page_id)?;
            let slotted_page = SlottedPage::new(page_guard.deref_mut());

            let header = slotted_page.header();
            let slot_count = header.slot_count;

            // Try to get a tuple from the current slot
            while self.current_slot < slot_count {
                let slot = self.current_slot;
                self.current_slot += 1;

                // Get the record data
                let record = slotted_page.get_record(slot);

                // Skip empty slots (length 0)
                if record.is_empty() {
                    continue;
                }

                let tuple = Tuple::deserialize(record, &self.table_info.schema);
                return Ok(Some(tuple));
            }

            // Exhausted current page, move to next page
            self.current_page_id = header.next_page_id;
            self.current_slot = 0;
            // Page guard drops here, releasing the latch
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffer_pool_manager::actor::ActorBufferPoolManager;
    use buffer_pool_manager::disk_manager::DiskManager;
    use storage_engine::table::TableHeap;
    use storage_engine::tuple::{Column, Type, Value};

    #[test]
    fn test_seq_scan_empty_table() {
        let disk_manager = Arc::new(DiskManager::new("test_seq_scan_empty.db", false).unwrap());
        let bpm = Arc::new(ActorBufferPoolManager::new(10, disk_manager));

        let schema = Schema {
            columns: vec![Column::new("id".to_string(), Type::Integer)],
        };

        let table_heap = Arc::new(TableHeap::new(bpm.clone(), schema.clone()));
        let table_info = Arc::new(TableInfo::new(1, "test".to_string(), schema, table_heap));

        let mut executor = SeqScanExecutor::new(table_info);
        executor.init().unwrap();

        // Empty table should return None
        assert!(executor.next().unwrap().is_none());

        std::fs::remove_file("test_seq_scan_empty.db").unwrap();
    }

    #[test]
    fn test_seq_scan_with_data() {
        let disk_manager = Arc::new(DiskManager::new("test_seq_scan_data.db", false).unwrap());
        let bpm = Arc::new(ActorBufferPoolManager::new(10, disk_manager));

        let schema = Schema {
            columns: vec![
                Column::new("id".to_string(), Type::Integer),
                Column::new("name".to_string(), Type::Varchar(50)),
            ],
        };

        let table_heap = Arc::new(TableHeap::new(bpm.clone(), schema.clone()));

        // Insert some tuples
        table_heap.insert_tuple(&Tuple {
            values: vec![Value::Integer(1), Value::Varchar("Alice".to_string())],
        });
        table_heap.insert_tuple(&Tuple {
            values: vec![Value::Integer(2), Value::Varchar("Bob".to_string())],
        });
        table_heap.insert_tuple(&Tuple {
            values: vec![Value::Integer(3), Value::Varchar("Charlie".to_string())],
        });

        let table_info = Arc::new(TableInfo::new(1, "test".to_string(), schema, table_heap));

        let mut executor = SeqScanExecutor::new(table_info);
        executor.init().unwrap();

        // Collect all tuples
        let mut tuples = Vec::new();
        while let Some(tuple) = executor.next().unwrap() {
            tuples.push(tuple);
        }

        assert_eq!(tuples.len(), 3);
        assert_eq!(tuples[0].values[0], Value::Integer(1));
        assert_eq!(tuples[1].values[0], Value::Integer(2));
        assert_eq!(tuples[2].values[0], Value::Integer(3));

        std::fs::remove_file("test_seq_scan_data.db").unwrap();
    }
}
