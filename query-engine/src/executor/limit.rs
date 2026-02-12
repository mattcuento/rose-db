//! Limit executor for LIMIT clauses.
//!
//! Returns only the first N tuples from the child executor.

use super::{BoxedExecutor, Executor};
use crate::Result;
use storage_engine::tuple::{Schema, Tuple};

/// Limit executor that returns at most N tuples.
pub struct LimitExecutor {
    child: BoxedExecutor,
    limit: usize,
    count: usize,
}

impl LimitExecutor {
    /// Creates a new limit executor.
    pub fn new(child: BoxedExecutor, limit: usize) -> Self {
        Self {
            child,
            limit,
            count: 0,
        }
    }
}

impl Executor for LimitExecutor {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn init(&mut self) -> Result<()> {
        self.count = 0;
        self.child.init()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.count >= self.limit {
            return Ok(None);
        }

        match self.child.next()? {
            None => Ok(None),
            Some(tuple) => {
                self.count += 1;
                Ok(Some(tuple))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableInfo;
    use crate::executor::SeqScanExecutor;
    use buffer_pool_manager::actor::ActorBufferPoolManager;
    use buffer_pool_manager::disk_manager::DiskManager;
    use storage_engine::table::TableHeap;
    use storage_engine::tuple::{Column, Schema, Type, Value};
    use std::sync::Arc;

    #[test]
    fn test_limit_executor() {
        let disk_manager = Arc::new(DiskManager::new("test_limit.db", false).unwrap());
        let bpm = Arc::new(ActorBufferPoolManager::new(10, disk_manager));

        let schema = Schema {
            columns: vec![Column::new("id".to_string(), Type::Integer)],
        };

        let table_heap = Arc::new(TableHeap::new(bpm.clone(), schema.clone()));

        // Insert 5 tuples
        for i in 1..=5 {
            table_heap.insert_tuple(&Tuple {
                values: vec![Value::Integer(i)],
            });
        }

        let table_info = Arc::new(TableInfo::new(1, "test".to_string(), schema, table_heap));

        // Create executor: SELECT * FROM test LIMIT 3
        let scan = Box::new(SeqScanExecutor::new(table_info));
        let mut limit = LimitExecutor::new(scan, 3);
        limit.init().unwrap();

        // Collect results
        let mut results = Vec::new();
        while let Some(tuple) = limit.next().unwrap() {
            results.push(tuple);
        }

        // Should return only first 3 tuples
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].values[0], Value::Integer(1));
        assert_eq!(results[1].values[0], Value::Integer(2));
        assert_eq!(results[2].values[0], Value::Integer(3));

        std::fs::remove_file("test_limit.db").unwrap();
    }
}
