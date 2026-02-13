//! Filter executor for WHERE clauses.
//!
//! Applies a predicate to tuples from a child executor, returning only matching tuples.

use super::{BoxedExecutor, Executor};
use crate::expression::Expression;
use crate::types::Value;
use crate::Result;
use storage_engine::tuple::{Schema, Tuple};

/// Filter executor that applies a predicate to tuples.
///
/// Returns only tuples for which the predicate evaluates to TRUE (non-zero integer).
pub struct FilterExecutor {
    child: BoxedExecutor,
    predicate: Expression,
}

impl FilterExecutor {
    /// Creates a new filter executor.
    pub fn new(child: BoxedExecutor, predicate: Expression) -> Self {
        Self { child, predicate }
    }
}

impl Executor for FilterExecutor {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn init(&mut self) -> Result<()> {
        self.child.init()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(tuple) => {
                    // Evaluate predicate
                    let result = self.predicate.evaluate(&tuple)?;

                    // Check if predicate is TRUE (non-zero integer, following SQL semantics)
                    let is_true = match result {
                        Value::Integer(i) if i != 0 => true,
                        _ => false, // NULL or 0 are both FALSE
                    };

                    if is_true {
                        return Ok(Some(tuple));
                    }
                    // Otherwise continue to next tuple
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableInfo;
    use crate::executor::SeqScanExecutor;
    use crate::expression::col;
    use buffer_pool_manager::actor::ActorBufferPoolManager;
    use buffer_pool_manager::disk_manager::DiskManager;
    use storage_engine::table::TableHeap;
    use storage_engine::tuple::{Column, Type, Value as StorageValue};
    use std::sync::Arc;

    #[test]
    fn test_filter_executor() {
        let disk_manager = Arc::new(DiskManager::new("test_filter.db", false).unwrap());
        let bpm = Arc::new(ActorBufferPoolManager::new(10, disk_manager));

        let schema = Schema {
            columns: vec![
                crate::int_column("id"),
                crate::int_column("age"),
            ],
        };

        let table_heap = Arc::new(TableHeap::new(bpm.clone(), schema.clone()));

        // Insert test data
        table_heap.insert_tuple(&Tuple {
            values: vec![StorageValue::Integer(1), StorageValue::Integer(25)],
        });
        table_heap.insert_tuple(&Tuple {
            values: vec![StorageValue::Integer(2), StorageValue::Integer(30)],
        });
        table_heap.insert_tuple(&Tuple {
            values: vec![StorageValue::Integer(3), StorageValue::Integer(20)],
        });

        let table_info = Arc::new(TableInfo::new(1, "test".to_string(), schema.clone(), table_heap));

        // Create executor: SELECT * FROM test WHERE age > 22
        let scan = Box::new(SeqScanExecutor::new(table_info.clone()));
        let predicate = col("age").gt(crate::expression::lit(22));
        let bound_predicate = predicate.bind(&schema).unwrap();

        let mut filter = FilterExecutor::new(scan, bound_predicate);
        filter.init().unwrap();

        // Collect results
        let mut results = Vec::new();
        while let Some(tuple) = filter.next().unwrap() {
            results.push(tuple);
        }

        // Should return tuples with age > 22 (id=1 with age=25, id=2 with age=30)
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].values[1], StorageValue::Integer(25));
        assert_eq!(results[1].values[1], StorageValue::Integer(30));

        std::fs::remove_file("test_filter.db").unwrap();
    }
}
