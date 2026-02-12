//! Projection executor for SELECT column lists.
//!
//! Evaluates expressions and produces output tuples with only the projected columns.

use super::{BoxedExecutor, Executor};
use crate::expression::Expression;
use crate::types::Value;
use crate::{QueryError, Result};
use storage_engine::tuple::{Column, Schema, Tuple, Type, Value as StorageValue};

/// Projection executor that evaluates expressions to produce output columns.
pub struct ProjectionExecutor {
    child: BoxedExecutor,
    projections: Vec<Expression>,
    output_schema: Schema,
}

impl ProjectionExecutor {
    /// Creates a new projection executor.
    ///
    /// # Arguments
    /// * `child` - The child executor to pull tuples from
    /// * `projections` - List of expressions to evaluate for each output column
    /// * `output_column_names` - Names for the output columns
    pub fn new(
        child: BoxedExecutor,
        projections: Vec<Expression>,
        output_column_names: Vec<String>,
    ) -> Result<Self> {
        if projections.len() != output_column_names.len() {
            return Err(QueryError::ExecutionError(
                "Number of projections must match number of column names".to_string(),
            ));
        }

        // Build output schema
        // For now, assume all projected columns are integers
        // TODO: Infer types from expressions
        let columns = output_column_names
            .into_iter()
            .map(|name| Column {
                name,
                column_type: Type::Integer,
                length: 4, // Size of integer
            })
            .collect();

        let output_schema = Schema { columns };

        Ok(Self {
            child,
            projections,
            output_schema,
        })
    }
}

impl Executor for ProjectionExecutor {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn init(&mut self) -> Result<()> {
        self.child.init()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        match self.child.next()? {
            None => Ok(None),
            Some(tuple) => {
                // Evaluate each projection expression
                let mut output_values = Vec::new();
                for expr in &self.projections {
                    let value = expr.evaluate(&tuple)?;

                    // Convert back to storage Value
                    // Skip NULL values for now (storage engine doesn't support them yet)
                    if let Some(storage_val) = value.to_storage() {
                        output_values.push(storage_val);
                    } else {
                        // For NULL, use a placeholder (0 for integers)
                        output_values.push(StorageValue::Integer(0));
                    }
                }

                Ok(Some(Tuple {
                    values: output_values,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableInfo;
    use crate::executor::SeqScanExecutor;
    use crate::expression::{col, lit, Expression};
    use buffer_pool_manager::actor::ActorBufferPoolManager;
    use buffer_pool_manager::disk_manager::DiskManager;
    use storage_engine::table::TableHeap;
    use storage_engine::tuple::Value as StorageValue;
    use std::sync::Arc;

    #[test]
    fn test_projection_executor() {
        let disk_manager = Arc::new(DiskManager::new("test_projection.db", false).unwrap());
        let bpm = Arc::new(ActorBufferPoolManager::new(10, disk_manager));

        let schema = Schema {
            columns: vec![
                Column::new("a".to_string(), Type::Integer),
                Column::new("b".to_string(), Type::Integer),
                Column::new("c".to_string(), Type::Integer),
            ],
        };

        let table_heap = Arc::new(TableHeap::new(bpm.clone(), schema.clone()));

        // Insert test data
        table_heap.insert_tuple(&Tuple {
            values: vec![
                StorageValue::Integer(1),
                StorageValue::Integer(10),
                StorageValue::Integer(100),
            ],
        });
        table_heap.insert_tuple(&Tuple {
            values: vec![
                StorageValue::Integer(2),
                StorageValue::Integer(20),
                StorageValue::Integer(200),
            ],
        });

        let table_info = Arc::new(TableInfo::new(1, "test".to_string(), schema.clone(), table_heap));

        // Create executor: SELECT b, a + c FROM test
        let scan = Box::new(SeqScanExecutor::new(table_info.clone()));

        let projections = vec![
            col("b").bind(&schema).unwrap(),
            col("a").add(col("c")).bind(&schema).unwrap(),
        ];

        let mut projection = ProjectionExecutor::new(
            scan,
            projections,
            vec!["b".to_string(), "sum".to_string()],
        )
        .unwrap();

        projection.init().unwrap();

        // Collect results
        let mut results = Vec::new();
        while let Some(tuple) = projection.next().unwrap() {
            results.push(tuple);
        }

        // Check results
        assert_eq!(results.len(), 2);
        // First tuple: b=10, a+c=1+100=101
        assert_eq!(results[0].values[0], StorageValue::Integer(10));
        assert_eq!(results[0].values[1], StorageValue::Integer(101));
        // Second tuple: b=20, a+c=2+200=202
        assert_eq!(results[1].values[0], StorageValue::Integer(20));
        assert_eq!(results[1].values[1], StorageValue::Integer(202));

        std::fs::remove_file("test_projection.db").unwrap();
    }
}
