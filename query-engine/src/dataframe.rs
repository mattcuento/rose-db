//! DataFrame API for building queries programmatically.
//!
//! Provides a fluent, method-chaining interface inspired by Polars and DataFusion.

use crate::catalog::TableInfo;
use crate::executor::{
    BoxedExecutor, Executor, FilterExecutor, LimitExecutor, ProjectionExecutor, SeqScanExecutor,
};
use crate::expression::{col, Expression};
use crate::types::Value;
use crate::{QueryError, Result};
use std::sync::Arc;
use storage_engine::table::RowId;
use storage_engine::tuple::Tuple;

/// A lazy query builder that produces an execution plan.
///
/// Methods can be chained to build complex queries:
/// ```no_run
/// df.filter(col("age").gt(25))
///   .select(&["name", "email"])
///   .limit(10)
///   .collect()
/// ```
pub struct DataFrame {
    table_info: Arc<TableInfo>,
    filter_expr: Option<Expression>,
    projection_exprs: Option<Vec<(Expression, String)>>, // (expr, output_name)
    limit: Option<usize>,
}

impl DataFrame {
    /// Creates a new DataFrame from a table.
    pub(crate) fn new(
        table_info: Arc<TableInfo>,
        filter_expr: Option<Expression>,
        projection_exprs: Option<Vec<(Expression, String)>>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            table_info,
            filter_expr,
            projection_exprs,
            limit,
        }
    }

    /// Adds a filter (WHERE clause) to the query.
    ///
    /// # Example
    /// ```no_run
    /// df.filter(col("age").gt(25))
    /// ```
    pub fn filter(mut self, predicate: Expression) -> Self {
        self.filter_expr = Some(predicate);
        self
    }

    /// Projects specific columns (SELECT clause).
    ///
    /// # Example
    /// ```no_run
    /// df.select(&["name", "email"])
    /// ```
    pub fn select(mut self, columns: &[&str]) -> Self {
        let exprs = columns
            .iter()
            .map(|col_name| (col(col_name), col_name.to_string()))
            .collect();
        self.projection_exprs = Some(exprs);
        self
    }

    /// Projects with custom expressions.
    ///
    /// # Example
    /// ```no_run
    /// df.select_exprs(&[
    ///     (col("name"), "name"),
    ///     (col("age").add(lit(1)), "age_plus_one"),
    /// ])
    /// ```
    pub fn select_exprs(mut self, exprs: &[(Expression, &str)]) -> Self {
        let exprs = exprs
            .iter()
            .map(|(expr, name)| (expr.clone(), name.to_string()))
            .collect();
        self.projection_exprs = Some(exprs);
        self
    }

    /// Limits the number of results (LIMIT clause).
    ///
    /// # Example
    /// ```no_run
    /// df.limit(10)
    /// ```
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Inserts a tuple into the table.
    ///
    /// # Example
    /// ```no_run
    /// df.insert(&[Value::Integer(1), Value::Varchar("Alice".to_string())])
    /// ```
    pub fn insert(&self, values: &[Value]) -> Result<RowId> {
        // Convert query_engine Values to storage Values
        let storage_values: Vec<_> = values
            .iter()
            .map(|v| {
                v.to_storage().ok_or_else(|| {
                    QueryError::ExecutionError("Cannot insert NULL values (not supported yet)".to_string())
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let tuple = Tuple {
            values: storage_values,
        };

        self.table_info
            .table_heap
            .insert_tuple(&tuple)
            .ok_or_else(|| QueryError::ExecutionError("Failed to insert tuple".to_string()))
    }

    /// Builds the executor tree and executes the query, collecting all results.
    ///
    /// This is the terminal operation that actually runs the query.
    pub fn collect(self) -> Result<Vec<Tuple>> {
        let mut executor = self.build_executor()?;
        executor.init()?;

        let mut results = Vec::new();
        while let Some(tuple) = executor.next()? {
            results.push(tuple);
        }

        Ok(results)
    }

    /// Builds the executor tree for this DataFrame.
    fn build_executor(&self) -> Result<BoxedExecutor> {
        // Start with sequential scan
        let mut executor: BoxedExecutor = Box::new(SeqScanExecutor::new(self.table_info.clone()));

        // Apply filter if present
        if let Some(ref filter_expr) = self.filter_expr {
            // Bind the expression to the current schema
            let bound_expr = filter_expr.bind(&self.table_info.schema)?;
            executor = Box::new(FilterExecutor::new(executor, bound_expr));
        }

        // Apply projection if present
        if let Some(ref proj_exprs) = self.projection_exprs {
            let (exprs, names): (Vec<_>, Vec<_>) = proj_exprs
                .iter()
                .map(|(expr, name)| {
                    (
                        expr.bind(&self.table_info.schema).unwrap(),
                        name.clone(),
                    )
                })
                .unzip();

            executor = Box::new(ProjectionExecutor::new(executor, exprs, names)?);
        }

        // Apply limit if present
        if let Some(limit_val) = self.limit {
            executor = Box::new(LimitExecutor::new(executor, limit_val));
        }

        Ok(executor)
    }

    /// Executes the query and prints results (for debugging/demo).
    pub fn show(self) -> Result<()> {
        let results = self.collect()?;

        println!("Results: {} rows", results.len());
        for (i, tuple) in results.iter().enumerate() {
            println!("{}: {:?}", i, tuple);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use storage_engine::tuple::Type;

    #[test]
    fn test_dataframe_insert_and_collect() {
        let db = Database::open("test_dataframe.db").unwrap();

        let schema = storage_engine::tuple::Schema {
            columns: vec![
                crate::int_column("id"),
                crate::int_column("age"),
            ],
        };

        db.create_table("users", schema).unwrap();

        let df = db.table("users").unwrap();

        // Insert data
        df.insert(&[
            crate::types::Value::Integer(1),
            crate::types::Value::Integer(25),
        ])
        .unwrap();
        df.insert(&[
            crate::types::Value::Integer(2),
            crate::types::Value::Integer(30),
        ])
        .unwrap();
        df.insert(&[
            crate::types::Value::Integer(3),
            crate::types::Value::Integer(20),
        ])
        .unwrap();

        // Query: SELECT * FROM users WHERE age > 22
        let results = db
            .table("users")
            .unwrap()
            .filter(col("age").gt(crate::expression::lit(22)))
            .collect()
            .unwrap();

        assert_eq!(results.len(), 2);

        std::fs::remove_file("test_dataframe.db").unwrap();
    }

    #[test]
    fn test_dataframe_select_and_limit() {
        let db = Database::open("test_dataframe2.db").unwrap();

        let schema = storage_engine::tuple::Schema {
            columns: vec![
                crate::int_column("a"),
                crate::int_column("b"),
                crate::int_column("c"),
            ],
        };

        db.create_table("test", schema).unwrap();

        let df = db.table("test").unwrap();

        // Insert data
        for i in 1..=5 {
            df.insert(&[
                crate::types::Value::Integer(i),
                crate::types::Value::Integer(i * 10),
                crate::types::Value::Integer(i * 100),
            ])
            .unwrap();
        }

        // Query: SELECT b FROM test LIMIT 3
        let results = db
            .table("test")
            .unwrap()
            .select(&["b"])
            .limit(3)
            .collect()
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].values.len(), 1); // Only column b
        assert_eq!(results[0].values[0], storage_engine::tuple::Value::Integer(10));

        std::fs::remove_file("test_dataframe2.db").unwrap();
    }
}
