//! Execution engine using the Volcano iterator model.
//!
//! Each executor implements the Executor trait and can be composed to form query plans.

use crate::{QueryError, Result};
use storage_engine::tuple::{Schema, Tuple};

pub mod seq_scan;
pub mod filter;
pub mod projection;
pub mod limit;

pub use seq_scan::SeqScanExecutor;
pub use filter::FilterExecutor;
pub use projection::ProjectionExecutor;
pub use limit::LimitExecutor;

/// The core executor trait for the Volcano iterator model.
///
/// Executors are pull-based: parents call next() on children to retrieve tuples.
pub trait Executor {
    /// Returns the schema of tuples produced by this executor.
    fn schema(&self) -> &Schema;

    /// Initialize the executor (acquire resources, position iterators, etc.).
    fn init(&mut self) -> Result<()>;

    /// Get the next tuple, or None if exhausted.
    fn next(&mut self) -> Result<Option<Tuple>>;

    /// Reset the executor to its initial state for re-execution.
    fn reset(&mut self) -> Result<()> {
        self.init()
    }
}

/// A boxed executor for dynamic dispatch.
pub type BoxedExecutor = Box<dyn Executor>;

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to collect all results from an executor
    pub fn collect_results(executor: &mut dyn Executor) -> Result<Vec<Tuple>> {
        let mut results = Vec::new();
        executor.init()?;
        while let Some(tuple) = executor.next()? {
            results.push(tuple);
        }
        Ok(results)
    }
}
