//! Expression system for building query predicates and projections.
//!
//! Provides a fluent API for building expressions like `col("id").eq(42)`.

use crate::types::Value;
use crate::{QueryError, Result};
use storage_engine::tuple::{Schema, Tuple};
use std::cmp::Ordering;

/// An expression that can be evaluated against a tuple.
#[derive(Debug, Clone)]
pub enum Expression {
    /// Reference to a column by name
    Column(String),
    /// Reference to a column by index (after binding)
    BoundColumn(usize),
    /// Literal value
    Literal(Value),
    /// Binary operation (e.g., a + b, a > b)
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// Unary operation (e.g., NOT, IS NULL)
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    // Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    // Logical
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    IsNull,
    IsNotNull,
}

impl Expression {
    /// Binds column names to column indices based on a schema.
    pub fn bind(&self, schema: &Schema) -> Result<Expression> {
        match self {
            Expression::Column(name) => {
                let index = schema
                    .columns
                    .iter()
                    .position(|col| &col.name == name)
                    .ok_or_else(|| QueryError::ColumnNotFound(name.clone()))?;
                Ok(Expression::BoundColumn(index))
            }
            Expression::BoundColumn(_) => Ok(self.clone()),
            Expression::Literal(_) => Ok(self.clone()),
            Expression::BinaryOp { left, op, right } => Ok(Expression::BinaryOp {
                left: Box::new(left.bind(schema)?),
                op: *op,
                right: Box::new(right.bind(schema)?),
            }),
            Expression::UnaryOp { op, expr } => Ok(Expression::UnaryOp {
                op: *op,
                expr: Box::new(expr.bind(schema)?),
            }),
        }
    }

    /// Evaluates the expression against a tuple.
    pub fn evaluate(&self, tuple: &Tuple) -> Result<Value> {
        match self {
            Expression::Column(name) => Err(QueryError::ExecutionError(format!(
                "Unbound column: {}. Call bind() first.",
                name
            ))),
            Expression::BoundColumn(index) => {
                if *index >= tuple.values.len() {
                    return Err(QueryError::ExecutionError(format!(
                        "Column index {} out of bounds",
                        index
                    )));
                }
                Ok(Value::from_storage(tuple.values[*index].clone()))
            }
            Expression::Literal(val) => Ok(val.clone()),
            Expression::BinaryOp { left, op, right } => {
                let left_val = left.evaluate(tuple)?;
                let right_val = right.evaluate(tuple)?;
                self.evaluate_binary_op(&left_val, *op, &right_val)
            }
            Expression::UnaryOp { op, expr } => {
                let val = expr.evaluate(tuple)?;
                self.evaluate_unary_op(*op, &val)
            }
        }
    }

    fn evaluate_binary_op(
        &self,
        left: &Value,
        op: BinaryOperator,
        right: &Value,
    ) -> Result<Value> {
        use BinaryOperator::*;
        match op {
            Add => left.add(right).ok_or_else(|| {
                QueryError::TypeMismatch(format!("Cannot add {:?} and {:?}", left, right))
            }),
            Subtract => left.subtract(right).ok_or_else(|| {
                QueryError::TypeMismatch(format!("Cannot subtract {:?} and {:?}", left, right))
            }),
            Multiply => left.multiply(right).ok_or_else(|| {
                QueryError::TypeMismatch(format!("Cannot multiply {:?} and {:?}", left, right))
            }),
            Divide => left.divide(right).ok_or_else(|| {
                QueryError::TypeMismatch(format!("Cannot divide {:?} by {:?}", left, right))
            }),
            Eq => match left.compare(right) {
                Some(Ordering::Equal) => Ok(Value::Integer(1)), // TRUE
                Some(_) => Ok(Value::Integer(0)),               // FALSE
                None => Ok(Value::Null),                        // NULL
            },
            NotEq => match left.compare(right) {
                Some(Ordering::Equal) => Ok(Value::Integer(0)), // FALSE
                Some(_) => Ok(Value::Integer(1)),               // TRUE
                None => Ok(Value::Null),                        // NULL
            },
            Lt => match left.compare(right) {
                Some(Ordering::Less) => Ok(Value::Integer(1)),
                Some(_) => Ok(Value::Integer(0)),
                None => Ok(Value::Null),
            },
            LtEq => match left.compare(right) {
                Some(Ordering::Less | Ordering::Equal) => Ok(Value::Integer(1)),
                Some(_) => Ok(Value::Integer(0)),
                None => Ok(Value::Null),
            },
            Gt => match left.compare(right) {
                Some(Ordering::Greater) => Ok(Value::Integer(1)),
                Some(_) => Ok(Value::Integer(0)),
                None => Ok(Value::Null),
            },
            GtEq => match left.compare(right) {
                Some(Ordering::Greater | Ordering::Equal) => Ok(Value::Integer(1)),
                Some(_) => Ok(Value::Integer(0)),
                None => Ok(Value::Null),
            },
            And => {
                // SQL AND logic: 1 AND 1 = 1, 0 AND x = 0, NULL AND 1 = NULL
                match (left, right) {
                    (Value::Integer(0), _) | (_, Value::Integer(0)) => Ok(Value::Integer(0)),
                    (Value::Integer(1), Value::Integer(1)) => Ok(Value::Integer(1)),
                    _ => Ok(Value::Null),
                }
            }
            Or => {
                // SQL OR logic: 1 OR x = 1, 0 OR 0 = 0, NULL OR 0 = NULL
                match (left, right) {
                    (Value::Integer(1), _) | (_, Value::Integer(1)) => Ok(Value::Integer(1)),
                    (Value::Integer(0), Value::Integer(0)) => Ok(Value::Integer(0)),
                    _ => Ok(Value::Null),
                }
            }
        }
    }

    fn evaluate_unary_op(&self, op: UnaryOperator, val: &Value) -> Result<Value> {
        match op {
            UnaryOperator::Not => match val {
                Value::Integer(0) => Ok(Value::Integer(1)),
                Value::Integer(_) => Ok(Value::Integer(0)),
                Value::Null => Ok(Value::Null),
                _ => Err(QueryError::TypeMismatch(format!(
                    "Cannot apply NOT to {:?}",
                    val
                ))),
            },
            UnaryOperator::IsNull => Ok(Value::Integer(if val.is_null() { 1 } else { 0 })),
            UnaryOperator::IsNotNull => Ok(Value::Integer(if val.is_null() { 0 } else { 1 })),
        }
    }

    // ===== Builder Methods for Fluent API =====

    /// Creates an equality comparison: `self == other`
    pub fn eq(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::Eq,
            right: Box::new(other),
        }
    }

    /// Creates an inequality comparison: `self != other`
    pub fn not_eq(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::NotEq,
            right: Box::new(other),
        }
    }

    /// Creates a less-than comparison: `self < other`
    pub fn lt(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::Lt,
            right: Box::new(other),
        }
    }

    /// Creates a less-than-or-equal comparison: `self <= other`
    pub fn lt_eq(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::LtEq,
            right: Box::new(other),
        }
    }

    /// Creates a greater-than comparison: `self > other`
    pub fn gt(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::Gt,
            right: Box::new(other),
        }
    }

    /// Creates a greater-than-or-equal comparison: `self >= other`
    pub fn gt_eq(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::GtEq,
            right: Box::new(other),
        }
    }

    /// Creates an AND logical operation: `self AND other`
    pub fn and(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::And,
            right: Box::new(other),
        }
    }

    /// Creates an OR logical operation: `self OR other`
    pub fn or(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::Or,
            right: Box::new(other),
        }
    }

    /// Creates an addition operation: `self + other`
    pub fn add(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::Add,
            right: Box::new(other),
        }
    }

    /// Checks if the value is NULL
    pub fn is_null(self) -> Expression {
        Expression::UnaryOp {
            op: UnaryOperator::IsNull,
            expr: Box::new(self),
        }
    }
}

// ===== Helper Functions for Building Expressions =====

/// Creates a column reference expression.
pub fn col(name: &str) -> Expression {
    Expression::Column(name.to_string())
}

/// Creates a literal integer expression.
pub fn lit(value: i32) -> Expression {
    Expression::Literal(Value::Integer(value))
}

/// Creates a literal string expression.
pub fn lit_str(value: &str) -> Expression {
    Expression::Literal(Value::Varchar(value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_engine::tuple::{Column, Type};

    #[test]
    fn test_expression_binding() {
        let schema = Schema {
            columns: vec![
                Column::new("id".to_string(), Type::Integer),
                Column::new("name".to_string(), Type::Varchar(50)),
            ],
        };

        let expr = col("id").eq(lit(42));
        let bound = expr.bind(&schema).unwrap();

        match bound {
            Expression::BinaryOp { left, .. } => match *left {
                Expression::BoundColumn(idx) => assert_eq!(idx, 0),
                _ => panic!("Expected BoundColumn"),
            },
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn test_expression_evaluation() {
        let tuple = Tuple {
            values: vec![
                storage_engine::tuple::Value::Integer(42),
                storage_engine::tuple::Value::Varchar("Alice".to_string()),
            ],
        };

        // Test: column 0 == 42
        let expr = Expression::BoundColumn(0).eq(lit(42));
        let result = expr.evaluate(&tuple).unwrap();
        assert_eq!(result, Value::Integer(1)); // TRUE

        // Test: column 0 > 50
        let expr = Expression::BoundColumn(0).gt(lit(50));
        let result = expr.evaluate(&tuple).unwrap();
        assert_eq!(result, Value::Integer(0)); // FALSE
    }

    #[test]
    fn test_arithmetic_expressions() {
        let tuple = Tuple {
            values: vec![storage_engine::tuple::Value::Integer(10)],
        };

        // Test: column 0 + 5
        let expr = Expression::BoundColumn(0).add(lit(5));
        let result = expr.evaluate(&tuple).unwrap();
        assert_eq!(result, Value::Integer(15));
    }
}
