# Query Engine - DataFrame API for Rose-DB

A programmatic query processing engine with a DataFrame-style API inspired by Polars and DataFusion.

## 🎯 Overview

Instead of starting with SQL parsing, this query engine provides a **fluent programmatic API** that allows users to build and execute queries using Rust method chaining. This approach:

- ✅ Gets to working queries **10x faster** than building a SQL parser first
- ✅ Provides **better error messages** at compile time (type safety)
- ✅ Makes **testing easier** (no string parsing required)
- ✅ Can be **extended with SQL** later (parser just translates SQL → DataFrame calls)

## 🚀 Quick Start

```rust
use query_engine::{Database, int_column, varchar_column, col, lit, Value};
use storage_engine::tuple::Schema;

// Open database
let db = Database::open("mydb.db")?;

// Create table
let schema = Schema {
    columns: vec![
        int_column("id"),
        varchar_column("name", 50),
        int_column("age"),
    ],
};
db.create_table("users", schema)?;

// Insert data
let users = db.table("users")?;
users.insert(&[
    Value::Integer(1),
    Value::Varchar("Alice".into()),
    Value::Integer(30),
])?;

// Query with DataFrame API
let results = db.table("users")?
    .filter(col("age").gt(lit(25)))      // WHERE age > 25
    .select(&["name", "age"])             // SELECT name, age
    .limit(10)                             // LIMIT 10
    .collect()?;                           // Execute and collect results
```

## 📋 API Reference

### Database Operations

```rust
// Open/create database
let db = Database::open("path/to/db.db")?;

// Create table
db.create_table("table_name", schema)?;

// Get table reference
let table = db.table("table_name")?;

// List all tables
let tables = db.list_tables();

// Drop table
db.drop_table("table_name")?;
```

### DataFrame Operations

```rust
// Get DataFrame from table
let df = db.table("users")?;

// Filter (WHERE clause)
df.filter(col("age").gt(lit(25)))

// Select columns (projection)
df.select(&["name", "email"])

// Limit results
df.limit(10)

// Execute query
let results = df.collect()?;

// Show results (debug print)
df.show()?;
```

### Expression Building

```rust
// Column reference
col("age")

// Literals
lit(42)              // Integer
lit_str("Alice")     // String

// Comparison operators
col("age").eq(lit(25))       // age = 25
col("age").not_eq(lit(25))   // age != 25
col("age").lt(lit(25))       // age < 25
col("age").lt_eq(lit(25))    // age <= 25
col("age").gt(lit(25))       // age > 25
col("age").gt_eq(lit(25))    // age >= 25

// Logical operators
col("age").gt(lit(25)).and(col("age").lt(lit(50)))  // age > 25 AND age < 50
col("name").eq(lit_str("Alice")).or(col("name").eq(lit_str("Bob")))

// Arithmetic
col("age").add(lit(1))       // age + 1
col("price").subtract(...)   // price - x
col("qty").multiply(...)     // qty * x

// NULL checks
col("email").is_null()
col("email").is_not_null()
```

### Helper Functions

```rust
// Create columns
int_column("id")                // Integer column
varchar_column("name", 50)      // Varchar column with length
column("custom", Type::Integer, 4)  // Generic column constructor
```

## 🏗️ Architecture

### Execution Model: Volcano Iterator

The query engine uses the **Volcano iterator model** (same as PostgreSQL, MySQL):

```
┌──────────────┐
│   collect()  │  ← Client calls
└──────┬───────┘
       │ next()
┌──────▼───────┐
│    Limit     │  ← Returns at most N tuples
└──────┬───────┘
       │ next()
┌──────▼───────┐
│  Projection  │  ← Evaluates expressions
└──────┬───────┘
       │ next()
┌──────▼───────┐
│    Filter    │  ← Applies WHERE predicate
└──────┬───────┘
       │ next()
┌──────▼───────┐
│   SeqScan    │  ← Scans TableHeap pages
└──────────────┘
```

Each operator implements the `Executor` trait:

```rust
pub trait Executor {
    fn schema(&self) -> &Schema;
    fn init(&mut self) -> Result<()>;
    fn next(&mut self) -> Result<Option<Tuple>>;
}
```

### Components

```
query-engine/
├── catalog/       # Table metadata management (RwLock for concurrency)
├── types/         # Value type with NULL support
├── expression/    # Expression system (col, lit, operators)
├── executor/      # Volcano-model executors
│   ├── seq_scan   # Sequential scan through TableHeap
│   ├── filter     # WHERE clause evaluation
│   ├── projection # SELECT column evaluation
│   └── limit      # LIMIT N results
├── database       # Main entry point
└── dataframe      # Fluent query builder API
```

## 🎨 Features

### ✅ Implemented

- **Database Management**: Open, create tables, catalog
- **Data Manipulation**: Insert tuples
- **Query Execution**:
  - Sequential scans
  - Predicate filtering (WHERE)
  - Column projection (SELECT)
  - Result limiting (LIMIT)
- **Expression System**:
  - Column references
  - Literals (integers, strings)
  - Comparison operators (=, !=, <, <=, >, >=)
  - Logical operators (AND, OR, NOT)
  - Arithmetic (+, -, *, /)
  - NULL-aware semantics
- **Type System**: Integer, Varchar, NULL
- **Concurrency**: Thread-safe catalog (RwLock)

### 🚧 Planned

- **Joins**: NestedLoopJoin, HashJoin, IndexNestedLoopJoin
- **Aggregation**: GROUP BY, COUNT, SUM, AVG, MIN, MAX
- **Sorting**: ORDER BY with external sort
- **Indexes**: IndexScan using B+ tree (already implemented!)
- **SQL Parser**: Translate SQL → DataFrame API
- **Optimizations**:
  - Predicate pushdown
  - Index selection
  - Join reordering
  - Cost-based optimization
- **Advanced Features**:
  - Transactions
  - Subqueries
  - Window functions

## 📊 Example Queries

See `examples/dataframe_demo.rs` for comprehensive examples.

### Basic Query

```rust
// SELECT * FROM users WHERE age > 25 LIMIT 10
db.table("users")?
    .filter(col("age").gt(lit(25)))
    .limit(10)
    .collect()?
```

### Projection

```rust
// SELECT name, email FROM users
db.table("users")?
    .select(&["name", "email"])
    .collect()?
```

### Complex Filter

```rust
// SELECT * FROM users WHERE (age > 25 AND age < 50) OR city = 'NYC'
db.table("users")?
    .filter(
        col("age").gt(lit(25))
            .and(col("age").lt(lit(50)))
            .or(col("city").eq(lit_str("NYC")))
    )
    .collect()?
```

### Expression Projection

```rust
// SELECT name, age + 1 AS next_year FROM users
db.table("users")?
    .select_exprs(&[
        (col("name"), "name"),
        (col("age").add(lit(1)), "next_year"),
    ])
    .collect()?
```

## 🔬 Implementation Details

### Lazy Evaluation

Queries are **lazy** - no execution happens until `.collect()` is called:

```rust
let df = db.table("users")?     // No execution
    .filter(col("age").gt(25))  // Builds plan
    .select(&["name"]);          // Extends plan

let results = df.collect()?;     // NOW executes the full plan
```

### Expression Binding

Expressions go through a **binding** phase that resolves column names to indices:

```rust
// Before binding
col("age").gt(lit(25))  // Column reference by name

// After binding (happens automatically)
BoundColumn(2).gt(Literal(25))  // Column reference by index
```

This allows efficient tuple access during execution.

### NULL Semantics

The type system properly handles NULL with SQL semantics:

```rust
NULL == 42     → NULL (not TRUE, not FALSE)
NULL AND TRUE  → NULL
TRUE OR NULL   → TRUE
NULL + 1       → NULL
```

## 🚀 Performance Characteristics

- **Memory**: Tuple-at-a-time streaming (Volcano model) - minimal memory footprint
- **Concurrency**: Catalog uses RwLock - many concurrent readers, exclusive writers
- **Latching**: Inherits from buffer pool's page-level latching
- **Optimization**: Currently rule-based (future: cost-based with statistics)

## 🎓 Educational Value

This implementation demonstrates:

1. **Iterator Pattern**: Composable operators via Volcano model
2. **Lazy Evaluation**: Query plans built without execution
3. **Expression Trees**: AST for predicates and projections
4. **Type Safety**: Compile-time checking for column references
5. **Concurrency**: Lock-based coordination (RwLock pattern)

## 🔗 Integration with Rose-DB

- **Storage**: Uses existing `TableHeap` from storage-engine
- **Buffer Pool**: Leverages buffer pool manager for I/O
- **Indexes**: Ready to integrate B+ tree (already implemented!)
- **Schemas**: Re-uses `Schema`, `Tuple`, `Type` from storage-engine

## 📝 Future: SQL Layer

The DataFrame API provides the **execution foundation**. Adding SQL is straightforward:

```rust
// SQL Parser (future)
let sql = "SELECT name FROM users WHERE age > 25";
let ast = parse_sql(sql)?;

// Translate to DataFrame API (this is just syntax sugar!)
let df = db.table(&ast.table_name)?;
if let Some(filter) = ast.where_clause {
    df = df.filter(translate_expr(filter));
}
df = df.select(&ast.columns);

// Execute (same as manual API usage)
let results = df.collect()?;
```

## 🧪 Testing

```bash
# Run all tests
cargo test --package query_engine

# Run specific test
cargo test --package query_engine test_name

# Run example
cargo run --package query_engine --example dataframe_demo
```

## 📚 References

- **Volcano Model**: Graefe, "Volcano—An Extensible and Parallel Query Evaluation System"
- **Expression Evaluation**: Similar to Apache DataFusion
- **Iterator Pattern**: Common in modern query engines (Polars, DataFusion)

## 🤝 Contributing

Areas for contribution:
- [ ] Fix remaining test failures (tuple serialization)
- [ ] Implement HashJoin operator
- [ ] Add GROUP BY / aggregation
- [ ] Build SQL parser
- [ ] Add benchmarks (vs SQLite)
- [ ] Optimize hot paths (vectorization)

---

Built with ❤️ for educational purposes. Demonstrates production-grade query processing patterns in Rust.
