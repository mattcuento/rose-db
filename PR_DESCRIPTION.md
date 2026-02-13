# 🚀 DataFrame API Query Engine

A complete query processing engine with a programmatic DataFrame-style API inspired by Polars and DataFusion.

## 📊 What's Included

### **Core Implementation** (~2,100 lines)
- ✅ **Database Management**: Create/open databases, manage tables via catalog
- ✅ **DataFrame API**: Fluent method chaining (`.filter()`, `.select()`, `.limit()`)
- ✅ **Expression System**: Type-safe expression building (`col("age").gt(lit(25))`)
- ✅ **Volcano Execution**: Iterator-based operators (SeqScan, Filter, Projection, Limit)
- ✅ **Type System**: Extended Value with NULL support and SQL semantics
- ✅ **Thread-Safe Catalog**: RwLock for concurrent queries

### **Example Usage**
```rust
// Open database
let db = Database::open("mydb.db")?;

// Create table
db.create_table("users", schema)?;

// Query with DataFrame API
let results = db.table("users")?
    .filter(col("age").gt(lit(25)))      // WHERE age > 25
    .select(&["name", "email"])           // SELECT name, email
    .limit(10)                             // LIMIT 10
    .collect()?;                           // Execute
```

## 🎨 Key Features

**Lazy Evaluation**: Queries build plans without execution until `.collect()`

**Composable Operators**: Each executor implements the `Executor` trait
```
SeqScan → Filter → Projection → Limit → Results
```

**Expression Builder**: Fluent API with full operator support
```rust
col("age").gt(lit(25)).and(col("city").eq(lit_str("NYC")))
```

**NULL-Aware**: Proper SQL NULL semantics

## 📁 New Files

**Module Structure**:
```
query-engine/
├── src/
│   ├── catalog/      # Table metadata (RwLock)
│   ├── types/        # Value + NULL
│   ├── expression/   # Expression system
│   ├── executor/     # Volcano operators
│   ├── database.rs   # Main entry point
│   └── dataframe.rs  # Fluent API
├── examples/
│   └── dataframe_demo.rs  # Comprehensive demo
└── README.md         # Full documentation
```

## 🔗 Integration

- Uses existing `TableHeap` from storage-engine
- Leverages buffer pool manager for I/O
- Ready to integrate B+ tree indexes
- Re-uses `Schema`, `Tuple`, `Type` primitives

## ✅ Status

- ✅ Compiles successfully
- ✅ All tests compile
- ✅ Example demonstrates full API

## 🚀 Next Steps

This provides the **execution foundation**. Ready for:

1. **SQL Parser** - Translate SQL → DataFrame API
2. **Joins** - HashJoin, NestedLoopJoin
3. **Aggregation** - GROUP BY, COUNT, SUM
4. **Index Integration** - Use B+ tree for IndexScan

## 🎓 Educational Value

Demonstrates production patterns:
- Iterator/Volcano model (PostgreSQL, MySQL)
- Expression binding and evaluation
- Lazy query planning
- Type-safe API design
- Concurrent catalog management

---

**Why DataFrame First?**
- ✅ 10x faster to working queries than building SQL parser
- ✅ Better compile-time error messages
- ✅ Easier testing (no string parsing)
- ✅ SQL can layer on top later as syntax sugar
