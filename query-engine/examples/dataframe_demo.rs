//! Comprehensive demo of the DataFrame API for rose-db.
//!
//! This example demonstrates:
//! - Creating a database and tables
//! - Inserting data
//! - Querying with filters
//! - Projections and limits
//! - Expression building

use query_engine::{column, int_column, varchar_column, Database, col, lit, lit_str, Value};
use storage_engine::tuple::Schema;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌹 Rose-DB DataFrame API Demo\n");

    // 1. Create/open database
    println!("📂 Opening database...");
    let db = Database::open("demo.db")?;
    println!("✓ Database opened\n");

    // 2. Create a users table
    println!("📋 Creating 'users' table...");
    let users_schema = Schema {
        columns: vec![
            int_column("id"),
            varchar_column("name", 50),
            int_column("age"),
            varchar_column("city", 50),
        ],
    };
    db.create_table("users", users_schema)?;
    println!("✓ Table created\n");

    // 3. Insert data using the DataFrame API
    println!("📝 Inserting data...");
    let users = db.table("users")?;

    users.insert(&[
        Value::Integer(1),
        Value::Varchar("Alice".to_string()),
        Value::Integer(30),
        Value::Varchar("NYC".to_string()),
    ])?;

    users.insert(&[
        Value::Integer(2),
        Value::Varchar("Bob".to_string()),
        Value::Integer(25),
        Value::Varchar("LA".to_string()),
    ])?;

    users.insert(&[
        Value::Integer(3),
        Value::Varchar("Charlie".to_string()),
        Value::Integer(35),
        Value::Varchar("NYC".to_string()),
    ])?;

    users.insert(&[
        Value::Integer(4),
        Value::Varchar("Diana".to_string()),
        Value::Integer(28),
        Value::Varchar("SF".to_string()),
    ])?;

    println!("✓ Inserted 4 users\n");

    // 4. Query: SELECT * FROM users
    println!("🔍 Query 1: SELECT * FROM users");
    println!("---");
    let all_users = db.table("users")?.collect()?;
    println!("Found {} users", all_users.len());
    for (i, user) in all_users.iter().enumerate() {
        println!("  User {}: {:?}", i + 1, user);
    }
    println!();

    // 5. Query with filter: SELECT * FROM users WHERE age > 27
    println!("🔍 Query 2: SELECT * FROM users WHERE age > 27");
    println!("---");
    let filtered = db.table("users")?
        .filter(col("age").gt(lit(27)))
        .collect()?;
    println!("Found {} users", filtered.len());
    for user in &filtered {
        println!("  {:?}", user);
    }
    println!();

    // 6. Query with projection: SELECT name, age FROM users
    println!("🔍 Query 3: SELECT name, age FROM users");
    println!("---");
    let projected = db.table("users")?
        .select(&["name", "age"])
        .collect()?;
    println!("Found {} users", projected.len());
    for user in &projected {
        println!("  {:?}", user);
    }
    println!();

    // 7. Combined query: SELECT name FROM users WHERE age > 27 LIMIT 2
    println!("🔍 Query 4: SELECT name FROM users WHERE age > 27 LIMIT 2");
    println!("---");
    let complex = db.table("users")?
        .filter(col("age").gt(lit(27)))
        .select(&["name"])
        .limit(2)
        .collect()?;
    println!("Found {} users", complex.len());
    for user in &complex {
        println!("  {:?}", user);
    }
    println!();

    // 8. Expression examples
    println!("📊 Expression Examples:");
    println!("---");

    // age >= 30
    let expr1 = col("age").gt_eq(lit(30));
    println!("✓ Built expression: age >= 30");

    // age > 25 AND age < 35
    let expr2 = col("age").gt(lit(25)).and(col("age").lt(lit(35)));
    println!("✓ Built expression: age > 25 AND age < 35");

    // name = 'Alice' OR name = 'Bob'
    let expr3 = col("name").eq(lit_str("Alice")).or(col("name").eq(lit_str("Bob")));
    println!("✓ Built expression: name = 'Alice' OR name = 'Bob'");
    println!();

    // 9. Show API features
    println!("✨ API Features:");
    println!("  - Lazy evaluation: plans built on .collect()");
    println!("  - Method chaining: .filter().select().limit()");
    println!("  - Type-safe expressions: compile-time checking");
    println!("  - NULL-aware semantics: SQL-compliant comparisons");
    println!("  - Volcano iterator model: memory efficient");
    println!();

    // Cleanup
    println!("🧹 Cleaning up...");
    std::fs::remove_file("demo.db").ok();
    println!("✓ Done!\n");

    println!("🎉 DataFrame API Demo Complete!");
    println!("\nNext steps:");
    println!("  - Add SQL parser (SQL → DataFrame API)");
    println!("  - Implement joins (HashJoin, NestedLoopJoin)");
    println!("  - Add aggregation (GROUP BY, COUNT, SUM)");
    println!("  - Integrate B+ tree indexes (IndexScan)");

    Ok(())
}
