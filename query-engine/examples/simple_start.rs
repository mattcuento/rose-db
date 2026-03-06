//! Simplest possible rose-db example.
//!
//! Great starting point for learning the basics.

use query_engine::{Database, col, lit, int_column, varchar_column, Value};
use storage_engine::tuple::Schema;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Rose-DB - Simple Start\n");

    // Step 1: Open a database
    let db = Database::open("simple.db")?;
    println!("Step 1: Database opened\n");

    // Step 2: Create a table
    let schema = Schema {
        columns: vec![
            int_column("id"),
            varchar_column("message", 100),
        ],
    };
    db.create_table("greetings", schema)?;
    println!("Step 2: Table created\n");

    // Step 3: Insert data
    let table = db.table("greetings")?;
    table.insert(&[
        Value::Integer(1),
        Value::Varchar("Hello, World!".to_string()),
    ])?;
    table.insert(&[
        Value::Integer(2),
        Value::Varchar("Welcome to Rose-DB!".to_string()),
    ])?;
    db.flush()?;
    println!("Step 3: Data inserted\n");

    // Step 4: Query the data
    let results = db.table("greetings")?.collect()?;
    println!("Step 4: Query results ({} rows):", results.len());
    for row in results {
        println!("  {:?}", row.values);
    }
    println!();

    // Step 5: Filter the data
    let filtered = db.table("greetings")?
        .filter(col("id").eq(lit(1)))
        .collect()?;
    println!("Step 5: Filtered results ({} rows):", filtered.len());
    for row in filtered {
        println!("  {:?}", row.values);
    }
    println!();

    // Clean up
    std::fs::remove_file("simple.db").ok();
    println!("Done!");

    Ok(())
}
