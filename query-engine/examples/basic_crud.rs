//! Basic CRUD operations with rose-db.
//!
//! This example demonstrates:
//! - Creating a database and table
//! - Inserting data
//! - Querying with filters
//! - Collecting results

use query_engine::{Database, col, lit, int_column, varchar_column, Value};
use storage_engine::tuple::{Schema, Type};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Rose-DB Basic CRUD Example\n");

    // Create/open database
    let db = Database::open("examples_basic.db")?;
    println!("Database opened\n");

    // Create a users table
    println!("Creating 'users' table...");
    let schema = Schema {
        columns: vec![
            int_column("id"),
            varchar_column("name", 50),
            int_column("age"),
            varchar_column("city", 50),
        ],
    };
    db.create_table("users", schema)?;
    println!("Table created\n");

    // Insert some users
    println!("Inserting users...");
    let users_df = db.table("users")?;

    users_df.insert(&[
        Value::Integer(1),
        Value::Varchar("Alice".to_string()),
        Value::Integer(28),
        Value::Varchar("Seattle".to_string()),
    ])?;

    users_df.insert(&[
        Value::Integer(2),
        Value::Varchar("Bob".to_string()),
        Value::Integer(35),
        Value::Varchar("Portland".to_string()),
    ])?;

    users_df.insert(&[
        Value::Integer(3),
        Value::Varchar("Charlie".to_string()),
        Value::Integer(42),
        Value::Varchar("Seattle".to_string()),
    ])?;

    users_df.insert(&[
        Value::Integer(4),
        Value::Varchar("Diana".to_string()),
        Value::Integer(31),
        Value::Varchar("Austin".to_string()),
    ])?;

    println!("Inserted 4 users\n");

    // Flush to ensure data is persisted
    db.flush()?;

    // Query 1: Get all users
    println!("Query 1: All users");
    println!("{:-<50}", "");
    let all_users = db.table("users")?.collect()?;
    println!("Found {} users:", all_users.len());
    for (i, user) in all_users.iter().enumerate() {
        println!("  User {}: {:?}", i + 1, user.values);
    }
    println!();

    // Query 2: Filter by age
    println!("Query 2: Users over 30");
    println!("{:-<50}", "");
    let older_users = db.table("users")?
        .filter(col("age").gt(lit(30)))
        .collect()?;
    println!("Found {} users over 30:", older_users.len());
    for user in older_users.iter() {
        println!("  {:?}", user.values);
    }
    println!();

    // Query 3: Filter by city
    println!("Query 3: Users in Seattle");
    println!("{:-<50}", "");
    let seattle_users = db.table("users")?
        .filter(col("city").eq(query_engine::lit_str("Seattle")))
        .collect()?;
    println!("Found {} users in Seattle:", seattle_users.len());
    for user in seattle_users.iter() {
        println!("  {:?}", user.values);
    }
    println!();

    // Query 4: Select specific columns
    println!("Query 4: Just names and ages");
    println!("{:-<50}", "");
    let names_ages = db.table("users")?
        .select(&["name", "age"])
        .collect()?;
    println!("Found {} records:", names_ages.len());
    for record in names_ages.iter() {
        println!("  {:?}", record.values);
    }
    println!();

    // Query 5: Limit results
    println!("Query 5: First 2 users");
    println!("{:-<50}", "");
    let first_two = db.table("users")?
        .limit(2)
        .collect()?;
    println!("Limited to {} users:", first_two.len());
    for user in first_two.iter() {
        println!("  {:?}", user.values);
    }
    println!();

    // Query 6: Chained filters and projection
    println!("Query 6: Names of users in Seattle over 25");
    println!("{:-<50}", "");
    let result = db.table("users")?
        .filter(col("city").eq(query_engine::lit_str("Seattle")))
        .filter(col("age").gt(lit(25)))
        .select(&["name"])
        .collect()?;
    println!("Found {} matching users:", result.len());
    for record in result.iter() {
        println!("  {:?}", record.values);
    }
    println!();

    // Clean up
    std::fs::remove_file("examples_basic.db").ok();
    println!("Cleanup complete");

    Ok(())
}
