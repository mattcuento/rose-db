//! Multi-table operations with rose-db.
//!
//! Demonstrates working with multiple tables in the same database.

use query_engine::{Database, col, lit, int_column, varchar_column, Value};
use storage_engine::tuple::Schema;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Rose-DB Multi-Table Example\n");

    let db = Database::open("examples_multi.db")?;

    // Create customers table
    println!("Creating 'customers' table...");
    let customers_schema = Schema {
        columns: vec![
            int_column("customer_id"),
            varchar_column("name", 50),
            varchar_column("email", 100),
        ],
    };
    db.create_table("customers", customers_schema)?;
    println!("Customers table created");

    // Create orders table
    println!("Creating 'orders' table...");
    let orders_schema = Schema {
        columns: vec![
            int_column("order_id"),
            int_column("customer_id"),
            int_column("amount"),
            varchar_column("status", 20),
        ],
    };
    db.create_table("orders", orders_schema)?;
    println!("Orders table created\n");

    // Insert customers
    println!("Inserting customers...");
    let customers = db.table("customers")?;

    let customer_data = vec![
        (1, "Alice Johnson", "alice@example.com"),
        (2, "Bob Smith", "bob@example.com"),
        (3, "Charlie Brown", "charlie@example.com"),
        (4, "Diana Prince", "diana@example.com"),
    ];

    for (id, name, email) in customer_data {
        customers.insert(&[
            Value::Integer(id),
            Value::Varchar(name.to_string()),
            Value::Varchar(email.to_string()),
        ])?;
    }
    println!("Inserted {} customers", 4);

    // Insert orders
    println!("Inserting orders...");
    let orders = db.table("orders")?;

    let order_data = vec![
        (101, 1, 150, "completed"),
        (102, 1, 200, "pending"),
        (103, 2, 75, "completed"),
        (104, 3, 300, "completed"),
        (105, 3, 120, "shipped"),
        (106, 4, 90, "pending"),
    ];

    for (order_id, customer_id, amount, status) in order_data {
        orders.insert(&[
            Value::Integer(order_id),
            Value::Integer(customer_id),
            Value::Integer(amount),
            Value::Varchar(status.to_string()),
        ])?;
    }
    println!("Inserted {} orders\n", 6);
    db.flush()?;

    // List all tables
    println!("Database tables:");
    println!("{:-<60}", "");
    let tables = db.list_tables();
    for table in &tables {
        println!("  - {}", table);
    }
    println!();

    // Query customers
    println!("All Customers:");
    println!("{:-<60}", "");
    let all_customers = db.table("customers")?.collect()?;
    println!("Total customers: {}", all_customers.len());
    for customer in all_customers.iter() {
        println!("  {:?}", customer.values);
    }
    println!();

    // Query orders
    println!("All Orders:");
    println!("{:-<60}", "");
    let all_orders = db.table("orders")?.collect()?;
    println!("Total orders: {}", all_orders.len());
    for order in all_orders.iter() {
        println!("  {:?}", order.values);
    }
    println!();

    // Query orders by status
    println!("Completed Orders:");
    println!("{:-<60}", "");
    let completed = db.table("orders")?
        .filter(col("status").eq(query_engine::lit_str("completed")))
        .collect()?;
    println!("Found {} completed orders:", completed.len());
    for order in completed.iter() {
        println!("  {:?}", order.values);
    }
    println!();

    // Query orders for specific customer
    println!("Orders for Customer #1 (Alice):");
    println!("{:-<60}", "");
    let alice_orders = db.table("orders")?
        .filter(col("customer_id").eq(lit(1)))
        .collect()?;
    println!("Found {} orders for customer #1:", alice_orders.len());
    for order in alice_orders.iter() {
        println!("  {:?}", order.values);
    }
    println!();

    // Query high-value orders
    println!("High-Value Orders (> $100):");
    println!("{:-<60}", "");
    let high_value = db.table("orders")?
        .filter(col("amount").gt(lit(100)))
        .collect()?;
    println!("Found {} high-value orders:", high_value.len());
    for order in high_value.iter() {
        println!("  {:?}", order.values);
    }
    println!();

    // Customer emails only
    println!("Customer Email List:");
    println!("{:-<60}", "");
    let emails = db.table("customers")?
        .select(&["email"])
        .collect()?;
    println!("Email addresses ({} total):", emails.len());
    for record in emails.iter() {
        println!("  {:?}", record.values);
    }
    println!();

    // Pending and shipped orders
    println!("Pending and Shipped Orders:");
    println!("{:-<60}", "");
    let in_progress = db.table("orders")?
        .filter(
            col("status").eq(query_engine::lit_str("pending"))
                .or(col("status").eq(query_engine::lit_str("shipped")))
        )
        .collect()?;
    println!("Found {} orders in progress:", in_progress.len());
    for order in in_progress.iter() {
        println!("  {:?}", order.values);
    }
    println!();

    // Clean up
    std::fs::remove_file("examples_multi.db").ok();
    println!("Cleanup complete");

    Ok(())
}
