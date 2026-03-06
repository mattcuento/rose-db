//! Expression builder demonstration.
//!
//! Shows how to build complex queries using the expression API.

use query_engine::{Database, col, lit, int_column, varchar_column, Value};
use storage_engine::tuple::Schema;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Rose-DB Expression Builder Example\n");

    let db = Database::open("examples_expressions.db")?;

    // Create products table
    println!("Creating 'products' table...");
    let schema = Schema {
        columns: vec![
            int_column("id"),
            varchar_column("name", 50),
            int_column("price"),
            int_column("quantity"),
            varchar_column("category", 30),
        ],
    };
    db.create_table("products", schema)?;
    println!("Table created\n");

    // Insert sample products
    println!("Inserting products...");
    let products = db.table("products")?;

    let sample_products = vec![
        (1, "Laptop", 999, 5, "Electronics"),
        (2, "Mouse", 25, 50, "Electronics"),
        (3, "Keyboard", 75, 30, "Electronics"),
        (4, "Desk", 200, 10, "Furniture"),
        (5, "Chair", 150, 15, "Furniture"),
        (6, "Monitor", 300, 8, "Electronics"),
        (7, "Lamp", 45, 20, "Furniture"),
    ];

    for (id, name, price, quantity, category) in sample_products {
        products.insert(&[
            Value::Integer(id),
            Value::Varchar(name.to_string()),
            Value::Integer(price),
            Value::Integer(quantity),
            Value::Varchar(category.to_string()),
        ])?;
    }
    println!("Inserted {} products\n", 7);
    db.flush()?;

    // Example 1: Simple comparison
    println!("Example 1: Products priced under $100");
    println!("{:-<60}", "");
    let affordable = db.table("products")?
        .filter(col("price").lt(lit(100)))
        .collect()?;
    println!("Found {} affordable products:", affordable.len());
    for product in affordable.iter() {
        println!("  {:?}", product.values);
    }
    println!();

    // Example 2: Range query (price between 50 and 200)
    println!("Example 2: Products priced between $50 and $200");
    println!("{:-<60}", "");
    let mid_range = db.table("products")?
        .filter(col("price").gt_eq(lit(50)))
        .filter(col("price").lt_eq(lit(200)))
        .collect()?;
    println!("Found {} mid-range products:", mid_range.len());
    for product in mid_range.iter() {
        println!("  {:?}", product.values);
    }
    println!();

    // Example 3: AND condition (Electronics AND price < 100)
    println!("Example 3: Affordable Electronics (price < $100)");
    println!("{:-<60}", "");
    let affordable_electronics = db.table("products")?
        .filter(
            col("category")
                .eq(query_engine::lit_str("Electronics"))
                .and(col("price").lt(lit(100)))
        )
        .collect()?;
    println!("Found {} affordable electronics:", affordable_electronics.len());
    for product in affordable_electronics.iter() {
        println!("  {:?}", product.values);
    }
    println!();

    // Example 4: OR condition (price > 500 OR quantity < 10)
    println!("Example 4: Expensive OR Low Stock (price > $500 OR quantity < 10)");
    println!("{:-<60}", "");
    let special_attention = db.table("products")?
        .filter(
            col("price").gt(lit(500))
                .or(col("quantity").lt(lit(10)))
        )
        .collect()?;
    println!("Found {} products needing attention:", special_attention.len());
    for product in special_attention.iter() {
        println!("  {:?}", product.values);
    }
    println!();

    // Example 5: Projection with calculated values (if supported)
    println!("Example 5: Product names and prices");
    println!("{:-<60}", "");
    let summary = db.table("products")?
        .select(&["name", "price"])
        .collect()?;
    println!("Product pricing summary ({} items):", summary.len());
    for item in summary.iter() {
        println!("  {:?}", item.values);
    }
    println!();

    // Example 6: Complex nested conditions
    println!("Example 6: (Electronics AND price > $50) OR (Furniture AND quantity > 12)");
    println!("{:-<60}", "");
    let complex_query = db.table("products")?
        .filter(
            col("category")
                .eq(query_engine::lit_str("Electronics"))
                .and(col("price").gt(lit(50)))
                .or(
                    col("category")
                        .eq(query_engine::lit_str("Furniture"))
                        .and(col("quantity").gt(lit(12)))
                )
        )
        .collect()?;
    println!("Found {} products matching complex criteria:", complex_query.len());
    for product in complex_query.iter() {
        println!("  {:?}", product.values);
    }
    println!();

    // Example 7: Limit with filter
    println!("Example 7: Top 3 most expensive products");
    println!("{:-<60}", "");
    // Note: Without ORDER BY, this just gets first 3 that match
    let top_products = db.table("products")?
        .limit(3)
        .collect()?;
    println!("First {} products:", top_products.len());
    for product in top_products.iter() {
        println!("  {:?}", product.values);
    }
    println!();

    // Clean up
    std::fs::remove_file("examples_expressions.db").ok();
    println!("Cleanup complete");

    Ok(())
}
