
use buffer_pool_manager::disk_manager::DiskManager;
use std::sync::Arc;
use std::fs;
use buffer_pool_manager::concurrent::ConcurrentBufferPoolManager;
use storage::table::TableHeap;
use storage::tuple::{Column, Schema, Tuple, Type, Value};

#[test]
fn test_table_heap_insert_get() {
    let db_file = "test_table_heap_insert_get.db";
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
    let bpm = Arc::new(ConcurrentBufferPoolManager::new(10, disk_manager));

    let schema = Schema {
        columns: vec![
            Column {
                name: "id".to_string(),
                column_type: Type::Integer,
                length: 4,
            },
            Column {
                name: "name".to_string(),
                column_type: Type::Varchar,
                length: 20,
            },
        ],
    };

    let table_heap = TableHeap::new(bpm.clone(), schema.clone());

    let tuple1 = Tuple {
        values: vec![
            Value::Integer(1),
            Value::Varchar("hello".to_string()),
        ],
    };

    let tuple2 = Tuple {
        values: vec![
            Value::Integer(2),
            Value::Varchar("world".to_string()),
        ],
    };

    let row_id1 = table_heap.insert_tuple(&tuple1).unwrap();
    let row_id2 = table_heap.insert_tuple(&tuple2).unwrap();

    let retrieved_tuple1 = table_heap.get_tuple(row_id1).unwrap();
    let retrieved_tuple2 = table_heap.get_tuple(row_id2).unwrap();

    assert_eq!(tuple1.values, retrieved_tuple1.values);
    assert_eq!(tuple2.values, retrieved_tuple2.values);

    fs::remove_file(db_file).unwrap();
}
