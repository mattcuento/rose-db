
use buffer_pool_manager::buffer_pool::disk_manager::DiskManager;
use buffer_pool_manager::buffer_pool::api::PAGE_SIZE;
use std::fs;
use std::sync::Arc;

#[test]
fn test_disk_manager_allocate() {
    let db_file = "test_disk_manager_allocate.db";
    let disk_manager = DiskManager::new(db_file).unwrap();
    assert_eq!(disk_manager.allocate_page(), 0);
    assert_eq!(disk_manager.allocate_page(), 1);
    fs::remove_file(db_file).unwrap();
}

#[test]
fn test_disk_manager_read_write() {
    let db_file = "test_disk_manager_read_write.db";
    let disk_manager = Arc::new(DiskManager::new(db_file).unwrap());
    let page_id = disk_manager.allocate_page();
    
    let mut data = [0u8; PAGE_SIZE];
    for i in 0..PAGE_SIZE {
        data[i] = i as u8;
    }

    disk_manager.write_page(page_id, &data).unwrap();

    let mut read_data = [0u8; PAGE_SIZE];
    disk_manager.read_page(page_id, &mut read_data).unwrap();

    assert_eq!(data, read_data);

    fs::remove_file(db_file).unwrap();
}
