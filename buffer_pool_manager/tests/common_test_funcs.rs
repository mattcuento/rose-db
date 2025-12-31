// common_test_funcs.rs
extern crate buffer_pool_manager;

use buffer_pool_manager::api::{BufferPoolManager, PageId};
use buffer_pool_manager::disk_manager::DiskManager;
// Removed unused imports: SlottedPage, PageType, INVALID_PAGE_ID, PAGE_SIZE
use std::fs;
use std::sync::Arc;
use std::thread;

// Helper to remove db file, useful in tests.
pub fn cleanup_db_file(db_file: &str) {
    let _ = fs::remove_file(db_file);
}

// Test case: new_page
pub fn test_case_new_page(bpm_factory: impl Fn(Arc<DiskManager>, usize) -> Arc<dyn BufferPoolManager + 'static>, db_file: &str, pool_size: usize) {
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
    let bpm = bpm_factory(disk_manager, pool_size);
    let page = bpm.new_page().unwrap();
    assert_eq!(page.page_id(), 0);
    drop(page); // Unpin the page before removing the file
    cleanup_db_file(db_file);
}

// Test case: fetch_page
pub fn test_case_fetch_page(bpm_factory: impl Fn(Arc<DiskManager>, usize) -> Arc<dyn BufferPoolManager + 'static>, db_file: &str, pool_size: usize) {
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
    let bpm = bpm_factory(disk_manager, pool_size);
    let page = bpm.new_page().unwrap();
    let page_id = page.page_id();
    drop(page);

    let fetched_page = bpm.fetch_page(page_id).unwrap();
    assert_eq!(fetched_page.page_id(), page_id);
    drop(fetched_page); // Unpin
    cleanup_db_file(db_file);
}

// Test case: unpin_page
pub fn test_case_unpin_page(bpm_factory: impl Fn(Arc<DiskManager>, usize) -> Arc<dyn BufferPoolManager + 'static>, db_file: &str, pool_size: usize) {
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
    let bpm = bpm_factory(disk_manager, pool_size);

    let mut pages = Vec::new();

    // Create a page and pin it.
    let page_pinned = bpm.new_page().unwrap();
    let _page_id_pinned = page_pinned.page_id();

    // Fill the buffer pool with (pool_size - 1) new pages.
    // These should not evict the pinned 'page_pinned'.
    for _ in 0..(pool_size - 1) {
        pages.push(bpm.new_page().unwrap());
    }

    // Now, try to create one more page. This should fail if 'page_pinned' is still pinned
    // and prevents eviction, as the pool is full and no other pages can be evicted.
    let res = bpm.new_page();
    assert!(res.is_err(), "Expected NoFreeFrames error, got {:?}", res);

    drop(page_pinned); // Unpin the original page
    pages.clear(); // Drop all other pages, unpinning them.

    // Now, we should be able to create a new page because frames are free.
    let _page_c = bpm.new_page().unwrap(); // This should succeed
    
    cleanup_db_file(db_file);
}

// Test case: multithreaded_many_threads_no_contention
pub fn test_case_multithreaded_many_threads_no_contention(bpm_factory: impl Fn(Arc<DiskManager>, usize) -> Arc<dyn BufferPoolManager + 'static>, db_file: &str, pool_size: usize) {
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
    let bpm = bpm_factory(disk_manager.clone(), pool_size); // Recreate BPM for this test
    let mut threads = vec![];
    let num_threads = 5;

    for _i in 0..num_threads {
        let bpm_clone = bpm.clone();
        threads.push(thread::spawn(move || {
            let mut page = bpm_clone.new_page().unwrap();
            let page_id = page.page_id();

            // Write a unique identifier to the page using DerefMut
            page[0] = page_id as u8; // Use page_id as unique identifier

            // The PageGuard will be dropped here, unpinning the page.
            page_id
        }));
    }

    let page_ids: Vec<PageId> = threads.into_iter().map(|t| t.join().unwrap()).collect();

    // Force all dirty pages to be written to disk.
    // This is a workaround to ensure that if a page was unexpectedly evicted, we can still fetch it.
    bpm.flush_all_pages().unwrap();

    // Verify the data in each page
    for page_id in page_ids.iter() {
        let page = bpm.fetch_page(*page_id).unwrap();
        // Read the data using Deref
        assert_eq!(
            page[0],
            *page_id as u8, // Compare with page_id as u8
            "Data corruption detected for page {}",
            page_id
        );
        drop(page);
    }
    cleanup_db_file(db_file);
}

#[macro_export]
macro_rules! test_bpm_implementation {
    ($test_suite_name:ident, $bpm_factory:expr) => {
        mod $test_suite_name {
            use super::*;
            use common_test_funcs::*; // Import common test functions

            const TEST_POOL_SIZE: usize = 3; // Define a small pool size for tests like unpin_page
            const MULTITHREADED_POOL_SIZE: usize = 10; // A larger pool size for multithreaded tests

            #[test]
            fn new_page() {
                let db_file = format!("{}_new_page.db", stringify!($test_suite_name));
                // The factory function receives the disk manager and the pool size.
                common_test_funcs::test_case_new_page(|dm, ps| $bpm_factory(dm, ps), &db_file, TEST_POOL_SIZE);
            }

            #[test]
            fn fetch_page() {
                let db_file = format!("{}_fetch_page.db", stringify!($test_suite_name));
                // The factory function receives the disk manager and the pool size.
                common_test_funcs::test_case_fetch_page(|dm, ps| $bpm_factory(dm, ps), &db_file, TEST_POOL_SIZE);
            }

            // Unpin page test adapted for both BPMs
            #[test]
            fn unpin_page() {
                let db_file = format!("{}_unpin_page.db", stringify!($test_suite_name));
                // We pass a clone of the factory and the specific pool size needed for this test.
                common_test_funcs::test_case_unpin_page(|dm, ps| $bpm_factory(dm, ps), &db_file, TEST_POOL_SIZE);
            }

            // Multithreaded test adapted for both BPMs
            #[test]
            fn multithreaded_many_threads_no_contention() {
                let db_file = format!("{}_multithreaded_many_threads_no_contention.db", stringify!($test_suite_name));
                common_test_funcs::test_case_multithreaded_many_threads_no_contention(
                    |dm, ps| $bpm_factory(dm, ps),
                    &db_file,
                    MULTITHREADED_POOL_SIZE // A larger pool size for this specific test
                );
            }

            // Clock replacement tests are specific to ConcurrentBufferPoolManager's internal state
            // and cannot be easily parameterized using only the BufferPoolManager trait.
            // These tests are not included here for now.
        }
    };
}