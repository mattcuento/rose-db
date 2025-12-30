use buffer_pool_manager::buffer_pool::api::{BufferPoolManager, PageId, PAGE_SIZE};
use buffer_pool_manager::buffer_pool::concurrent::ConcurrentBufferPoolManager;
use buffer_pool_manager::buffer_pool::disk_manager::DiskManager;
use std::fs;
use std::sync::Arc;
use std::thread;

#[test]
fn test_bpm_new_page() {
    let db_file = "test_bpm_new_page.db";
    let disk_manager = Arc::new(DiskManager::new(db_file).unwrap());
    let bpm = ConcurrentBufferPoolManager::new(10, disk_manager);

    let page = bpm.new_page().unwrap();
    assert_eq!(page.page_id(), 0);

    fs::remove_file(db_file).unwrap();
}

#[test]
fn test_bpm_fetch_page() {
    let db_file = "test_bpm_fetch_page.db";
    let disk_manager = Arc::new(DiskManager::new(db_file).unwrap());
    let bpm = ConcurrentBufferPoolManager::new(10, disk_manager);

    let page = bpm.new_page().unwrap();
    let page_id = page.page_id();
    drop(page);

    let fetched_page = bpm.fetch_page(page_id).unwrap();
    assert_eq!(fetched_page.page_id(), page_id);

    fs::remove_file(db_file).unwrap();
}

#[test]
fn test_bpm_unpin_page() {
    let db_file = "test_bpm_unpin_page.db";
    let disk_manager = Arc::new(DiskManager::new(db_file).unwrap());
    let bpm = ConcurrentBufferPoolManager::new(3, disk_manager); // Small buffer pool

    let page = bpm.new_page().unwrap();
    let _page_id = page.page_id();

    // Page is pinned. Try to fill the buffer pool with other pages.
    // These new pages should not evict the pinned 'page'.
    let page_a = bpm.new_page().unwrap();
    let page_b = bpm.new_page().unwrap();

    // Now, try to create one more page. This should fail if 'page' is still pinned
    // and prevents eviction.
    let res = bpm.new_page();
    assert!(res.is_err()); // Should be NoFreeFrames because 'page' is pinned

    drop(page); // Unpin the original page
    drop(page_a); // Explicitly drop to unpin
    drop(page_b); // Explicitly drop to unpin

    // Now, we should be able to create a new page.
    let _page_c = bpm.new_page().unwrap(); // This should succeed

    fs::remove_file(db_file).unwrap();
}

#[test]
fn test_bpm_clock_replacement_predictable() {
    let db_file = "test_bpm_clock_replacement_predictable.db";
    let disk_manager = Arc::new(DiskManager::new(db_file).unwrap());
    let bpm = ConcurrentBufferPoolManager::new(3, disk_manager);

    // Create and unpin P0, P1, P2.
    // They will now all have ref=true.
    let p0 = bpm.new_page().unwrap();
    let id0 = p0.page_id();
    drop(p0);
    let p1 = bpm.new_page().unwrap();
    let id1 = p1.page_id();
    drop(p1);
    let p2 = bpm.new_page().unwrap();
    let id2 = p2.page_id();
    drop(p2);

    // This should cause a full sweep, setting all ref bits to false.
    // It will evict one page (the first one that was added, P0, from Frame 2)
    // and this new page p3 will take its place.
    let p3 = bpm.new_page().unwrap();
    let id3 = p3.page_id();
    drop(p3);

    // State after p3: P0 is evicted. P1, P2, P3 are in the pool.
    // P1, P2 have ref=false. P3 has ref=true.
    assert!(!bpm.page_table.read().unwrap().contains_key(&id0));

    // Access P1, setting its ref bit to true.
    drop(bpm.fetch_page(id1));

    // Current state: P2.ref=false, P1.ref=true, P3.ref=true
    // The clock hand should have advanced past the frame where p3 is.
    // When we create a new page, the next victim must be P2.
    let p4 = bpm.new_page().unwrap();
    let id4 = p4.page_id();
    drop(p4);

    // Assert P2 is now evicted.
    assert!(!bpm.page_table.read().unwrap().contains_key(&id2));

    // Assert P1, P3, and P4 are still in the pool.
    assert!(bpm.page_table.read().unwrap().contains_key(&id1));
    assert!(bpm.page_table.read().unwrap().contains_key(&id3));
    assert!(bpm.page_table.read().unwrap().contains_key(&id4));

    fs::remove_file(db_file).unwrap();
}

#[test]
fn test_bpm_clock_replacement_minimal() {
    let db_file = "test_bpm_clock_replacement_minimal.db";
    let disk_manager = Arc::new(DiskManager::new(db_file).unwrap());
    let bpm = ConcurrentBufferPoolManager::new(1, disk_manager); // Buffer pool size 1

    // Create page0 and unpin it.
    let page0 = bpm.new_page().unwrap();
    let page_id0 = page0.page_id();
    drop(page0);

    // At this point, page0 is in the buffer pool, unpinned, is_referenced = true.

    // Create page1. This should evict page0.
    let page1 = bpm.new_page().unwrap();
    let page_id1 = page1.page_id();
    drop(page1);

    // Assert that page0 is evicted (cannot be fetched from buffer pool).
    // Note: This tests if it's gone from the page_table. A fetch would read it from disk.
    assert!(!bpm.page_table.read().unwrap().contains_key(&page_id0));
    // Assert that page1 is in the buffer pool.
    assert!(bpm.page_table.read().unwrap().contains_key(&page_id1));

    fs::remove_file(db_file).unwrap();
}

#[test]
fn test_bpm_clock_replacement_minimal_v2() {
    let db_file = "test_bpm_clock_replacement_minimal_v2.db";
    let disk_manager = Arc::new(DiskManager::new(db_file).unwrap());
    let bpm = ConcurrentBufferPoolManager::new(2, disk_manager); // Pool size 2

    // Create page0 and page1 and unpin them.
    let page0 = bpm.new_page().unwrap(); let page_id0 = page0.page_id(); drop(page0);
    let page1 = bpm.new_page().unwrap(); let page_id1 = page1.page_id(); drop(page1);

    // Both are now in the pool, dirty, and have their reference bit set.
    // To create a predictable scenario, flush them both to make them clean.
    bpm.flush_all_pages().unwrap();

    // Now, access only page0. This sets its reference bit to true.
    // page1's reference bit remains false from the last CLOCK sweep (which hasn't happened yet,
    // but a full sweep would set it to false).
    // Let's perform a dummy new_page to cycle the clock and set all ref bits to false.
    let temp_page = bpm.new_page().unwrap(); let temp_id = temp_page.page_id(); drop(temp_page);
    bpm.flush_page(temp_id).unwrap(); // make it clean
    // Now p0 and p1 have ref=false. The new temp_page is in the pool.

    // Let's re-establish a clean state.
    // Pool has p0, p1. Evict one by creating p2.
    let p2 = bpm.new_page().unwrap(); let id2 = p2.page_id(); drop(p2);
    // Now pool has p1, p2 (assuming p0 was evicted). Both ref=false after sweep.
    bpm.flush_all_pages().unwrap();

    // Now fetch p1 to give it a ref bit.
    drop(bpm.fetch_page(page_id1));

    // State: p1.ref = true, p2.ref = false.
    // Create p3. Victim must be p2.
    let _p3 = bpm.new_page().unwrap();

    assert!(bpm.page_table.read().unwrap().contains_key(&page_id1));
    assert!(!bpm.page_table.read().unwrap().contains_key(&id2));
    
    fs::remove_file(db_file).unwrap();
}

// This test confirms that multiple threads can create pages concurrently without
// corrupting each other's data.
#[test]
fn test_bpm_multithreaded_many_threads_no_contention() {
    let db_file = "test_bpm_multithreaded_many_threads_no_contention.db";
    let disk_manager = Arc::new(DiskManager::new(db_file).unwrap());
    let bpm = Arc::new(ConcurrentBufferPoolManager::new(10, disk_manager));
    let mut threads = vec![];
    let num_threads = 5;

    for i in 0..num_threads {
        let bpm_clone = bpm.clone();
        threads.push(thread::spawn(move || {
            let mut page = bpm_clone.new_page().unwrap();
            let page_id = page.page_id();

            // Write a unique identifier to the page using DerefMut
            page[0] = i as u8;

            // The PageGuard will be dropped here, unpinning the page.
            page_id
        }));
    }

    let page_ids: Vec<PageId> = threads.into_iter().map(|t| t.join().unwrap()).collect();

    // Force all dirty pages to be written to disk. This is a workaround to ensure
    // that if a page was unexpectedly evicted, we can still fetch it.
    bpm.flush_all_pages().unwrap();

    // Verify the data in each page
    for (i, page_id) in page_ids.iter().enumerate() {
        let page = bpm.fetch_page(*page_id).unwrap();
        // Read the data using Deref
        assert_eq!(
            page[0],
            i as u8,
            "Data corruption detected for page {}",
            page_id
        );
    }

    fs::remove_file(db_file).unwrap();
}

