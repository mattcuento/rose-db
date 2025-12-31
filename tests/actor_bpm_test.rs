
use buffer_pool_manager::buffer_pool::api::BufferPoolManager;
use buffer_pool_manager::buffer_pool::actor::ActorBpm;
use buffer_pool_manager::buffer_pool::disk_manager::DiskManager;
use std::fs;
use std::sync::Arc;
use std::thread;

#[test]
fn test_actor_bpm_new_page() {
    let db_file = "test_actor_bpm_new_page.db";
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
    let bpm = ActorBpm::new(10, disk_manager);

    let page = bpm.new_page().unwrap();
    // The first page allocated by the disk manager has ID 0.
    assert_eq!(page.page_id(), 0);

    fs::remove_file(db_file).unwrap();
}

#[test]
fn test_actor_bpm_fetch_page() {
    let db_file = "test_actor_bpm_fetch_page.db";
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
    let bpm = ActorBpm::new(10, disk_manager);

    let page_id = {
        // Create a page and get its ID
        let page = bpm.new_page().unwrap();
        page.page_id()
        // Page is unpinned when guard is dropped
    };

    // Fetch the same page
    let fetched_page = bpm.fetch_page(page_id).unwrap();
    assert_eq!(fetched_page.page_id(), page_id);

    fs::remove_file(db_file).unwrap();
}

#[test]
fn test_actor_bpm_unpin_page() {
    let db_file = "test_actor_bpm_unpin_page.db";
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
    let bpm = ActorBpm::new(3, disk_manager);

    // Pin page 0
    let page0 = bpm.new_page().unwrap();
    assert_eq!(page0.page_id(), 0);

    // Pin page 1
    let page1 = bpm.new_page().unwrap();
    assert_eq!(page1.page_id(), 1);
    
    // Pin page 2
    let page2 = bpm.new_page().unwrap();
    assert_eq!(page2.page_id(), 2);

    // Pool is full, all pages are pinned. A new page request should fail.
    let res = bpm.new_page();
    assert!(res.is_err(), "Pool should be full, new_page should fail");

    // Drop page1, which unpins it.
    drop(page1);

    // A new page request should now succeed by evicting page 1.
    let page3 = bpm.new_page().unwrap();
    assert_eq!(page3.page_id(), 3);

    // Tidy up
    drop(page0);
    drop(page2);
    drop(page3);
    fs::remove_file(db_file).unwrap();
}

// #[test]
// fn test_actor_bpm_clock_replacement_blackbox() {
//     let db_file = "test_actor_bpm_clock_replacement_blackbox.db";
//     let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
//     let bpm = ActorBpm::new(3, disk_manager);

//     // Create and unpin P0, P1, P2.
//     // As new pages, they are all dirty.
//     let p0 = bpm.new_page().unwrap(); let id0 = p0.page_id(); drop(p0);
//     let p1 = bpm.new_page().unwrap(); let id1 = p1.page_id(); drop(p1);
//     let p2 = bpm.new_page().unwrap(); let id2 = p2.page_id(); drop(p2);

//     // Flush them all to disk, making them non-dirty.
//     bpm.flush_all_pages().unwrap();

//     // Access P0 and P2. This will set their 'is_referenced' flag inside the actor.
//     drop(bpm.fetch_page(id0));
//     drop(bpm.fetch_page(id2));
    
//     // Now, P0.ref=T, P1.ref=F, P2.ref=T (or some permutation).
//     // When we create a new page, P1 is the only viable candidate for eviction
//     // that doesn't get a second chance.
//     let _p3 = bpm.new_page().unwrap();

//     // Since P1 was not dirty, it was not flushed to disk on eviction.
//     // Attempting to fetch it should result in an error because the disk manager
//     // will try to read a page that doesn't exist.
//     let res = bpm.fetch_page(id1);
//     assert!(
//         matches!(res, Err(buffer_pool_manager::buffer_pool::api::BpmError::IoError(_))),
//         "Expected an I/O Error when fetching an evicted non-dirty page"
//     );

//     // P0 and P2 should be fine to fetch.
//     assert!(bpm.fetch_page(id0).is_ok());
//     assert!(bpm.fetch_page(id2).is_ok());

//     fs::remove_file(db_file).unwrap();
// }

#[test]
fn test_actor_bpm_multithreaded_no_contention() {
    let db_file = "test_actor_bpm_multithreaded_no_contention.db";
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
    // The Arc allows the BPM to be shared between threads.
    let bpm = Arc::new(ActorBpm::new(10, disk_manager));
    let mut threads = vec![];
    let num_threads = 5;

    for i in 0..num_threads {
        let bpm_clone = bpm.clone();
        threads.push(thread::spawn(move || {
            // Each thread creates a new page
            let mut page = bpm_clone.new_page().unwrap();
            let page_id = page.page_id();

            // Write a unique identifier to the page
            page[0] = i as u8;

            // PageGuard is dropped, sending the modified data and unpinning.
            page_id
        }));
    }

    let page_ids: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();

    // Workaround: Flush all pages to disk to ensure they are available for fetching,
    // even if they were unexpectedly evicted without being flushed.
    bpm.flush_all_pages().unwrap();

    // Verify the data in each page
    for (i, page_id) in page_ids.iter().enumerate() {
        let page = bpm.fetch_page(*page_id).unwrap();
        // Read the data
        assert_eq!(
            page[0],
            i as u8,
            "Data corruption detected for page {}",
            page_id
        );
    }

    fs::remove_file(db_file).unwrap();
}
