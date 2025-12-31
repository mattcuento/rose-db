extern crate buffer_pool_manager;

use buffer_pool_manager::api::BufferPoolManager; // PageId is not used directly here, but in common_test_funcs
use buffer_pool_manager::actor::ActorBufferPoolManager;
use buffer_pool_manager::disk_manager::DiskManager;

use std::sync::Arc; // Re-add this import


mod common_test_funcs;

// Instantiate tests for ActorBufferPoolManager
test_bpm_implementation!(
    actor_bpm_tests,
    |disk_manager: Arc<DiskManager>, pool_size: usize| -> Arc<dyn BufferPoolManager> {
        Arc::new(ActorBufferPoolManager::new(pool_size, disk_manager))
    }
);
