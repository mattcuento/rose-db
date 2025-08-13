use std::sync::Arc;

// Import the new modules
mod buffer_pool;


use buffer_pool::actor::ActorBpm;
use buffer_pool::concurrent::ConcurrentBpm;
use buffer_pool::disk_manager::DiskManager;

fn main() {
    println!("Setting up Buffer Pool Manager implementations for benchmarking.");

    // Initialize the disk manager
    let disk_manager = match DiskManager::new("database.db") {
        Ok(dm) => Arc::new(dm),
        Err(e) => {
            eprintln!("Failed to create disk manager: {}", e);
            return;
        }
    };

    // Instantiate the two BPM implementations
    // Note: The `new` functions will need to be implemented fully for this to work.
    let _concurrent_bpm = ConcurrentBpm::new(100, disk_manager.clone());
    let _actor_bpm = ActorBpm::new(100, disk_manager);

    println!("Skeletons created. Next steps would be to implement the `todo!()` sections and add benchmark logic.");
}