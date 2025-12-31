
use std::sync::Arc;
use std::time::{Duration, Instant};
use buffer_pool_manager::actor::ActorBufferPoolManager;
use buffer_pool_manager::api::BufferPoolManager;
use buffer_pool_manager::disk_manager::DiskManager;
use buffer_pool_manager::concurrent::ConcurrentBufferPoolManager;

enum BenchmarkType {
    Read,
    Write,
}

struct BenchmarkResult {
    concurrent_write: Duration,
    concurrent_read: Duration,
}

fn main() {
    println!("Setting up Buffer Pool Manager implementations for benchmarking.");

    let db_file = "benchmark.db";
    let disk_manager_os_cache = match DiskManager::new(db_file, false) {
        Ok(dm) => Arc::new(dm),
        Err(e) => {
            eprintln!("Failed to create disk manager: {}", e);
            return;
        }
    };

    let disk_manager_direct_io = match DiskManager::new(db_file, true) {
        Ok(dm) => Arc::new(dm),
        Err(e) => {
            eprintln!("Failed to create disk manager: {}", e);
            return;
        }
    };

    let concurrent_bpm_os_cache = Arc::new(ConcurrentBufferPoolManager::new(100, disk_manager_os_cache.clone()));
    let actor_bpm_os_cache = Arc::new(ActorBufferPoolManager::new(100, disk_manager_os_cache.clone()));
    let concurrent_bpm_direct_io= Arc::new(ConcurrentBufferPoolManager::new(100, disk_manager_direct_io.clone()));
    let actor_bpm_direct_io= Arc::new(ActorBufferPoolManager::new(100, disk_manager_direct_io.clone()));

    let concurrent_impl_results = BenchmarkResult {
        concurrent_write: run_benchmark(concurrent_bpm_os_cache.clone(), BenchmarkType::Write),
        concurrent_read: run_benchmark(concurrent_bpm_os_cache.clone(), BenchmarkType::Read),
    };

    let actor_impl_results = BenchmarkResult {
        concurrent_write: run_benchmark(actor_bpm_os_cache.clone(), BenchmarkType::Write),
        concurrent_read: run_benchmark(actor_bpm_os_cache.clone(), BenchmarkType::Read),
    };

    let concurrent_impl_results_direct_io = BenchmarkResult {
        concurrent_write: run_benchmark(concurrent_bpm_direct_io.clone(), BenchmarkType::Write),
        concurrent_read: run_benchmark(concurrent_bpm_direct_io.clone(), BenchmarkType::Read),
    };

    let actor_impl_results_direct_io = BenchmarkResult {
        concurrent_write: run_benchmark(actor_bpm_direct_io.clone(), BenchmarkType::Write),
        concurrent_read: run_benchmark(actor_bpm_direct_io.clone(), BenchmarkType::Read),
    };

    println!("\n--- Benchmark Results ---");
    println!("| Implementation             | Direct I/O | Write Time      | Read Time       |");
    println!("|----------------------------|------------|-----------------|-----------------|");
    println!("| ConcurrentBufferPoolManager| {:<10?} | {:<15?} | {:<15?} |", true, concurrent_impl_results_direct_io.concurrent_write, concurrent_impl_results_direct_io.concurrent_read);
    println!("| ConcurrentBufferPoolManager| {:<10?} | {:<15?} | {:<15?} |", false, concurrent_impl_results.concurrent_write, concurrent_impl_results.concurrent_read);
    println!("| ActorBufferPoolManager     | {:<10?} | {:<15?} | {:<15?} |", true, actor_impl_results_direct_io.concurrent_write, actor_impl_results_direct_io.concurrent_read);
    println!("| ActorBufferPoolManager     | {:<10?} | {:<15?} | {:<15?} |", false, actor_impl_results.concurrent_write, actor_impl_results.concurrent_read);

    std::fs::remove_file(db_file).unwrap();
}

fn run_benchmark(bpm: Arc<dyn BufferPoolManager>, benchmark_type: BenchmarkType) -> Duration {
    const NUM_PAGES: usize = 1000;

    match benchmark_type {
        BenchmarkType::Write => {
            let start = Instant::now();
            for _ in 0..NUM_PAGES {
                if let Err(e) = bpm.new_page() {
                    eprintln!("Failed to create new page: {:?}", e);
                    return Duration::ZERO;
                }
            }
            start.elapsed()
        }
        BenchmarkType::Read => {
            let mut page_ids = Vec::new();
            for _ in 0..NUM_PAGES {
                match bpm.new_page() {
                    Ok(guard) => {
                        page_ids.push(guard.page_id());
                    }
                    Err(e) => {
                        eprintln!("Failed to create new page: {:?}", e);
                        return Duration::ZERO;
                    }
                }
            }
            bpm.flush_all_pages().unwrap();

            let start = Instant::now();
            for &page_id in &page_ids {
                if let Err(e) = bpm.fetch_page(page_id) {
                    eprintln!("Failed to fetch page {}: {:?}", page_id, e);
                }
            }
            start.elapsed()
        }
    }
}
