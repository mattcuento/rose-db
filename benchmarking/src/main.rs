
use std::sync::Arc;
use std::time::{Duration, Instant};
use common::api::BufferPoolManager;
use concurrent::ConcurrentBufferPoolManager;
use common::disk_manager::DiskManager;

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
    let disk_manager = match DiskManager::new(db_file) {
        Ok(dm) => Arc::new(dm),
        Err(e) => {
            eprintln!("Failed to create disk manager: {}", e);
            return;
        }
    };

    let concurrent_bpm = Arc::new(ConcurrentBufferPoolManager::new(100, disk_manager.clone()));

    let results = BenchmarkResult {
        concurrent_write: run_benchmark(concurrent_bpm.clone(), BenchmarkType::Write),
        concurrent_read: run_benchmark(concurrent_bpm, BenchmarkType::Read),
    };

    println!("\n--- Benchmark Results ---");
    println!("| Implementation              | Write Time      | Read Time       |");
    println!("|-----------------------------|-----------------|-----------------|");
    println!("| ConcurrentBufferPoolManager | {:<15?} | {:<15?} |", results.concurrent_write, results.concurrent_read);

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
