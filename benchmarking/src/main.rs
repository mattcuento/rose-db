
use std::sync::Arc;
use std::time::{Duration, Instant};
use actor_buffer_pool_manager::ActorBufferPoolManager;
use common::api::BufferPoolManager;
use common::disk_manager::DiskManager;
use concurrent_buffer_pool_manager::ConcurrentBufferPoolManager;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Enable direct I/O
    #[arg(short, long)]
    direct_io: bool,
}

enum BenchmarkType {
    Read,
    Write,
}

struct BenchmarkResult {
    concurrent_write: Duration,
    concurrent_read: Duration,
}

fn main() {
    let args = Args::parse();
    println!("Setting up Buffer Pool Manager implementations for benchmarking.");
    println!("Direct I/O enabled: {}", args.direct_io);

    let db_file = "benchmark.db";
    let disk_manager = match DiskManager::new(db_file, args.direct_io) {
        Ok(dm) => Arc::new(dm),
        Err(e) => {
            eprintln!("Failed to create disk manager: {}", e);
            return;
        }
    };

    let concurrent_bpm = Arc::new(ConcurrentBufferPoolManager::new(100, disk_manager.clone()));
    let actor_bpm = Arc::new(ActorBufferPoolManager::new(100, disk_manager.clone()));

    let concurrent_impl_results = BenchmarkResult {
        concurrent_write: run_benchmark(concurrent_bpm.clone(), BenchmarkType::Write),
        concurrent_read: run_benchmark(concurrent_bpm, BenchmarkType::Read),
    };

    let actor_impl_results = BenchmarkResult {
        concurrent_write: run_benchmark(actor_bpm.clone(), BenchmarkType::Write),
        concurrent_read: run_benchmark(actor_bpm, BenchmarkType::Read),
    };

    println!("\n--- Benchmark Results ---");
    println!("| Implementation              | Write Time      | Read Time       |");
    println!("|-----------------------------|-----------------|-----------------|");
    println!("| ConcurrentBufferPoolManager | {:<15?} | {:<15?} |", concurrent_impl_results.concurrent_write, concurrent_impl_results.concurrent_read);
    println!("| ActorBufferPoolManager      | {:<15?} | {:<15?} |", actor_impl_results.concurrent_write, actor_impl_results.concurrent_read);

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
