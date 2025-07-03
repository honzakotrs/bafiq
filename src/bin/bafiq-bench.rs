use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::time::Instant;

use anyhow::Result;
use bafiq::{benchmark, FlagIndex};

/// Clear OS file system caches to ensure cold start conditions
fn clear_file_system_cache() -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("purge").output();
        match output {
            Ok(result) => {
                if !result.status.success() {
                    if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
                        eprintln!("Warning: purge command failed, caches may not be cleared");
                    }
                }
            }
            Err(_) => {
                if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
                    eprintln!("Warning: purge command not available, caches may not be cleared");
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    {
        let _ = Command::new("sync").output();
        let output = Command::new("sh")
            .arg("-c")
            .arg("echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1")
            .output();

        if let Ok(result) = output {
            if !result.status.success() {
                if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
                    eprintln!(
                        "Warning: Could not clear Linux page cache (try running with sudo)"
                    );
                }
            }
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
            eprintln!("Warning: Cache clearing not implemented for this platform");
        }
    }

    Ok(())
}

/// Create a temporary copy of the BAM file
fn create_temp_bam_copy(original_path: &str) -> Result<PathBuf> {
    use std::fs;
    use std::io::Write;

    let temp_dir = std::env::temp_dir();
    let temp_file = temp_dir.join(format!("bafiq_bench_{}.bam", std::process::id()));

    if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
        println!("   Creating temp copy: {:?}", temp_file);
    }

    fs::copy(original_path, &temp_file)?;

    let mut file = fs::OpenOptions::new().write(true).open(&temp_file)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);

    Ok(temp_file)
}

/// Remove temporary file
fn cleanup_temp_file(temp_path: &PathBuf) {
    if let Err(e) = std::fs::remove_file(temp_path) {
        if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
            eprintln!(
                "Warning: Failed to cleanup temp file {:?}: {}",
                temp_path, e
            );
        }
    } else if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
        println!("   Cleaned up temp file: {:?}", temp_path);
    }
}

/// Run a cold start benchmark
fn run_benchmark<F>(
    name: &str,
    input_file: &str,
    mut benchmark_fn: F,
) -> Result<(std::time::Duration, u64)>
where
    F: FnMut(&str) -> Result<FlagIndex>,
{
    if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
        println!("🧊 Running cold start benchmark: {}", name);
    }

    // Clear caches
    clear_file_system_cache()?;

    // Create fresh copy
    let temp_file = create_temp_bam_copy(input_file)?;

    // Small delay
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Run benchmark
    let start = Instant::now();
    let index = benchmark_fn(temp_file.to_str().unwrap())?;
    let duration = start.elapsed();
    let total_records = index.total_records();

    // Cleanup
    cleanup_temp_file(&temp_file);

    Ok((duration, total_records))
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <input.bam>", args[0]);
        eprintln!("Example: {} /path/to/test.bam", args[0]);
        eprintln!();
        eprintln!("Environment variables:");
        eprintln!("  BAFIQ_BENCH_DEBUG=1    Enable detailed logging");
        std::process::exit(1);
    }

    let input_file = &args[1];

    println!("Flag Index Building Benchmark: {}", input_file);
    println!("🧊 Using cold start conditions (realistic first-time usage)");

    if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
        println!("Debug mode enabled - detailed logging active");
        println!("   Each test will clear caches and use fresh file copies");
    } else {
        println!("For detailed logging, set: BAFIQ_BENCH_DEBUG=1");
    }

    println!("{}", "=".repeat(80));

    // Run benchmarks
    println!("\nrust-htslib Flag Index Building:");
    let (rust_htslib_duration, rust_htslib_total) =
        run_benchmark("rust-htslib", input_file, |path| FlagIndex::from_path(path))?;
    println!("   Time: {:.3} seconds", rust_htslib_duration.as_secs_f64());
    println!("   Total records: {}", rust_htslib_total);

    println!("\nSequential Low-level Flag Index Building:");
    let (sequential_duration, sequential_total) =
        run_benchmark("sequential", input_file, |path| {
            benchmark::build_flag_index_low_level(path)
        })?;
    println!("   Time: {:.3} seconds", sequential_duration.as_secs_f64());
    println!("   Total records: {}", sequential_total);

    println!("\nStreaming Parallel Low-level Flag Index Building:");
    let (parallel_duration, parallel_total) =
        run_benchmark("streaming parallel", input_file, |path| {
            benchmark::build_flag_index_streaming_parallel(path)
        })?;
    println!("   Time: {:.3} seconds", parallel_duration.as_secs_f64());
    println!("   Total records: {}", parallel_total);

    // Performance comparison
    println!("\nPerformance Comparison:");
    println!(
        "   rust-htslib:        {:.3}s",
        rust_htslib_duration.as_secs_f64()
    );
    println!(
        "   Sequential:         {:.3}s",
        sequential_duration.as_secs_f64()
    );
    println!(
        "   Streaming Parallel: {:.3}s",
        parallel_duration.as_secs_f64()
    );

    // Verify correctness
    if rust_htslib_total == sequential_total && sequential_total == parallel_total {
        println!("   All record counts match: {}", rust_htslib_total);
    } else {
        println!("   Record count mismatch!");
        println!("      rust-htslib: {}", rust_htslib_total);
        println!("      Sequential:  {}", sequential_total);
        println!("      Streaming Parallel: {}", parallel_total);
    }

    // Calculate speedups
    println!("\nSpeedup Analysis:");

    if sequential_duration < rust_htslib_duration {
        let speedup = rust_htslib_duration.as_secs_f64() / sequential_duration.as_secs_f64();
        println!("   Sequential vs rust-htslib: {:.2}x faster", speedup);
    } else {
        let slowdown = sequential_duration.as_secs_f64() / rust_htslib_duration.as_secs_f64();
        println!("   Sequential vs rust-htslib: {:.2}x slower", slowdown);
    }

    if parallel_duration < rust_htslib_duration {
        let speedup = rust_htslib_duration.as_secs_f64() / parallel_duration.as_secs_f64();
        println!(
            "   Streaming Parallel vs rust-htslib: {:.2}x faster",
            speedup
        );

        if speedup >= 1.2 {
            println!("   🏆 Streaming Parallel SUCCESS: Meets 20% improvement goal!");
        } else {
            println!("   Streaming Parallel below 20% improvement threshold");
        }
    } else {
        println!("   📉 Streaming Parallel approach is slower than rust-htslib");
    }

    if parallel_duration < sequential_duration {
        let speedup = sequential_duration.as_secs_f64() / parallel_duration.as_secs_f64();
        println!(
            "   Streaming Parallel vs Sequential: {:.2}x faster",
            speedup
        );
        println!("   Parallelization effective!");
    } else {
        let slowdown = parallel_duration.as_secs_f64() / sequential_duration.as_secs_f64();
        println!(
            "   Streaming Parallel vs Sequential: {:.2}x slower",
            slowdown
        );
        println!("   Parallelization overhead detected");
    }

    Ok(())
}
