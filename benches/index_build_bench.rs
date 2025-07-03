use anyhow::Result;
use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, Criterion,
};
use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use bafiq::{benchmark, FlagIndex};

/// Clear OS file system caches to ensure cold start conditions
/// This is platform-specific and requires appropriate permissions
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
                    eprintln!("Warning: Could not clear Linux page cache (try running with sudo)");
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

/// Run samtools view -c as external baseline
fn run_samtools_count(input_file: &str) -> Result<u64> {
    let output = Command::new("samtools")
        .arg("view")
        .arg("-c")
        .arg(input_file)
        .output()
        .map_err(|e| anyhow::anyhow!("Failed to run samtools (is it installed?): {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("samtools failed: {}", stderr));
    }

    // Parse the record count from samtools output
    let stdout = String::from_utf8_lossy(&output.stdout);
    let total_records = stdout.trim().parse::<u64>().map_err(|e| {
        anyhow::anyhow!("Failed to parse samtools output '{}': {}", stdout.trim(), e)
    })?;

    Ok(total_records)
}

/// Simple samtools benchmark with cold start conditions
fn run_simple_samtools_benchmark(test_bam: &str) -> Result<(Duration, u64)> {
    println!("🧊 Running cold start benchmark: samtools view -c");

    // Clear caches
    clear_file_system_cache()?;

    // Create fresh copy
    let temp_file = create_temp_bam_copy(test_bam)?;

    // Small delay to ensure cache clearing takes effect
    std::thread::sleep(Duration::from_millis(100));

    // Run samtools
    let start = Instant::now();
    let total_records = run_samtools_count(temp_file.to_str().unwrap())?;
    let duration = start.elapsed();

    // Cleanup
    cleanup_temp_file(&temp_file);

    println!(
        "   Time: {:.3}s, Records: {}",
        duration.as_secs_f64(),
        total_records
    );

    Ok((duration, total_records))
}

/// Create a temporary copy of the BAM file to avoid any caching effects
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

/// Calculate the serialized size of an index in bytes
fn calculate_index_size(index: &FlagIndex) -> Result<usize> {
    use bincode::serialize;
    let serialized = serialize(index)?;
    Ok(serialized.len())
}

/// Get the size of a file in bytes
fn get_file_size(path: &str) -> Result<u64> {
    let metadata = std::fs::metadata(path)?;
    Ok(metadata.len())
}

/// Format file size in human-readable form
fn format_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB"];
    const THRESHOLD: f64 = 1024.0;

    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= THRESHOLD && unit_index < UNITS.len() - 1 {
        size /= THRESHOLD;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

/// Simple direct timing benchmark that returns both timing and the index (for verification)
fn run_simple_benchmark_with_index<F>(
    name: &str,
    test_bam: &str,
    mut benchmark_fn: F,
) -> Result<(Duration, FlagIndex)>
where
    F: FnMut(&str) -> Result<FlagIndex>,
{
    println!("🧊 Running cold start benchmark: {}", name);

    // Clear caches
    clear_file_system_cache()?;

    // Create fresh copy
    let temp_file = create_temp_bam_copy(test_bam)?;

    // Small delay to ensure cache clearing takes effect
    std::thread::sleep(Duration::from_millis(100));

    // Run benchmark
    let start = Instant::now();
    let index = benchmark_fn(temp_file.to_str().unwrap())?;
    let duration = start.elapsed();
    let total_records = index.total_records();

    // Calculate index size
    let index_size = calculate_index_size(&index)?;

    // Cleanup
    cleanup_temp_file(&temp_file);

    println!(
        "   Time: {:.3}s, Records: {}, Index: {}",
        duration.as_secs_f64(),
        total_records,
        format_size(index_size as u64)
    );

    Ok((duration, index))
}

/// Verify that all flag indexes are equivalent
fn verify_indexes_equivalent(indexes: &[(&str, &FlagIndex)]) -> Result<bool> {
    if indexes.len() < 2 {
        return Ok(true);
    }

    let (reference_name, reference_index) = indexes[0];
    let mut all_equivalent = true;

    for (name, index) in &indexes[1..] {
        println!("   {} vs {}:", reference_name, name);

        if reference_index.is_equivalent_to(index) {
            // The is_equivalent_to method now handles its own output
        } else {
            all_equivalent = false;
        }
        println!(); // Add spacing between comparisons
    }

    Ok(all_equivalent)
}

/// Simple benchmarks mode (fast development)
fn simple_benchmarks() -> Result<()> {
    let test_bam = match env::var("BAFIQ_TEST_BAM") {
        Ok(path) => path,
        Err(_) => {
            eprintln!("BAFIQ_TEST_BAM environment variable not set");
            eprintln!("   Example: export BAFIQ_TEST_BAM=/path/to/test.bam");
            return Ok(());
        }
    };

    println!("Simple Benchmarking with file: {}", test_bam);
    println!("Fast development mode (single cold start run per method)");

    // Get original BAM file size for comparison
    let bam_size = get_file_size(&test_bam)?;
    println!("Original BAM size: {}", format_size(bam_size));
    println!("{}", "=".repeat(80));

    // Run each benchmark once and collect both timing and index results
    let (rust_htslib_duration, rust_htslib_index) =
        run_simple_benchmark_with_index("rust-htslib", &test_bam, |path| {
            FlagIndex::from_path(path)
        })?;

    let (parallel_duration, parallel_index) =
        run_simple_benchmark_with_index("parallel_low_level", &test_bam, |path| {
            benchmark::build_flag_index_parallel(path)
        })?;

    let (streaming_duration, streaming_index) =
        run_simple_benchmark_with_index("streaming_parallel", &test_bam, |path| {
            benchmark::build_flag_index_streaming_parallel(path)
        })?;

    let (chunk_streaming_duration, chunk_streaming_index) =
        run_simple_benchmark_with_index("chunk_streaming", &test_bam, |path| {
            use bafiq::{BuildStrategy, IndexBuilder};
            let builder = IndexBuilder::with_strategy(BuildStrategy::ChunkStreaming);
            builder.build(path)
        })?;

    let (parallel_chunk_streaming_duration, parallel_chunk_streaming_index) =
        run_simple_benchmark_with_index("parallel_chunk_streaming", &test_bam, |path| {
            use bafiq::{BuildStrategy, IndexBuilder};
            let builder = IndexBuilder::with_strategy(BuildStrategy::ParallelChunkStreaming);
            builder.build(path)
        })?;

    let (optimized_duration, optimized_index) =
        run_simple_benchmark_with_index("optimized", &test_bam, |path| {
            use bafiq::{BuildStrategy, IndexBuilder};
            let builder = IndexBuilder::with_strategy(BuildStrategy::Optimized);
            builder.build(path)
        })?;

    let (rayon_optimized_duration, rayon_optimized_index) =
        run_simple_benchmark_with_index("rayon_optimized", &test_bam, |path| {
            use bafiq::{BuildStrategy, IndexBuilder};
            let builder = IndexBuilder::with_strategy(BuildStrategy::RayonOptimized);
            builder.build(path)
        })?;

    let (rayon_streaming_optimized_duration, rayon_streaming_optimized_index) =
        run_simple_benchmark_with_index("rayon_streaming_optimized", &test_bam, |path| {
            use bafiq::{BuildStrategy, IndexBuilder};
            let builder = IndexBuilder::with_strategy(BuildStrategy::RayonStreamingOptimized);
            builder.build(path)
        })?;

    let (sequential_duration, sequential_index) =
        run_simple_benchmark_with_index("sequential", &test_bam, |path| {
            use bafiq::{BuildStrategy, IndexBuilder};
            let builder = IndexBuilder::with_strategy(BuildStrategy::Sequential);
            builder.build(path)
        })?;

    // Run samtools benchmark
    let samtools_result = run_simple_samtools_benchmark(&test_bam)?;

    // Summary
    println!("\nPerformance Summary:");
    println!(
        "   rust-htslib:        {:.3}s",
        rust_htslib_duration.as_secs_f64()
    );
    println!(
        "   parallel:           {:.3}s",
        parallel_duration.as_secs_f64()
    );
    println!(
        "   streaming_parallel: {:.3}s",
        streaming_duration.as_secs_f64()
    );
    println!(
        "   chunk_streaming:    {:.3}s",
        chunk_streaming_duration.as_secs_f64()
    );
    println!(
        "   parallel_chunk_streaming: {:.3}s",
        parallel_chunk_streaming_duration.as_secs_f64()
    );
    println!(
        "   optimized:          {:.3}s",
        optimized_duration.as_secs_f64()
    );
    println!(
        "   rayon_optimized:    {:.3}s",
        rayon_optimized_duration.as_secs_f64()
    );
    println!(
        "   rayon_streaming_optimized: {:.3}s",
        rayon_streaming_optimized_duration.as_secs_f64()
    );
    println!(
        "   sequential:         {:.3}s",
        sequential_duration.as_secs_f64()
    );
    println!(
        "   samtools:           {:.3}s",
        samtools_result.0.as_secs_f64()
    );

    // Index size analysis
    println!("\n💾 Index Size Analysis:");
    let rust_htslib_size = calculate_index_size(&rust_htslib_index)?;
    let parallel_size = calculate_index_size(&parallel_index)?;
    let streaming_size = calculate_index_size(&streaming_index)?;
    let chunk_streaming_size = calculate_index_size(&chunk_streaming_index)?;
    let parallel_chunk_streaming_size = calculate_index_size(&parallel_chunk_streaming_index)?;
    let optimized_size = calculate_index_size(&optimized_index)?;
    let rayon_optimized_size = calculate_index_size(&rayon_optimized_index)?;
    let rayon_streaming_optimized_size = calculate_index_size(&rayon_streaming_optimized_index)?;
    let sequential_size = calculate_index_size(&sequential_index)?;

    let rust_percentage = (rust_htslib_size as f64 / bam_size as f64) * 100.0;
    let parallel_percentage = (parallel_size as f64 / bam_size as f64) * 100.0;
    let streaming_percentage = (streaming_size as f64 / bam_size as f64) * 100.0;
    let chunk_streaming_percentage = (chunk_streaming_size as f64 / bam_size as f64) * 100.0;
    let parallel_chunk_streaming_percentage =
        (parallel_chunk_streaming_size as f64 / bam_size as f64) * 100.0;
    let optimized_percentage = (optimized_size as f64 / bam_size as f64) * 100.0;
    let rayon_optimized_percentage = (rayon_optimized_size as f64 / bam_size as f64) * 100.0;
    let rayon_streaming_optimized_percentage =
        (rayon_streaming_optimized_size as f64 / bam_size as f64) * 100.0;
    let sequential_percentage = (sequential_size as f64 / bam_size as f64) * 100.0;

    println!(
        "   rust-htslib:        {} ({:.2}% of BAM)",
        format_size(rust_htslib_size as u64),
        rust_percentage
    );
    println!(
        "   parallel:           {} ({:.2}% of BAM)",
        format_size(parallel_size as u64),
        parallel_percentage
    );
    println!(
        "   streaming_parallel: {} ({:.2}% of BAM)",
        format_size(streaming_size as u64),
        streaming_percentage
    );
    println!(
        "   chunk_streaming:    {} ({:.2}% of BAM)",
        format_size(chunk_streaming_size as u64),
        chunk_streaming_percentage
    );
    println!(
        "   parallel_chunk_streaming: {} ({:.2}% of BAM)",
        format_size(parallel_chunk_streaming_size as u64),
        parallel_chunk_streaming_percentage
    );
    println!(
        "   optimized:          {} ({:.2}% of BAM)",
        format_size(optimized_size as u64),
        optimized_percentage
    );
    println!(
        "   rayon_optimized:    {} ({:.2}% of BAM)",
        format_size(rayon_optimized_size as u64),
        rayon_optimized_percentage
    );
    println!(
        "   rayon_streaming_optimized: {} ({:.2}% of BAM)",
        format_size(rayon_streaming_optimized_size as u64),
        rayon_streaming_optimized_percentage
    );
    println!(
        "   sequential:         {} ({:.2}% of BAM)",
        format_size(sequential_size as u64),
        sequential_percentage
    );

    // Check if all indexes have the same size (they should, since they're functionally equivalent)
    if rust_htslib_size == parallel_size
        && parallel_size == streaming_size
        && streaming_size == chunk_streaming_size
        && chunk_streaming_size == parallel_chunk_streaming_size
        && parallel_chunk_streaming_size == optimized_size
        && optimized_size == rayon_optimized_size
        && rayon_optimized_size == rayon_streaming_optimized_size
        && rayon_streaming_optimized_size == sequential_size
    {
        println!("   All indexes have identical size");
    } else {
        println!("   Index sizes differ (unexpected for equivalent indexes)");
    }

    // Comprehensive index verification
    println!("\nIndex Verification:");
    let rust_total = rust_htslib_index.total_records();
    let parallel_total = parallel_index.total_records();
    let streaming_total = streaming_index.total_records();
    let chunk_streaming_total = chunk_streaming_index.total_records();
    let parallel_chunk_streaming_total = parallel_chunk_streaming_index.total_records();
    let optimized_total = optimized_index.total_records();
    let rayon_optimized_total = rayon_optimized_index.total_records();
    let rayon_streaming_optimized_total = rayon_streaming_optimized_index.total_records();
    let sequential_total = sequential_index.total_records();
    let samtools_total = samtools_result.1;

    if rust_total == parallel_total
        && parallel_total == streaming_total
        && streaming_total == chunk_streaming_total
        && chunk_streaming_total == parallel_chunk_streaming_total
        && parallel_chunk_streaming_total == optimized_total
        && optimized_total == rayon_optimized_total
        && rayon_optimized_total == rayon_streaming_optimized_total
        && rayon_streaming_optimized_total == sequential_total
        && sequential_total == samtools_total
    {
        println!("   All record counts match: {}", rust_total);
        println!("   samtools verification: {} records", samtools_total);
    } else {
        println!("   Record count mismatch!");
        println!("      rust-htslib:              {}", rust_total);
        println!("      parallel:                 {}", parallel_total);
        println!("      streaming:                {}", streaming_total);
        println!("      chunk_streaming:          {}", chunk_streaming_total);
        println!(
            "      parallel_chunk_streaming: {}",
            parallel_chunk_streaming_total
        );
        println!("      optimized:                {}", optimized_total);
        println!("      rayon_optimized:          {}", rayon_optimized_total);
        println!(
            "      rayon_streaming_optimized: {}",
            rayon_streaming_optimized_total
        );
        println!("      sequential:               {}", sequential_total);
        println!("      samtools:                 {}", samtools_total);
        return Err(anyhow::anyhow!(
            "Index verification failed: record count mismatch"
        ));
    }

    // Verify index contents are functionally equivalent
    let verification_passed = verify_indexes_equivalent(&[
        ("rust-htslib", &rust_htslib_index),
        ("parallel", &parallel_index),
        ("streaming", &streaming_index),
        ("chunk_streaming", &chunk_streaming_index),
        ("parallel_chunk_streaming", &parallel_chunk_streaming_index),
        ("optimized", &optimized_index),
        ("rayon_optimized", &rayon_optimized_index),
        (
            "rayon_streaming_optimized",
            &rayon_streaming_optimized_index,
        ),
        ("sequential", &sequential_index),
    ])?;

    if verification_passed {
        println!("All indexes are functionally equivalent");
    } else {
        return Err(anyhow::anyhow!(
            "Index verification failed: indexes are not functionally equivalent"
        ));
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

    let best_parallel = parallel_duration
        .min(streaming_duration)
        .min(chunk_streaming_duration)
        .min(parallel_chunk_streaming_duration)
        .min(optimized_duration)
        .min(rayon_optimized_duration)
        .min(rayon_streaming_optimized_duration)
        .min(sequential_duration);

    if best_parallel < rust_htslib_duration {
        let speedup = rust_htslib_duration.as_secs_f64() / best_parallel.as_secs_f64();
        let best_name = if parallel_duration == best_parallel {
            "parallel"
        } else if streaming_duration == best_parallel {
            "streaming"
        } else if chunk_streaming_duration == best_parallel {
            "chunk_streaming"
        } else if parallel_chunk_streaming_duration == best_parallel {
            "parallel_chunk_streaming"
        } else if optimized_duration == best_parallel {
            "optimized"
        } else if rayon_optimized_duration == best_parallel {
            "rayon_optimized"
        } else if rayon_streaming_optimized_duration == best_parallel {
            "rayon_streaming_optimized"
        } else {
            "sequential"
        };
        println!(
            "   Best parallel ({}) vs rust-htslib: {:.2}x faster",
            best_name, speedup
        );

        if speedup >= 1.2 {
            println!("   🏆 SUCCESS: Meets 20% improvement goal!");
        } else {
            println!("   Below 20% improvement threshold");
        }
    }

    // Samtools comparison
    println!("\nExternal Baseline Comparison:");
    let samtools_duration = samtools_result.0;
    if best_parallel < samtools_duration {
        let speedup = samtools_duration.as_secs_f64() / best_parallel.as_secs_f64();
        let best_name = if parallel_duration == best_parallel {
            "parallel"
        } else if streaming_duration == best_parallel {
            "streaming"
        } else if chunk_streaming_duration == best_parallel {
            "chunk_streaming"
        } else if parallel_chunk_streaming_duration == best_parallel {
            "parallel_chunk_streaming"
        } else if optimized_duration == best_parallel {
            "optimized"
        } else if rayon_optimized_duration == best_parallel {
            "rayon_optimized"
        } else if rayon_streaming_optimized_duration == best_parallel {
            "rayon_streaming_optimized"
        } else {
            "sequential"
        };
        println!(
            "   Best bafiq ({}) vs samtools: {:.2}x faster",
            best_name, speedup
        );

        if speedup >= 2.0 {
            println!("   🏆 EXCELLENT: Beats samtools by 2x+ target!");
        } else if speedup >= 1.5 {
            println!("   GOOD: Significant improvement over samtools");
        } else {
            println!("   Moderate improvement over samtools");
        }
    } else {
        let slowdown = best_parallel.as_secs_f64() / samtools_duration.as_secs_f64();
        println!("   Best bafiq vs samtools: {:.2}x slower", slowdown);
        println!("   NEEDS WORK: samtools is faster");
    }

    println!("\nFor detailed Criterion analysis, run: BAFIQ_USE_CRITERION=1 cargo bench");

    Ok(())
}

/// Cold start benchmark that ensures no caching between measurements (Criterion mode)
fn benchmark_cold_start<F>(
    group: &mut BenchmarkGroup<WallTime>,
    name: &str,
    test_bam: &str,
    mut benchmark_fn: F,
) where
    F: FnMut(&str) -> u64,
{
    if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
        println!("🧊 Setting up cold start benchmark: {}", name);
    }

    group.bench_function(name, |b| {
        b.iter_custom(|iters| {
            let mut total_time = Duration::new(0, 0);

            for i in 0..iters {
                if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
                    println!("   Iteration {}/{} for {}", i + 1, iters, name);
                }

                // Clear caches before each iteration
                if let Err(e) = clear_file_system_cache() {
                    if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
                        eprintln!("Cache clearing failed: {}", e);
                    }
                }

                // Create a fresh copy of the file to avoid any file handle caching
                let temp_file =
                    create_temp_bam_copy(test_bam).expect("Failed to create temporary BAM file");

                // Small delay to ensure cache clearing takes effect
                std::thread::sleep(Duration::from_millis(100));

                // Run the actual benchmark
                let start = std::time::Instant::now();
                let _result = benchmark_fn(temp_file.to_str().unwrap());
                let elapsed = start.elapsed();

                if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
                    println!(
                        "     Iteration {} took: {:.3}s",
                        i + 1,
                        elapsed.as_secs_f64()
                    );
                }

                total_time += elapsed;

                // Cleanup
                cleanup_temp_file(&temp_file);

                // Force any remaining buffers to be released
                std::thread::sleep(Duration::from_millis(50));
            }

            total_time
        });
    });
}

/// Performance benchmarks using Criterion (detailed analysis mode)
fn criterion_benchmarks(c: &mut Criterion) {
    // Get the test BAM file path from environment variable
    let test_bam = match env::var("BAFIQ_TEST_BAM") {
        Ok(path) => path,
        Err(_) => {
            eprintln!("BAFIQ_TEST_BAM environment variable not set");
            eprintln!(
                "   Skipping benchmarks. Set this variable to a BAM file path to run benchmarks."
            );
            eprintln!("   Example: export BAFIQ_TEST_BAM=/path/to/test.bam");
            return;
        }
    };

    println!("Criterion Benchmarking with file: {}", test_bam);
    println!("🧊 Using cold start conditions via cache clearing (3 samples for fast development)");

    if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
        println!("Debug mode enabled");
        println!("   Each benchmark iteration will:");
        println!("   1. Clear OS file system caches");
        println!("   2. Create a fresh copy of the BAM file");
        println!("   3. Run the benchmark on the fresh copy");
        println!("   4. Clean up the temporary file");
    } else {
        println!("Set BAFIQ_BENCH_DEBUG=1 for detailed logging");
    }

    let mut group = c.benchmark_group("flag_index_building");

    // Configure for cold start benchmarking (fast development mode)
    group
        .sample_size(10) // Minimum samples required by Criterion
        .measurement_time(Duration::from_secs(30)) // Reasonable time for 10 samples
        .warm_up_time(Duration::from_millis(100)); // Minimal warmup to satisfy Criterion

    // Benchmark the rust-htslib approach (building FlagIndex)
    benchmark_cold_start(&mut group, "rust_htslib", &test_bam, |file_path| {
        let index =
            FlagIndex::from_path(file_path).expect("Failed to build index with rust-htslib");
        index.total_records()
    });

    // Benchmark the sequential low-level approach (building FlagIndex)
    benchmark_cold_start(&mut group, "sequential_low_level", &test_bam, |file_path| {
        let index = benchmark::build_flag_index_low_level(file_path)
            .expect("Failed to build index with sequential low-level approach");
        index.total_records()
    });

    // Benchmark the parallel low-level approach (building FlagIndex)
    benchmark_cold_start(&mut group, "parallel_low_level", &test_bam, |file_path| {
        let index = benchmark::build_flag_index_parallel(file_path)
            .expect("Failed to build index with parallel low-level approach");
        index.total_records()
    });

    // Benchmark the streaming parallel approach
    benchmark_cold_start(&mut group, "streaming_parallel", &test_bam, |file_path| {
        let index = benchmark::build_flag_index_streaming_parallel(file_path)
            .expect("Failed to build index with streaming parallel approach");
        index.total_records()
    });

    // Benchmark the chunk streaming approach
    benchmark_cold_start(&mut group, "chunk_streaming", &test_bam, |file_path| {
        use bafiq::{BuildStrategy, IndexBuilder};
        let builder = IndexBuilder::with_strategy(BuildStrategy::ChunkStreaming);
        let index = builder
            .build(file_path)
            .expect("Failed to build index with chunk streaming approach");
        index.total_records()
    });

    // Benchmark the parallel chunk streaming approach (new default)
    benchmark_cold_start(
        &mut group,
        "parallel_chunk_streaming",
        &test_bam,
        |file_path| {
            use bafiq::{BuildStrategy, IndexBuilder};
            let builder = IndexBuilder::with_strategy(BuildStrategy::ParallelChunkStreaming);
            let index = builder
                .build(file_path)
                .expect("Failed to build index with parallel chunk streaming approach");
            index.total_records()
        },
    );

    // Benchmark the optimized approach (crossbeam-channel + rayon)
    benchmark_cold_start(&mut group, "optimized", &test_bam, |file_path| {
        use bafiq::{BuildStrategy, IndexBuilder};
        let builder = IndexBuilder::with_strategy(BuildStrategy::Optimized);
        let index = builder
            .build(file_path)
            .expect("Failed to build index with optimized approach");
        index.total_records()
    });

    // Benchmark the Rayon-optimized approach (current default)
    benchmark_cold_start(&mut group, "rayon_optimized", &test_bam, |file_path| {
        use bafiq::{BuildStrategy, IndexBuilder};
        let builder = IndexBuilder::with_strategy(BuildStrategy::RayonOptimized);
        let index = builder
            .build(file_path)
            .expect("Failed to build index with Rayon-optimized approach");
        index.total_records()
    });

    // Benchmark the new RayonStreamingOptimized approach (streaming evolution)
    benchmark_cold_start(
        &mut group,
        "rayon_streaming_optimized",
        &test_bam,
        |file_path| {
            use bafiq::{BuildStrategy, IndexBuilder};
            let builder = IndexBuilder::with_strategy(BuildStrategy::RayonStreamingOptimized);
            let index = builder
                .build(file_path)
                .expect("Failed to build index with RayonStreamingOptimized approach");
            index.total_records()
        },
    );

    // Benchmark the sequential approach (single-threaded baseline)
    benchmark_cold_start(&mut group, "sequential", &test_bam, |file_path| {
        use bafiq::{BuildStrategy, IndexBuilder};
        let builder = IndexBuilder::with_strategy(BuildStrategy::Sequential);
        let index = builder
            .build(file_path)
            .expect("Failed to build index with sequential approach");
        index.total_records()
    });

    group.finish();

    // Benchmark external tools in separate group
    let mut external_group = c.benchmark_group("external_baselines");
    external_group
        .sample_size(10)
        .measurement_time(Duration::from_secs(30))
        .warm_up_time(Duration::from_millis(100));

    // Benchmark samtools view -c
    benchmark_cold_start(
        &mut external_group,
        "samtools_view_c",
        &test_bam,
        |file_path| run_samtools_count(file_path).expect("Failed to run samtools view -c"),
    );

    external_group.finish();

    println!("🧊 Criterion benchmarking completed");

    if !env::var("BAFIQ_BENCH_DEBUG").is_ok() {
        println!("For detailed logging, run: BAFIQ_BENCH_DEBUG=1 cargo bench");
    }
}

/// Performance benchmarks entry point that chooses between simple and Criterion modes
fn performance_benchmarks(c: &mut Criterion) {
    // Check if we should use simple mode for fast development
    if env::var("BAFIQ_USE_CRITERION").is_err() {
        // Use simple direct timing for fast development
        println!("Using simple timing for fast development");
        println!("For detailed Criterion analysis, run: BAFIQ_USE_CRITERION=1 cargo bench");

        if let Err(e) = simple_benchmarks() {
            eprintln!("Benchmark failed: {}", e);
            std::process::exit(1);
        }
        return;
    }

    // Use Criterion for detailed benchmarking
    println!("Using Criterion for detailed statistical analysis");
    criterion_benchmarks(c);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .measurement_time(std::time::Duration::from_secs(30))
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_millis(100));
    targets = performance_benchmarks
}

criterion_main!(benches);
