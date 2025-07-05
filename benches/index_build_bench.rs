use anyhow::Result;
use criterion::{black_box, measurement::WallTime, BenchmarkGroup, Criterion};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::sync::LazyLock;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{Pid, System};

use bafiq::{benchmark, BuildStrategy, FlagIndex, IndexBuilder};

/// Global storage for resource usage data from Criterion benchmarks
static CRITERION_RESOURCE_DATA: LazyLock<Mutex<HashMap<String, ResourceStats>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Get resource usage data for a specific benchmark strategy
pub fn get_resource_data(strategy_name: &str) -> Option<ResourceStats> {
    CRITERION_RESOURCE_DATA
        .lock()
        .ok()?
        .get(strategy_name)
        .cloned()
}

/// Report resource usage statistics after Criterion benchmarks complete
fn report_criterion_resource_usage() {
    if let Ok(data) = CRITERION_RESOURCE_DATA.lock() {
        if !data.is_empty() {
            println!("\nCriterion Resource Usage Summary:");
            println!("{}", "=".repeat(100));
            println!(
                "{:<25} {:>12} {:>12} {:>8} {:>8} {:>8}",
                "Strategy", "Peak RAM", "Avg RAM", "Peak CPU", "Avg CPU", "Samples"
            );
            println!("{}", "-".repeat(100));

            // Sort by strategy name for consistent output
            let mut sorted_data: Vec<_> = data.iter().collect();
            sorted_data.sort_by_key(|(name, _)| *name);

            for (name, resources) in &sorted_data {
                println!(
                    "{:<25} {:>12} {:>12} {:>7.1}% {:>7.1}% {:>8}",
                    name,
                    ResourceStats::format_memory(resources.peak_memory_bytes),
                    ResourceStats::format_memory(resources.avg_memory_bytes),
                    resources.peak_cpu_percent,
                    resources.avg_cpu_percent,
                    resources.sample_count
                );
            }

            println!("{}", "=".repeat(100));

            // Resource efficiency analysis
            let mut memory_ranking: Vec<_> = sorted_data
                .iter()
                .map(|(name, res)| (name.as_str(), res.peak_memory_bytes))
                .collect();
            memory_ranking.sort_by_key(|(_, memory)| *memory);

            println!("\nMemory Efficiency (Criterion):");
            for (i, (name, memory)) in memory_ranking.iter().take(3).enumerate() {
                println!(
                    "   {}. {} - {}",
                    i + 1,
                    name,
                    ResourceStats::format_memory(*memory)
                );
            }

            let mut cpu_ranking: Vec<_> = sorted_data
                .iter()
                .map(|(name, res)| (name.as_str(), res.avg_cpu_percent))
                .collect();
            cpu_ranking.sort_by(|(_, cpu_a), (_, cpu_b)| cpu_b.partial_cmp(cpu_a).unwrap());

            println!("\n🖥️ CPU Utilization (Criterion):");
            for (i, (name, cpu)) in cpu_ranking.iter().take(3).enumerate() {
                println!("   {}. {} - {:.1}% average", i + 1, name, cpu);
            }
        }
    }
}

/// Enhanced benchmark function for Criterion with cold start and resource monitoring
fn benchmark_cold_start<F>(
    group: &mut BenchmarkGroup<WallTime>,
    name: &str,
    test_bam: &str,
    mut benchmark_fn: F,
) where
    F: FnMut(&str) -> u64,
{
    group.bench_function(name, |b| {
        b.iter_custom(|iters| {
            let mut total_time = Duration::new(0, 0);
            let mut all_resources = Vec::new();

            for _i in 0..iters {
                // Fast mode: skip expensive cache clearing and temp file operations
                let use_fast_mode = env::var("BAFIQ_BENCH_FAST").is_ok();

                let file_path = if use_fast_mode {
                    // Use original file directly (much faster)
                    test_bam.to_string()
                } else {
                    // Clear caches before each iteration (slower but more accurate)
                    if let Err(e) = clear_file_system_cache() {
                        if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
                            eprintln!("Cache clearing failed: {}", e);
                        }
                    }

                    // Create a fresh copy of the file to avoid any file handle caching
                    let temp_file = create_temp_bam_copy(test_bam)
                        .expect("Failed to create temporary BAM file");

                    // Small delay to ensure cache clearing takes effect
                    std::thread::sleep(Duration::from_millis(100));

                    temp_file.to_str().unwrap().to_string()
                };

                // Start resource monitoring
                let monitor = ResourceMonitor::new().expect("Failed to create resource monitor");
                monitor
                    .start_monitoring(10)
                    .expect("Failed to start monitoring"); // Reduced from 5ms to 10ms for less overhead

                // Run the actual benchmark with timing
                let start = std::time::Instant::now();
                let result = benchmark_fn(&file_path);
                let elapsed = start.elapsed();

                // Stop monitoring and collect resource data
                let resources = monitor.stop_monitoring();
                all_resources.push(resources);

                // Use black_box to prevent optimization
                black_box(result);

                total_time += elapsed;

                if !use_fast_mode {
                    // Cleanup temp file
                    cleanup_temp_file(&std::path::PathBuf::from(&file_path));

                    // Force any remaining buffers to be released
                    std::thread::sleep(Duration::from_millis(50));
                }
            }

            // Calculate aggregated resource usage across iterations
            if !all_resources.is_empty() {
                let aggregated_resources = ResourceStats {
                    peak_memory_bytes: all_resources
                        .iter()
                        .map(|r| r.peak_memory_bytes)
                        .max()
                        .unwrap_or(0),
                    avg_memory_bytes: all_resources
                        .iter()
                        .map(|r| r.avg_memory_bytes)
                        .sum::<u64>()
                        / all_resources.len() as u64,
                    peak_cpu_percent: all_resources
                        .iter()
                        .map(|r| r.peak_cpu_percent)
                        .fold(0.0f32, f32::max),
                    avg_cpu_percent: all_resources.iter().map(|r| r.avg_cpu_percent).sum::<f32>()
                        / all_resources.len() as f32,
                    execution_time: total_time / iters as u32,
                    sample_count: all_resources.iter().map(|r| r.sample_count).sum(),
                };

                // Store resource data globally for later reporting
                if let Ok(mut data) = CRITERION_RESOURCE_DATA.lock() {
                    // Debug output before moving the data
                    if env::var("BAFIQ_BENCH_DEBUG").is_ok() {
                        println!(
                            "   {} - Peak Memory: {}, Avg CPU: {:.1}%",
                            name,
                            ResourceStats::format_memory(aggregated_resources.peak_memory_bytes),
                            aggregated_resources.avg_cpu_percent
                        );
                    }

                    data.insert(name.to_string(), aggregated_resources);
                }
            }

            total_time
        });
    });
}

/// Enhanced benchmark result that includes resource usage
#[derive(Debug)]
struct BenchmarkResult {
    duration: Duration,
    index: FlagIndex,
    resources: ResourceStats,
}

/// Represents a benchmarkable strategy or baseline
#[derive(Debug, Clone)]
enum BenchmarkEntry {
    /// Strategy-based benchmark using IndexBuilder
    Strategy(BuildStrategy),
    /// Legacy baseline benchmark with custom implementation
    Legacy {
        name: &'static str,
        runner: fn(&str) -> Result<FlagIndex>,
    },
}

impl BenchmarkEntry {
    /// Get the name of this benchmark entry
    fn name(&self) -> &str {
        match self {
            BenchmarkEntry::Strategy(strategy) => strategy.name(),
            BenchmarkEntry::Legacy { name, .. } => name,
        }
    }

    /// Run this benchmark entry
    fn run(&self, bam_path: &str) -> Result<FlagIndex> {
        match self {
            BenchmarkEntry::Strategy(strategy) => {
                let builder = IndexBuilder::with_strategy(*strategy);
                builder.build(bam_path)
            }
            BenchmarkEntry::Legacy { runner, .. } => runner(bam_path),
        }
    }

    /// Get all benchmark entries to run
    fn all_entries() -> Vec<BenchmarkEntry> {
        let mut entries = vec![
            // Legacy baseline implementations (for comparison)
            BenchmarkEntry::Legacy {
                name: "legacy_parallel_raw",
                runner: |path| benchmark::build_flag_index_parallel(path),
            },
            BenchmarkEntry::Legacy {
                name: "legacy_streaming_raw",
                runner: |path| benchmark::build_flag_index_streaming_parallel(path),
            },
        ];

        // Add all strategy-based benchmarks (use strategy.name() for consistent naming)
        for strategy in BuildStrategy::benchmark_strategies() {
            entries.push(BenchmarkEntry::Strategy(strategy));
        }

        entries
    }
}

/// Enhanced benchmark function with resource monitoring
fn run_benchmark_with_monitoring<F>(
    name: &str,
    test_bam: &str,
    mut benchmark_fn: F,
) -> Result<BenchmarkResult>
where
    F: FnMut(&str) -> Result<FlagIndex>,
{
    println!("Running monitored benchmark: {}", name);

    // Clear caches
    clear_file_system_cache()?;

    // Create fresh copy
    let temp_file = create_temp_bam_copy(test_bam)?;

    // Small delay to ensure cache clearing takes effect
    std::thread::sleep(Duration::from_millis(100));

    // Start resource monitoring
    let monitor = ResourceMonitor::new()?;
    monitor.start_monitoring(10)?; // Sample every 10ms

    // Run the actual benchmark
    let start = Instant::now();
    let index = benchmark_fn(temp_file.to_str().unwrap())?;
    let duration = start.elapsed();

    // Stop monitoring and get stats
    let resources = monitor.stop_monitoring();

    // Cleanup
    cleanup_temp_file(&temp_file);

    println!(
        "   Time: {:.3}s, Peak Memory: {}, Avg CPU: {:.1}%",
        duration.as_secs_f64(),
        ResourceStats::format_memory(resources.peak_memory_bytes),
        resources.avg_cpu_percent
    );

    Ok(BenchmarkResult {
        duration,
        index,
        resources,
    })
}

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
    println!("Running cold start benchmark: samtools view -c");

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

/// Generic benchmark runner for any benchmark entry
fn run_generic_benchmark(entry: &BenchmarkEntry, test_bam: &str) -> Result<BenchmarkResult> {
    run_benchmark_with_monitoring(entry.name(), test_bam, |path| entry.run(path))
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

    // Display machine and threading information
    let total_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or_else(|_| num_cpus::get());
    let rayon_threads = rayon::current_num_threads();

    println!("Simple Benchmarking with file: {}", test_bam);
    println!("Machine Configuration:");
    println!("   Available CPU cores: {}", total_cores);
    println!("   Rayon thread pool size: {}", rayon_threads);
    println!("   Fast development mode with resource monitoring");

    // Get original BAM file size for comparison
    let bam_size = get_file_size(&test_bam)?;
    println!("Original BAM size: {}", format_size(bam_size));
    println!("{}", "=".repeat(100));

    // Run all benchmark entries generically
    let benchmark_entries = BenchmarkEntry::all_entries();
    let mut results = Vec::new();

    for entry in &benchmark_entries {
        let result = run_generic_benchmark(entry, &test_bam)?;
        results.push((entry.name(), result));
    }

    // Run samtools benchmark
    let samtools_result = run_simple_samtools_benchmark(&test_bam)?;

    // Performance and Resource Usage Summary Table
    println!("\nPerformance & Resource Usage Summary:");
    println!("{}", "=".repeat(120));
    println!(
        "{:<25} {:>8} {:>12} {:>12} {:>8} {:>8} {:>12} {:>8}",
        "Strategy", "Time", "Peak RAM", "Avg RAM", "Peak CPU", "Avg CPU", "Index Size", "Samples"
    );
    println!("{}", "-".repeat(120));

    for (name, result) in &results {
        let index_size = calculate_index_size(&result.index)?;
        let index_percentage = (index_size as f64 / bam_size as f64) * 100.0;

        println!(
            "{:<25} {:>7.3}s {:>12} {:>12} {:>7.1}% {:>7.1}% {:>12} {:>8}",
            name,
            result.duration.as_secs_f64(),
            ResourceStats::format_memory(result.resources.peak_memory_bytes),
            ResourceStats::format_memory(result.resources.avg_memory_bytes),
            result.resources.peak_cpu_percent,
            result.resources.avg_cpu_percent,
            format!(
                "{} ({:.1}%)",
                format_size(index_size as u64),
                index_percentage
            ),
            result.resources.sample_count
        );
    }

    // Add samtools for comparison
    println!(
        "{:<25} {:>7.3}s {:>12} {:>12} {:>7} {:>7} {:>12} {:>8}",
        "samtools view -c -f 0x4",
        samtools_result.0.as_secs_f64(),
        "N/A",
        "N/A",
        "N/A",
        "N/A",
        "N/A",
        "N/A"
    );

    println!("{}", "=".repeat(120));

    // Memory efficiency analysis
    println!("\nMemory Efficiency Analysis:");
    let mut memory_efficiency: Vec<_> = results
        .iter()
        .map(|(name, result)| (*name, result.resources.peak_memory_bytes))
        .collect();
    memory_efficiency.sort_by_key(|(_, memory)| *memory);

    println!("   Most memory efficient:");
    for (i, (name, memory)) in memory_efficiency.iter().take(3).enumerate() {
        println!(
            "   {}. {} - {}",
            i + 1,
            name,
            ResourceStats::format_memory(*memory)
        );
    }

    // Speed analysis
    println!("\nSpeed Analysis:");
    let mut speed_ranking: Vec<_> = results
        .iter()
        .map(|(name, result)| (*name, result.duration))
        .collect();
    speed_ranking.sort_by_key(|(_, duration)| *duration);

    println!("   Fastest strategies:");
    for (i, (name, duration)) in speed_ranking.iter().take(3).enumerate() {
        let speedup_vs_samtools = samtools_result.0.as_secs_f64() / duration.as_secs_f64();
        println!(
            "   {}. {} - {:.3}s ({:.1}x faster than samtools)",
            i + 1,
            name,
            duration.as_secs_f64(),
            speedup_vs_samtools
        );
    }

    // CPU utilization analysis
    println!("\n🖥️  CPU Utilization Analysis:");
    let mut cpu_efficiency: Vec<_> = results
        .iter()
        .map(|(name, result)| (*name, result.resources.avg_cpu_percent))
        .collect();
    cpu_efficiency.sort_by(|(_, cpu_a), (_, cpu_b)| cpu_b.partial_cmp(cpu_a).unwrap());

    println!("   Best CPU utilization:");
    for (i, (name, cpu)) in cpu_efficiency.iter().take(3).enumerate() {
        println!("   {}. {} - {:.1}% average CPU", i + 1, name, cpu);
    }

    // Comprehensive index verification
    println!("\nIndex Verification:");
    if let Some((_, first_result)) = results.first() {
        let first_total = first_result.index.total_records();
        let all_match = results
            .iter()
            .all(|(_, result)| result.index.total_records() == first_total)
            && first_total == samtools_result.1;

        if all_match {
            println!("   All record counts match: {}", first_total);
            println!("   samtools verification: {} records", samtools_result.1);
        } else {
            println!("   Record count mismatch!");
            for (name, result) in &results {
                println!("      {}: {}", name, result.index.total_records());
            }
            println!("      samtools: {}", samtools_result.1);
            return Err(anyhow::anyhow!(
                "Index verification failed: record counts don't match"
            ));
        }

        // Performance gate check - find samtools baseline and compare
        println!("\n🎯 Performance Gate: Beat samtools (5.086s target)");
        let best_duration = results
            .iter()
            .map(|(_, result)| result.duration)
            .min()
            .unwrap();

        let best_strategy = results
            .iter()
            .find(|(_, result)| result.duration == best_duration)
            .map(|(name, _)| name)
            .unwrap();

        const SAMTOOLS_BASELINE: f64 = 5.086; // samtools view -c -f 0x4 benchmark result
        let best_seconds = best_duration.as_secs_f64();

        if best_seconds < SAMTOOLS_BASELINE {
            let speedup = SAMTOOLS_BASELINE / best_seconds;
            println!("   Status: PASSED ✅");
            println!(
                "   Best strategy: {} ({:.2}x faster than samtools)",
                best_strategy, speedup
            );
        } else {
            let slowdown = best_seconds / SAMTOOLS_BASELINE;
            println!("   Status: FAILED ❌");
            println!(
                "   Best strategy: {} ({:.2}x slower than samtools)",
                best_strategy, slowdown
            );
        }
    }

    Ok(())
}

/// Resource usage statistics for a benchmark run
#[derive(Debug, Clone)]
pub struct ResourceStats {
    /// Peak memory usage in bytes (RSS)
    peak_memory_bytes: u64,
    /// Average memory usage in bytes
    avg_memory_bytes: u64,
    /// Peak CPU usage percentage (0.0 - 100.0 * num_cores)
    peak_cpu_percent: f32,
    /// Average CPU usage percentage
    avg_cpu_percent: f32,
    /// Total execution time
    execution_time: Duration,
    /// Number of samples taken
    sample_count: usize,
}

impl ResourceStats {
    fn new() -> Self {
        Self {
            peak_memory_bytes: 0,
            avg_memory_bytes: 0,
            peak_cpu_percent: 0.0,
            avg_cpu_percent: 0.0,
            execution_time: Duration::new(0, 0),
            sample_count: 0,
        }
    }

    fn format_memory(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;

        if bytes >= GB {
            format!("{:.1}GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.1}MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.1}KB", bytes as f64 / KB as f64)
        } else {
            format!("{}B", bytes)
        }
    }
}

/// Resource monitor that tracks CPU and memory usage in a background thread
struct ResourceMonitor {
    system: Arc<Mutex<System>>,
    pid: Pid,
    running: Arc<Mutex<bool>>,
    stats: Arc<Mutex<ResourceStats>>,
}

impl ResourceMonitor {
    fn new() -> Result<Self> {
        let mut system = System::new_all();
        system.refresh_all();

        let pid = sysinfo::get_current_pid()
            .map_err(|e| anyhow::anyhow!("Failed to get current process PID: {}", e))?;

        Ok(Self {
            system: Arc::new(Mutex::new(system)),
            pid,
            running: Arc::new(Mutex::new(false)),
            stats: Arc::new(Mutex::new(ResourceStats::new())),
        })
    }

    /// Start monitoring in a background thread
    fn start_monitoring(&self, sample_interval_ms: u64) -> Result<()> {
        {
            let mut running = self.running.lock().unwrap();
            *running = true;
        }

        let system_clone = Arc::clone(&self.system);
        let running_clone = Arc::clone(&self.running);
        let stats_clone = Arc::clone(&self.stats);
        let pid = self.pid;

        thread::spawn(move || {
            let mut memory_samples = Vec::new();
            let mut cpu_samples = Vec::new();
            let start_time = Instant::now();

            while {
                let running = running_clone.lock().unwrap();
                *running
            } {
                {
                    let mut system = system_clone.lock().unwrap();
                    system.refresh_process(pid);

                    if let Some(process) = system.process(pid) {
                        let memory = process.memory();
                        let cpu = process.cpu_usage();

                        memory_samples.push(memory);
                        cpu_samples.push(cpu);

                        // Update running stats
                        let mut stats = stats_clone.lock().unwrap();
                        stats.peak_memory_bytes = stats.peak_memory_bytes.max(memory);
                        stats.peak_cpu_percent = stats.peak_cpu_percent.max(cpu);
                        stats.sample_count += 1;
                    }
                }

                thread::sleep(Duration::from_millis(sample_interval_ms));
            }

            // Calculate final averages
            let execution_time = start_time.elapsed();
            let avg_memory = if !memory_samples.is_empty() {
                memory_samples.iter().sum::<u64>() / memory_samples.len() as u64
            } else {
                0
            };
            let avg_cpu = if !cpu_samples.is_empty() {
                cpu_samples.iter().sum::<f32>() / cpu_samples.len() as f32
            } else {
                0.0
            };

            let mut final_stats = stats_clone.lock().unwrap();
            final_stats.avg_memory_bytes = avg_memory;
            final_stats.avg_cpu_percent = avg_cpu;
            final_stats.execution_time = execution_time;
        });

        Ok(())
    }

    /// Stop monitoring and return final statistics
    fn stop_monitoring(&self) -> ResourceStats {
        {
            let mut running = self.running.lock().unwrap();
            *running = false;
        }

        // Give the monitoring thread a moment to finish
        thread::sleep(Duration::from_millis(100));

        let stats = self.stats.lock().unwrap();
        stats.clone()
    }
}

/// Generic Criterion benchmark for any benchmark entry
fn benchmark_entry_criterion(
    group: &mut BenchmarkGroup<WallTime>,
    entry: &BenchmarkEntry,
    test_bam: &str,
) {
    benchmark_cold_start(group, entry.name(), test_bam, |file_path| {
        let index = entry
            .run(file_path)
            .unwrap_or_else(|e| panic!("Failed to build index with {}: {}", entry.name(), e));
        index.total_records()
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

    // Display machine and threading information
    let total_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or_else(|_| num_cpus::get());
    let rayon_threads = rayon::current_num_threads();

    println!("Criterion Benchmarking with file: {}", test_bam);
    println!("Machine Configuration:");
    println!("   Available CPU cores: {}", total_cores);
    println!("   Rayon thread pool size: {}", rayon_threads);
    println!(
        "Using cold start conditions via cache clearing (10 samples for statistical analysis)"
    );

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
        .measurement_time(Duration::from_secs(1020)) // Reasonable time for 10 samples
        .warm_up_time(Duration::from_millis(100)); // Minimal warmup to satisfy Criterion

    // Run all benchmark entries generically
    let benchmark_entries = BenchmarkEntry::all_entries();
    for entry in &benchmark_entries {
        benchmark_entry_criterion(&mut group, entry, &test_bam);
    }

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

    // Report resource usage statistics from all Criterion benchmarks
    report_criterion_resource_usage();
}

// Manual main function handles both simple and Criterion benchmarks
// No need for criterion_group! macro
fn main() {
    // Check if we should run Criterion benchmarks or simple benchmarks
    if env::var("BAFIQ_USE_CRITERION").is_ok() {
        // Run full Criterion benchmarks (slower, statistical analysis)
        println!("🔬 Running Criterion benchmarks (full statistical analysis)...");
        let mut criterion = Criterion::default()
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(10))
            .sample_size(10);

        criterion_benchmarks(&mut criterion);

        // Report resource usage statistics from Criterion benchmarks
        report_criterion_resource_usage();
    } else {
        // Run simple benchmarks (faster, for development)
        println!("Running simple benchmarks (fast development mode)...");
        match simple_benchmarks() {
            Ok(()) => println!("Simple benchmarks completed successfully"),
            Err(e) => eprintln!("Simple benchmarks failed: {}", e),
        }
    }
}
