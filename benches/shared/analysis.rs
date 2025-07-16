use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{format_thread_display, format_thread_with_suffix};

/// Detailed benchmark result with comprehensive metrics
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub strategy: String,
    pub thread_count: usize,
    pub duration: Duration,
    pub peak_memory_mb: f64,
    pub avg_memory_mb: f64,
    pub peak_cpu_percent: f64,
    pub avg_cpu_percent: f64,
    pub index_size_mb: f64,
    pub record_count: usize,
    pub sample_count: usize,
    pub memory_samples: Vec<ResourceSample>,
}

/// Memory and CPU sample for timeline tracking
#[derive(Debug, Clone)]
pub struct ResourceSample {
    pub timestamp_ms: u64,
    pub memory_mb: f64,
    pub cpu_percent: f64,
}

/// Analysis results for comprehensive reporting
#[derive(Debug)]
pub struct AnalysisResults {
    pub fastest_overall: BenchmarkResult,
    pub most_memory_efficient: BenchmarkResult,
    pub best_cpu_utilization: BenchmarkResult,
    pub samtools_baseline: Option<BenchmarkResult>,
    pub all_results: Vec<BenchmarkResult>,
}

impl BenchmarkResult {
    /// Format memory display (MB for sub-GB, GB for 1GB+)
    pub fn format_memory(memory_mb: f64) -> String {
        if memory_mb >= 1024.0 {
            format!("{:.1}GB", memory_mb / 1024.0)
        } else {
            format!("{:.0}MB", memory_mb)
        }
    }

    /// Get formatted peak memory string
    pub fn peak_memory_formatted(&self) -> String {
        Self::format_memory(self.peak_memory_mb)
    }

    /// Get formatted average memory string
    pub fn avg_memory_formatted(&self) -> String {
        Self::format_memory(self.avg_memory_mb)
    }

    /// Calculate speedup vs baseline (1.0 = same speed, 2.0 = 2x faster)
    pub fn speedup_vs(&self, baseline: &BenchmarkResult) -> f64 {
        baseline.duration.as_secs_f64() / self.duration.as_secs_f64()
    }
}

/// Comprehensive analysis engine
pub fn analyze_results(results: &[BenchmarkResult]) -> AnalysisResults {
    let fastest_overall = results
        .iter()
        .min_by(|a, b| a.duration.cmp(&b.duration))
        .unwrap()
        .clone();

    let most_memory_efficient = results
        .iter()
        .min_by(|a, b| a.peak_memory_mb.partial_cmp(&b.peak_memory_mb).unwrap())
        .unwrap()
        .clone();

    let best_cpu_utilization = results
        .iter()
        .max_by(|a, b| a.avg_cpu_percent.partial_cmp(&b.avg_cpu_percent).unwrap())
        .unwrap()
        .clone();

    let samtools_baseline = results.iter().find(|r| r.strategy == "samtools").cloned();

    AnalysisResults {
        fastest_overall,
        most_memory_efficient,
        best_cpu_utilization,
        samtools_baseline,
        all_results: results.to_vec(),
    }
}

/// Display comprehensive performance summary
pub fn display_performance_summary(results: &[BenchmarkResult]) {
    println!("\nPerformance & Resource Usage Summary:");
    println!("{}", "=".repeat(100));
    println!(
        "{:<25} {:<8} {:<10} {:<10} {:<8} {:<8} {:<12}",
        "Strategy", "Threads", "Time", "Peak RAM", "Avg RAM", "Peak CPU", "Avg CPU"
    );
    println!("{}", "-".repeat(100));

    for result in results {
        let thread_display = format_thread_with_suffix(result.thread_count);

        println!(
            "{:<25} {:<8} {:<10} {:<10} {:<8} {:<8} {:<12}",
            result.strategy,
            thread_display,
            format!("{:.3}s", result.duration.as_secs_f64()),
            result.peak_memory_formatted(),
            result.avg_memory_formatted(),
            format!("{:.1}%", result.peak_cpu_percent),
            format!("{:.1}%", result.avg_cpu_percent)
        );
    }
    println!("{}", "=".repeat(100));
}

/// Display comprehensive analysis
pub fn display_comprehensive_analysis(analysis: &AnalysisResults) {
    println!("\nThread Scaling Analysis:");

    // Group by thread count and find best for each
    let mut thread_groups: HashMap<usize, Vec<&BenchmarkResult>> = HashMap::new();
    for result in &analysis.all_results {
        thread_groups
            .entry(result.thread_count)
            .or_default()
            .push(result);
    }

    for (&thread_count, group) in thread_groups.iter() {
        let best = group
            .iter()
            .min_by(|a, b| a.duration.cmp(&b.duration))
            .unwrap();
        let thread_display = format_thread_display(thread_count);
        println!(
            "   {} thread(s): Best: {} - {:.3}s",
            thread_display,
            best.strategy,
            best.duration.as_secs_f64()
        );
    }

    println!("\nSpeed Analysis:");
    println!(
        "   Fastest overall: {} ({} threads) - {:.3}s",
        analysis.fastest_overall.strategy,
        format_thread_display(analysis.fastest_overall.thread_count),
        analysis.fastest_overall.duration.as_secs_f64()
    );

    if let Some(ref samtools) = analysis.samtools_baseline {
        let speedup = analysis.fastest_overall.speedup_vs(samtools);
        println!("   Speedup vs samtools: {:.1}x faster", speedup);
    }

    println!("\nMemory Efficiency Analysis:");
    println!(
        "   Most memory efficient: {} ({} threads) - {}",
        analysis.most_memory_efficient.strategy,
        format_thread_with_suffix(analysis.most_memory_efficient.thread_count),
        analysis.most_memory_efficient.peak_memory_formatted()
    );

    println!("\nCPU Utilization Analysis:");
    println!(
        "   Best CPU utilization: {} ({} threads) - {:.1}% average CPU",
        analysis.best_cpu_utilization.strategy,
        format_thread_with_suffix(analysis.best_cpu_utilization.thread_count),
        analysis.best_cpu_utilization.avg_cpu_percent
    );
}

/// Generate CSV output
pub fn display_csv_results(results: &[BenchmarkResult]) {
    println!("\nCSV Results:");
    println!(
        "threads,strategy,time_ms,peak_memory_mb,avg_memory_mb,peak_cpu_percent,avg_cpu_percent,index_size_mb,samples"
    );

    for result in results {
        println!(
            "{},{},{},{:.1},{:.1},{:.1},{:.1},{:.1},{}",
            result.thread_count, // 0 represents auto mode
            result.strategy,
            result.duration.as_millis() as u64,
            result.peak_memory_mb,
            result.avg_memory_mb,
            result.peak_cpu_percent,
            result.avg_cpu_percent,
            result.index_size_mb,
            result.sample_count
        );
    }
}

/// CSV export functionality
pub fn export_csv_results(
    results: &[BenchmarkResult],
    suite_name: &str,
) -> Result<(String, String)> {
    // Create output directory
    std::fs::create_dir_all("./benchmark_results")?;

    // Generate timestamp
    let timestamp_str = {
        let now = SystemTime::now();
        let duration = now.duration_since(UNIX_EPOCH).unwrap();
        format!("timestamp_{}", duration.as_secs())
    };

    let combined_csv = format!("./benchmark_results/{}_{}.csv", suite_name, timestamp_str);
    let memory_csv = format!(
        "./benchmark_results/{}_memory_samples_{}.csv",
        suite_name, timestamp_str
    );

    // Write main results CSV
    let mut file = File::create(&combined_csv)?;
    writeln!(
        file,
        "threads,strategy,time_ms,peak_memory_mb,avg_memory_mb,peak_cpu_percent,avg_cpu_percent,index_size_mb,samples"
    )?;

    for result in results {
        writeln!(
            file,
            "{},{},{},{:.1},{:.1},{:.1},{:.1},{:.1},{}",
            result.thread_count, // 0 represents auto mode
            result.strategy,
            result.duration.as_millis() as u64,
            result.peak_memory_mb,
            result.avg_memory_mb,
            result.peak_cpu_percent,
            result.avg_cpu_percent,
            result.index_size_mb,
            result.sample_count
        )?;
    }

    // Write detailed memory samples CSV
    let mut mem_file = File::create(&memory_csv)?;
    writeln!(
        mem_file,
        "threads,strategy,timestamp_ms,memory_mb,cpu_percent"
    )?;

    // Export individual memory samples from all benchmark runs
    for result in results {
        let thread_csv = result.thread_count; // 0 represents auto mode

        if !result.memory_samples.is_empty() {
            // Write actual memory samples
            for sample in &result.memory_samples {
                writeln!(
                    mem_file,
                    "{},{},{},{:.1},{:.1}",
                    thread_csv,
                    result.strategy,
                    sample.timestamp_ms,
                    sample.memory_mb,
                    sample.cpu_percent
                )?;
            }
        } else {
            // Fallback: write a single entry using peak values for backward compatibility
            writeln!(
                mem_file,
                "{},{},0,{:.1},{:.1}",
                thread_csv, result.strategy, result.peak_memory_mb, result.peak_cpu_percent
            )?;
        }
    }

    Ok((combined_csv, memory_csv))
}
