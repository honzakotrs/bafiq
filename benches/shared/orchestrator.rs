use anyhow::Result;
use clap::Parser;

use super::{
    BenchmarkConfig, BenchmarkSuite, VerificationResult,
    analysis::{
        BenchmarkResult, analyze_results, display_comprehensive_analysis, display_csv_results,
        display_performance_summary, export_csv_results,
    },
    commands::clear_system_cache,
};

/// Shared CLI arguments for all benchmark types
#[derive(Parser, Debug)]
pub struct BenchmarkArgs {
    /// Thread counts to test (comma-separated), e.g., "2,4,8,max"
    #[arg(long, default_value = "2,max")]
    pub threads: String,

    /// Number of samples (1 = single run, >1 = statistical analysis)
    #[arg(long, default_value = "1")]
    pub samples: usize,

    /// Keep temporary files for inspection
    #[arg(long)]
    pub keep_temp: bool,

    /// Clear system cache before each benchmark (requires sudo on Linux)
    #[arg(long)]
    pub clear_cache: bool,
}

impl BenchmarkConfig {
    /// Parse thread counts string into actual thread counts
    fn parse_thread_counts_from_string(threads_str: &str) -> Result<Vec<usize>> {
        let max_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or_else(|_| num_cpus::get());

        let mut thread_counts = Vec::new();

        for thread_spec in threads_str.split(',') {
            let thread_spec = thread_spec.trim();

            if thread_spec == "max" {
                thread_counts.push(max_cores);
            } else if thread_spec == "auto" {
                // Special case: use default rayon thread pool (no explicit setting)
                thread_counts.push(0); // 0 will mean "auto" in our implementation
            } else {
                match thread_spec.parse::<usize>() {
                    Ok(count) if count > 0 => thread_counts.push(count),
                    Ok(0) => thread_counts.push(0), // Auto mode
                    _ => {
                        eprintln!("Warning: Invalid thread count '{}', skipping", thread_spec);
                    }
                }
            }
        }

        if thread_counts.is_empty() {
            thread_counts.push(max_cores); // Default to max if nothing valid parsed
        }

        // Remove duplicates and sort
        thread_counts.sort_unstable();
        thread_counts.dedup();

        Ok(thread_counts)
    }

    /// Display configuration summary
    pub fn display_summary(&self, suite: &dyn BenchmarkSuite) {
        let max_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or_else(|_| num_cpus::get());

        println!(
            "Benchmarking {}: {}",
            suite.suite_name(),
            suite.input_description()
        );

        println!("Machine Configuration:");
        println!("   Available CPU cores: {}", max_cores);
        println!("   Thread counts to test: {:?}", self.thread_counts);
        println!("   Samples per benchmark: {}", self.sample_size);

        if self.keep_temp_files {
            println!("   Temp file preservation: enabled");
        }
        if self.clear_cache {
            println!("   Cache clearing: enabled (ensures fair benchmarking)");
        } else {
            println!(
                "   Warning: Cache clearing disabled (results may be affected by file caching)"
            );
        }

        if self.sample_size == 1 {
            println!("   Single sample mode: One run per benchmark for rapid iteration");
            println!("   Tip: Use --samples >1 for statistical analysis");
        } else {
            println!("   Multi-sample mode: Multiple runs with statistical analysis");
            println!("   Tip: Use --samples 1 for development");
        }
    }
}

/// Main benchmark orchestrator that can run any benchmark suite
pub struct BenchmarkOrchestrator {
    config: BenchmarkConfig,
}

impl BenchmarkOrchestrator {
    /// Create new orchestrator with provided configuration
    pub fn with_config(config: BenchmarkConfig) -> Self {
        Self { config }
    }

    /// Create configuration from individual benchmark arguments
    pub fn create_config_from_args(args: &BenchmarkArgs) -> Result<BenchmarkConfig> {
        // Parse thread counts
        let thread_counts = BenchmarkConfig::parse_thread_counts_from_string(&args.threads)?;

        Ok(BenchmarkConfig {
            thread_counts,
            sample_size: args.samples,
            keep_temp_files: args.keep_temp,
            clear_cache: args.clear_cache,
        })
    }

    /// Run a complete benchmark suite with thread scaling and analysis
    pub fn run_suite(&self, suite: Box<dyn BenchmarkSuite>) -> Result<()> {
        // Display configuration summary
        self.config.display_summary(&*suite);

        println!("{}", "=".repeat(100));

        println!("Thread Configuration: Passed to commands via --threads parameter");
        println!("   2-thread benchmarks -> --threads 2");
        println!("   10-thread benchmarks -> --threads 10");
        println!("   Auto benchmarks -> no --threads parameter (command decides)");
        println!("{}", "=".repeat(100));

        // Get all commands to benchmark
        let commands = suite.commands();
        let total_combinations = commands.len() * self.config.thread_counts.len();

        println!("Running {} benchmark configurations...", total_combinations);

        // Run all benchmarks sequentially with comprehensive monitoring
        let mut comprehensive_results = Vec::new();

        let mut config_index = 0;
        for command in &commands {
            for &thread_count in &self.config.thread_counts {
                config_index += 1;

                // Clear cache before each benchmark for fair comparison
                if let Err(e) = clear_system_cache(self.config.clear_cache) {
                    eprintln!("   Warning: Cache clearing failed: {}", e);
                }

                if thread_count == 0 {
                    println!(
                        "\n[{}/{}] {} (auto threads)",
                        config_index,
                        total_combinations,
                        command.name()
                    );
                } else {
                    println!(
                        "\n[{}/{}] {} (--threads {})",
                        config_index,
                        total_combinations,
                        command.name(),
                        thread_count
                    );
                }

                // Run multiple samples for statistical analysis
                let mut sample_results = Vec::new();

                for sample_num in 1..=self.config.sample_size {
                    if self.config.sample_size > 1 {
                        println!("   Sample {}/{}", sample_num, self.config.sample_size);

                        // Clear cache before each sample for consistency
                        if let Err(e) = clear_system_cache(self.config.clear_cache) {
                            eprintln!("      Warning: Cache clearing failed: {}", e);
                        }
                    }

                    let result = command.run_monitored(thread_count)?;
                    sample_results.push(result);

                    if self.config.sample_size > 1 {
                        let last_result = sample_results.last().unwrap();
                        println!(
                            "      Sample {}: {:.3}s, Peak Memory: {:.0}MB, Avg CPU: {:.1}%",
                            sample_num,
                            last_result.duration.as_secs_f64(),
                            last_result.peak_memory_mb,
                            last_result.avg_cpu_percent
                        );
                    }
                }

                // Aggregate results across all samples
                let durations: Vec<f64> = sample_results
                    .iter()
                    .map(|r| r.duration.as_secs_f64())
                    .collect();
                let mean_duration = durations.iter().sum::<f64>() / durations.len() as f64;
                let min_duration = durations.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                let max_duration = durations.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

                let peak_memories: Vec<f64> =
                    sample_results.iter().map(|r| r.peak_memory_mb).collect();
                let mean_peak_memory =
                    peak_memories.iter().sum::<f64>() / peak_memories.len() as f64;

                let avg_cpus: Vec<f64> = sample_results.iter().map(|r| r.avg_cpu_percent).collect();
                let mean_avg_cpu = avg_cpus.iter().sum::<f64>() / avg_cpus.len() as f64;

                // Use the first sample's values for record count and memory samples (should be consistent)
                let first_result = &sample_results[0];

                // Create comprehensive result with aggregated data
                let comprehensive_result = BenchmarkResult {
                    strategy: command.name(),
                    thread_count,
                    duration: std::time::Duration::from_secs_f64(mean_duration),
                    peak_memory_mb: mean_peak_memory,
                    avg_memory_mb: first_result.avg_memory_mb,
                    peak_cpu_percent: first_result.peak_cpu_percent,
                    avg_cpu_percent: mean_avg_cpu,
                    index_size_mb: first_result.index_size_mb,
                    record_count: first_result.record_count,
                    sample_count: first_result.sample_count,
                    memory_samples: first_result.memory_samples.clone(),
                };

                comprehensive_results.push(comprehensive_result);

                // Show aggregated results
                if self.config.sample_size == 1 {
                    println!(
                        "   Time: {:.3}s, Records: {}, Peak Memory: {:.0}MB, Avg CPU: {:.1}%",
                        mean_duration, first_result.record_count, mean_peak_memory, mean_avg_cpu
                    );
                } else {
                    let std_dev = {
                        let variance = durations
                            .iter()
                            .map(|&x| (x - mean_duration).powi(2))
                            .sum::<f64>()
                            / durations.len() as f64;
                        variance.sqrt()
                    };

                    println!(
                        "   Mean: {:.3}s ± {:.3}s, Range: {:.3}s - {:.3}s, Records: {}, Peak Memory: {:.0}MB ± {:.0}MB",
                        mean_duration,
                        std_dev,
                        min_duration,
                        max_duration,
                        first_result.record_count,
                        mean_peak_memory,
                        peak_memories
                            .iter()
                            .map(|&x| (x - mean_peak_memory).abs())
                            .sum::<f64>()
                            / peak_memories.len() as f64
                    );
                }
            }
        }

        // Comprehensive output and analysis
        display_performance_summary(&comprehensive_results);

        let analysis = analyze_results(&comprehensive_results);
        display_comprehensive_analysis(&analysis);

        // Verification
        println!("\nVerification:");
        match suite.verify_results(&comprehensive_results) {
            VerificationResult::Passed(msg) => println!("   {}", msg),
            VerificationResult::Failed(msg) => {
                println!("   {}", msg);
                eprintln!("Warning: Verification failed - results may be inconsistent");
            }
            VerificationResult::Skipped(msg) => println!("   {}", msg),
        }

        display_csv_results(&comprehensive_results);

        // Export CSV files
        let (combined_csv, memory_csv) =
            export_csv_results(&comprehensive_results, suite.suite_name())?;
        println!("\nResults saved to: {}", combined_csv);
        println!("Detailed memory samples saved to: {}", memory_csv);

        Ok(())
    }
}
