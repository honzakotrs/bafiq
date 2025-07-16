use anyhow::Result;
use std::process::Command;

use crate::shared::{
    BAFIQ_FAST_COUNT, BenchmarkCommand, BenchmarkOrchestrator, BenchmarkSuite, SAMTOOLS,
    VerificationResult,
    analysis::BenchmarkResult,
    commands::{create_bafiq_command, extract_record_count_from_output, run_with_monitoring},
};
use bafiq::BuildStrategy;

use super::cli::IndexBenchArgs;

/// Index building command that implements BenchmarkCommand
struct IndexBuildCommand {
    strategy: BuildStrategy,
    bam_path: String,
}

impl BenchmarkCommand for IndexBuildCommand {
    fn name(&self) -> String {
        format!("bafiq index {}", self.strategy.name())
    }

    fn run_monitored(&self, thread_count: usize) -> Result<BenchmarkResult> {
        // Clean up any existing index to ensure fresh build
        let index_path = format!("{}.bfi", self.bam_path);
        let _ = std::fs::remove_file(&index_path);

        let mut cmd = create_bafiq_command(thread_count);
        cmd.args(&["index", "--strategy", &self.strategy.name(), &self.bam_path]);

        // Run index building with monitoring
        let (duration, _, memory_stats, cpu_stats, sample_count, memory_samples) =
            run_with_monitoring(cmd)?;

        // Now query the index to get record count (quick operation, no additional monitoring needed)
        let query_output = Command::new("./target/release/bafiq")
            .args(&["query", &self.bam_path])
            .output()?;

        let record_count = if query_output.status.success() {
            // Try both stdout and stderr since bafiq might output to stderr
            let stdout = String::from_utf8_lossy(&query_output.stdout);
            let stderr = String::from_utf8_lossy(&query_output.stderr);
            let combined_output = format!("{}\n{}", stdout, stderr);
            extract_record_count_from_output(&combined_output)
        } else {
            0
        };

        // Calculate index size
        let index_size_mb = if std::path::Path::new(&index_path).exists() {
            let metadata = std::fs::metadata(&index_path)?;
            metadata.len() as f64 / 1024.0 / 1024.0 // Convert to MB
        } else {
            0.0
        };

        Ok(BenchmarkResult {
            strategy: self.name(),
            thread_count,
            duration,
            peak_memory_mb: memory_stats.0,
            avg_memory_mb: memory_stats.1,
            peak_cpu_percent: cpu_stats.0,
            avg_cpu_percent: cpu_stats.1,
            index_size_mb,
            record_count: record_count as usize,
            sample_count,
            memory_samples,
        })
    }
}

/// Fast count command that implements BenchmarkCommand
struct FastCountCommand {
    bam_path: String,
}

impl BenchmarkCommand for FastCountCommand {
    fn name(&self) -> String {
        BAFIQ_FAST_COUNT.to_string()
    }

    fn run_monitored(&self, thread_count: usize) -> Result<BenchmarkResult> {
        let mut cmd = create_bafiq_command(thread_count);
        cmd.args(&["fast-count", &self.bam_path]);

        // Run with monitoring
        let (duration, record_count, memory_stats, cpu_stats, sample_count, memory_samples) =
            run_with_monitoring(cmd)?;

        Ok(BenchmarkResult {
            strategy: self.name(),
            thread_count,
            duration,
            peak_memory_mb: memory_stats.0,
            avg_memory_mb: memory_stats.1,
            peak_cpu_percent: cpu_stats.0,
            avg_cpu_percent: cpu_stats.1,
            index_size_mb: 0.0, // No index created
            record_count: record_count as usize,
            sample_count,
            memory_samples,
        })
    }
}

/// Samtools comparison command that implements BenchmarkCommand  
struct SamtoolsCommand {
    bam_path: String,
}

impl BenchmarkCommand for SamtoolsCommand {
    fn name(&self) -> String {
        SAMTOOLS.to_string()
    }

    fn run_monitored(&self, thread_count: usize) -> Result<BenchmarkResult> {
        let mut cmd = Command::new(SAMTOOLS);
        cmd.arg("view");

        // Add thread parameter if not auto (0)
        // Note: samtools -@ specifies additional threads, so for total thread_count we use (thread_count - 1)
        if thread_count > 1 {
            cmd.args(&["-@", &(thread_count - 1).to_string()]);
        }

        cmd.args(&["-c", &self.bam_path]);

        // Run with monitoring
        let (duration, record_count, memory_stats, cpu_stats, sample_count, memory_samples) =
            run_with_monitoring(cmd)?;

        Ok(BenchmarkResult {
            strategy: self.name(),
            thread_count,
            duration,
            peak_memory_mb: memory_stats.0,
            avg_memory_mb: memory_stats.1,
            peak_cpu_percent: cpu_stats.0,
            avg_cpu_percent: cpu_stats.1,
            index_size_mb: 0.0, // No index created
            record_count: record_count as usize,
            sample_count,
            memory_samples,
        })
    }
}

/// Index building benchmark suite
struct IndexBenchmarkSuite {
    bam_path: String,
}

impl BenchmarkSuite for IndexBenchmarkSuite {
    fn commands(&self) -> Vec<Box<dyn BenchmarkCommand>> {
        let mut commands: Vec<Box<dyn BenchmarkCommand>> = Vec::new();

        // Add core bafiq index building strategies
        for strategy in BuildStrategy::benchmark_strategies() {
            commands.push(Box::new(IndexBuildCommand {
                strategy,
                bam_path: self.bam_path.clone(),
            }));
        }

        // Add bafiq fast-count (direct record counting without indexing)
        commands.push(Box::new(FastCountCommand {
            bam_path: self.bam_path.clone(),
        }));

        // Add external tools for comparison (check availability first)
        if Command::new(SAMTOOLS).arg("--version").output().is_ok() {
            commands.push(Box::new(SamtoolsCommand {
                bam_path: self.bam_path.clone(),
            }));
        }

        commands
    }

    fn verify_results(&self, results: &[BenchmarkResult]) -> VerificationResult {
        // Verification status - only check tools that output record counts
        let count_outputting_results: Vec<&BenchmarkResult> = results
            .iter()
            .filter(|r| r.strategy == BAFIQ_FAST_COUNT || r.strategy == SAMTOOLS)
            .collect();

        if !count_outputting_results.is_empty() {
            let first_count = count_outputting_results[0].record_count;
            let all_match = count_outputting_results
                .iter()
                .all(|r| r.record_count == first_count);

            if all_match {
                VerificationResult::Passed(format!(
                    "Record count: PASSED ({} records)",
                    first_count
                ))
            } else {
                VerificationResult::Failed(format!("Record count: FAILED (counts differ)"))
            }
        } else {
            VerificationResult::Skipped(
                "Record count: SKIPPED (no tools output record counts)".to_string(),
            )
        }
    }

    fn suite_name(&self) -> &str {
        "thread_scaling"
    }

    fn input_description(&self) -> String {
        format!(
            "index building with file: {}",
            std::path::Path::new(&self.bam_path)
                .file_name()
                .unwrap()
                .to_string_lossy()
        )
    }
}

/// Run index benchmark with provided arguments
pub fn run_index_benchmark(index_args: IndexBenchArgs) -> Result<()> {
    // Create configuration from parsed args
    let config = BenchmarkOrchestrator::create_config_from_args(&index_args.common)?;

    // Create and run the index benchmark suite
    let suite = Box::new(IndexBenchmarkSuite {
        bam_path: index_args.bam,
    });

    // Create orchestrator with the parsed configuration
    let orchestrator = BenchmarkOrchestrator::with_config(config);

    println!("Running index benchmarks...");
    orchestrator.run_suite(suite)?;
    println!("\nIndex benchmarks completed successfully");

    Ok(())
}
