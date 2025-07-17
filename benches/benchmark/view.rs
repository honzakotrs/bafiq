use anyhow::Result;
use std::process::Command;

use crate::shared::{
    BenchmarkCommand, BenchmarkOrchestrator, BenchmarkSuite, SAMTOOLS, VerificationResult,
    analysis::BenchmarkResult,
    commands::{create_bafiq_command, run_with_monitoring},
};

use super::cli::ViewBenchArgs;

/// Bafiq view command that implements BenchmarkCommand
struct BafiqViewCommand {
    bam_path: String,
    flag: u32,
}

impl BenchmarkCommand for BafiqViewCommand {
    fn name(&self) -> String {
        format!("bafiq view -f 0x{:x}", self.flag)
    }

    fn run_monitored(&self, thread_count: usize) -> Result<BenchmarkResult> {
        let mut cmd = create_bafiq_command(thread_count);
        cmd.args(&["view", "-f", &format!("0x{:x}", self.flag), &self.bam_path]);

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

/// Samtools view command that implements BenchmarkCommand
struct SamtoolsViewCommand {
    bam_path: String,
    flag: u32,
}

impl BenchmarkCommand for SamtoolsViewCommand {
    fn name(&self) -> String {
        format!("samtools view -f 0x{:x}", self.flag)
    }

    fn run_monitored(&self, thread_count: usize) -> Result<BenchmarkResult> {
        let mut cmd = Command::new(SAMTOOLS);
        cmd.arg("view");

        // Add thread parameter if not auto (0)
        if thread_count > 1 {
            cmd.args(&["-@", &(thread_count - 1).to_string()]);
        }

        cmd.args(&["-h", "-f", &format!("0x{:x}", self.flag), &self.bam_path]);

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

/// View benchmark suite
struct ViewBenchmarkSuite {
    bam_files: Vec<String>,
    flags: Vec<u32>,
}

impl ViewBenchmarkSuite {
    fn new(bam_files_str: &str, flags_str: &str) -> Result<Self> {
        let bam_files: Vec<String> = bam_files_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let flags: Result<Vec<u32>, _> = flags_str
            .split(',')
            .map(|s| {
                let trimmed = s.trim();
                if trimmed.starts_with("0x") || trimmed.starts_with("0X") {
                    u32::from_str_radix(&trimmed[2..], 16)
                } else {
                    trimmed.parse::<u32>()
                }
            })
            .collect();

        Ok(Self {
            bam_files,
            flags: flags.map_err(|e| anyhow::anyhow!("Invalid flag format: {}", e))?,
        })
    }
}

impl BenchmarkSuite for ViewBenchmarkSuite {
    fn commands(&self) -> Vec<Box<dyn BenchmarkCommand>> {
        let mut commands: Vec<Box<dyn BenchmarkCommand>> = Vec::new();

        // For each combination of BAM file and flag
        for bam_file in &self.bam_files {
            for &flag in &self.flags {
                // Add bafiq view command
                commands.push(Box::new(BafiqViewCommand {
                    bam_path: bam_file.clone(),
                    flag,
                }));

                // Add samtools view command if available
                if Command::new(SAMTOOLS).arg("--version").output().is_ok() {
                    commands.push(Box::new(SamtoolsViewCommand {
                        bam_path: bam_file.clone(),
                        flag,
                    }));
                }
            }
        }

        commands
    }

    fn verify_results(&self, _results: &[BenchmarkResult]) -> VerificationResult {
        // For view operations, we could compare output content, but for now we skip verification
        // since view operations don't produce simple counts to compare
        VerificationResult::Skipped(
            "Content verification: SKIPPED (view operations produce SAM output)".to_string(),
        )
    }

    fn suite_name(&self) -> &str {
        "view_performance"
    }

    fn input_description(&self) -> String {
        format!(
            "view operations with BAM files: {:?}, flags: {:?}",
            self.bam_files
                .iter()
                .map(|f| std::path::Path::new(f)
                    .file_name()
                    .unwrap()
                    .to_string_lossy())
                .collect::<Vec<_>>(),
            self.flags
                .iter()
                .map(|&f| format!("0x{:x}", f))
                .collect::<Vec<_>>()
        )
    }
}

/// Run view benchmark with provided arguments
pub fn run_view_benchmark(view_args: ViewBenchArgs) -> Result<()> {
    // Create configuration from parsed args
    let config = BenchmarkOrchestrator::create_config_from_args(&view_args.common)?;

    // Create the view benchmark suite
    let suite = ViewBenchmarkSuite::new(&view_args.bam, &view_args.flags)?;
    let suite = Box::new(suite);

    // Create orchestrator with the parsed configuration
    let orchestrator = BenchmarkOrchestrator::with_config(config);

    println!("Running view benchmarks...");
    orchestrator.run_suite(suite)?;
    println!("\nView benchmarks completed successfully");

    Ok(())
}
