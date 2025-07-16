use anyhow::Result;
use clap::Parser;

use std::path::Path;
use std::process::Command;

mod shared;
use shared::{
    BenchmarkCommand, BenchmarkOrchestrator, BenchmarkSuite, SAMTOOLS, VerificationResult,
    analysis::BenchmarkResult,
    commands::{create_bafiq_command, run_with_monitoring},
};

/// View benchmark CLI arguments
#[derive(Parser, Debug)]
#[command(name = "view_bench")]
#[command(about = "Bafiq view performance benchmark with thread scaling")]
struct ViewBenchArgs {
    #[command(flatten)]
    common: shared::orchestrator::BenchmarkArgs,

    /// BAM file(s) to benchmark (comma-separated)
    #[arg(long)]
    bam: String,

    /// Flags to test (comma-separated hex values), e.g., "0x4,0x2"
    #[arg(long, default_value = "0x4")]
    flags: String,
}

/// Bafiq view command
struct BafiqViewCommand {
    bam_path: String,
    flag: u16,
}

impl BenchmarkCommand for BafiqViewCommand {
    fn name(&self) -> String {
        format!("bafiq view -f 0x{:x}", self.flag)
    }

    fn run_monitored(&self, thread_count: usize) -> Result<BenchmarkResult> {
        let mut cmd = create_bafiq_command(thread_count);
        cmd.args(&["view", "-f", &format!("0x{:x}", self.flag), &self.bam_path]);

        let (duration, _record_count, memory_stats, cpu_stats, sample_count, memory_samples) =
            run_with_monitoring(cmd)?;

        Ok(BenchmarkResult {
            strategy: self.name(),
            thread_count,
            duration,
            peak_memory_mb: memory_stats.0,
            avg_memory_mb: memory_stats.1,
            peak_cpu_percent: cpu_stats.0,
            avg_cpu_percent: cpu_stats.1,
            index_size_mb: 0.0, // No index created for view operations
            record_count: 0,    // View operations don't count records directly
            sample_count,
            memory_samples,
        })
    }
}

/// Samtools view command for comparison
struct SamtoolsViewCommand {
    bam_path: String,
    flag: u16,
}

impl BenchmarkCommand for SamtoolsViewCommand {
    fn name(&self) -> String {
        format!("samtools view -f 0x{:x}", self.flag)
    }

    fn run_monitored(&self, thread_count: usize) -> Result<BenchmarkResult> {
        let mut cmd = Command::new(SAMTOOLS);
        cmd.args(&[
            "view",
            "-h",
            "-f",
            &format!("0x{:x}", self.flag),
            &self.bam_path,
        ]);

        // Add thread parameter if not auto (0)
        if thread_count > 1 {
            cmd.args(&["-@", &(thread_count - 1).to_string()]);
        }

        let (duration, _record_count, memory_stats, cpu_stats, sample_count, memory_samples) =
            run_with_monitoring(cmd)?;

        Ok(BenchmarkResult {
            strategy: self.name(),
            thread_count,
            duration,
            peak_memory_mb: memory_stats.0,
            avg_memory_mb: memory_stats.1,
            peak_cpu_percent: cpu_stats.0,
            avg_cpu_percent: cpu_stats.1,
            index_size_mb: 0.0, // No index created for view operations
            record_count: 0,    // View operations don't count records directly
            sample_count,
            memory_samples,
        })
    }
}

/// View benchmark suite
struct ViewBenchmarkSuite {
    bam_files: Vec<String>,
    flags: Vec<u16>,
}

impl ViewBenchmarkSuite {
    fn new(bam_str: &str, flags_str: &str) -> Result<Self> {
        let bam_files: Vec<String> = bam_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let flags: Result<Vec<u16>, _> = flags_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| {
                if s.starts_with("0x") || s.starts_with("0X") {
                    u16::from_str_radix(&s[2..], 16)
                } else {
                    s.parse::<u16>()
                }
            })
            .collect();

        let flags = flags.map_err(|e| anyhow::anyhow!("Failed to parse flags: {}", e))?;

        // Validate BAM files exist
        for bam_file in &bam_files {
            if !Path::new(bam_file).exists() {
                return Err(anyhow::anyhow!("BAM file not found: {}", bam_file));
            }
        }

        Ok(ViewBenchmarkSuite { bam_files, flags })
    }
}

impl BenchmarkSuite for ViewBenchmarkSuite {
    fn commands(&self) -> Vec<Box<dyn BenchmarkCommand>> {
        let mut commands: Vec<Box<dyn BenchmarkCommand>> = Vec::new();

        for bam_file in &self.bam_files {
            for &flag in &self.flags {
                // Check if index exists for bafiq (required for view operations)
                let index_path = format!("{}.bfi", bam_file);
                if Path::new(&index_path).exists() {
                    commands.push(Box::new(BafiqViewCommand {
                        bam_path: bam_file.clone(),
                        flag,
                    }));
                } else {
                    eprintln!(
                        "Warning: Index not found for {}, skipping bafiq view",
                        bam_file
                    );
                    eprintln!("   Expected: {}", index_path);
                    eprintln!("   Please build index first: bafiq index \"{}\"", bam_file);
                }

                // Add samtools for comparison (if available)
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
        let bam_names: Vec<String> = self
            .bam_files
            .iter()
            .map(|path| {
                Path::new(path)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
            })
            .collect();

        let flag_strs: Vec<String> = self
            .flags
            .iter()
            .map(|&flag| format!("0x{:x}", flag))
            .collect();

        format!(
            "view operations with BAM files: [{}], flags: [{}]",
            bam_names.join(", "),
            flag_strs.join(", ")
        )
    }
}

fn main() {
    // Parse CLI arguments
    let args = ViewBenchArgs::parse();

    // Create configuration from parsed args
    let config = match BenchmarkOrchestrator::create_config_from_args(&args.common) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error: Configuration error: {}", e);
            std::process::exit(1);
        }
    };

    // Create the view benchmark suite
    let suite = match ViewBenchmarkSuite::new(&args.bam, &args.flags) {
        Ok(suite) => Box::new(suite),
        Err(e) => {
            eprintln!("Error: Failed to create view benchmark suite: {}", e);
            std::process::exit(1);
        }
    };

    // Create orchestrator with the parsed configuration
    let orchestrator = BenchmarkOrchestrator::with_config(config);

    println!("Running view benchmarks...");
    match orchestrator.run_suite(suite) {
        Ok(()) => println!("\nView benchmarks completed successfully"),
        Err(e) => {
            eprintln!("Error: View benchmarks failed: {}", e);
            std::process::exit(1);
        }
    }
}
