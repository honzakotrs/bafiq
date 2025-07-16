use anyhow::Result;

pub mod analysis;
pub mod commands;
pub mod orchestrator;

// Re-export commonly used types
pub use analysis::BenchmarkResult;
pub use commands::{format_thread_display, format_thread_with_suffix};
pub use orchestrator::BenchmarkOrchestrator;

// Strategy name constants
pub const BAFIQ_FAST_COUNT: &str = "bafiq-fast-count";
pub const SAMTOOLS: &str = "samtools";

/// Core benchmark configuration shared across all benchmark types
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub thread_counts: Vec<usize>,
    pub sample_size: usize,
    pub keep_temp_files: bool,
    pub clear_cache: bool,
}

/// A single benchmark command that can be executed with monitoring
pub trait BenchmarkCommand {
    /// Display name for this command (e.g., "bafiq index work-stealing (4 threads)")
    fn name(&self) -> String;

    /// Execute this command with monitoring and return detailed metrics
    fn run_monitored(&self, thread_count: usize) -> Result<BenchmarkResult>;
}

/// Verification result from comparing benchmark outputs
#[derive(Debug)]
pub enum VerificationResult {
    Passed(String),
    Failed(String),
    Skipped(String),
}

/// A complete benchmark suite (e.g., index building, view operations)
pub trait BenchmarkSuite {
    /// All commands to benchmark in this suite
    fn commands(&self) -> Vec<Box<dyn BenchmarkCommand>>;

    /// Verify that all results are consistent (e.g., same record counts, same outputs)
    fn verify_results(&self, results: &[BenchmarkResult]) -> VerificationResult;

    /// Display name for this benchmark suite
    fn suite_name(&self) -> &str;

    /// Input files or parameters for this suite
    fn input_description(&self) -> String;
}
