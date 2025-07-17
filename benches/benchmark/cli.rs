use clap::{Parser, Subcommand};

/// Unified benchmark CLI with subcommands
#[derive(Parser, Debug)]
#[command(name = "benchmark")]
#[command(about = "Bafiq comprehensive benchmark suite")]
pub struct BenchmarkArgs {
    #[command(subcommand)]
    pub command: BenchmarkCommands,
}

/// Available benchmark subcommands
#[derive(Subcommand, Debug)]
pub enum BenchmarkCommands {
    /// Index building benchmarks with thread scaling
    Index(IndexBenchArgs),
    /// View operation benchmarks with different flags
    View(ViewBenchArgs),
}

/// Index benchmark arguments
#[derive(Parser, Debug)]
pub struct IndexBenchArgs {
    #[command(flatten)]
    pub common: crate::shared::orchestrator::BenchmarkArgs,

    /// BAM file to benchmark (required)
    #[arg(long)]
    pub bam: String,
}

/// View benchmark arguments  
#[derive(Parser, Debug)]
pub struct ViewBenchArgs {
    #[command(flatten)]
    pub common: crate::shared::orchestrator::BenchmarkArgs,

    /// BAM file(s) to benchmark (comma-separated)
    #[arg(long)]
    pub bam: String,

    /// Flags to test (comma-separated hex values), e.g., "0x4,0x2"
    #[arg(long, default_value = "0x4")]
    pub flags: String,
}
