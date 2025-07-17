use anyhow::Result;
use clap::Parser;

mod benchmark;
mod shared;

use benchmark::{cli::BenchmarkArgs, index::run_index_benchmark, view::run_view_benchmark};

fn main() -> Result<()> {
    // Parse CLI arguments
    let args = BenchmarkArgs::parse();

    // Dispatch to appropriate benchmark module
    match args.command {
        benchmark::cli::BenchmarkCommands::Index(index_args) => {
            run_index_benchmark(index_args)?;
        }
        benchmark::cli::BenchmarkCommands::View(view_args) => {
            run_view_benchmark(view_args)?;
        }
    }

    Ok(())
}
