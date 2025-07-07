use crate::FlagIndex;
use anyhow::{anyhow, Result};
use clap::ValueEnum;

use std::path::Path;

// Import strategies
use crate::index::strategies::{
    channel_pc::ChannelProducerConsumerStrategy, constant_memory::ConstantMemoryStrategy,
    work_stealing::WorkStealingStrategy, IndexingStrategy,
};

/// Available index building strategies
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum BuildStrategy {
    /// Channel-based producer-consumer - crossbeam channels architecture
    #[value(name = "channel-producer-consumer")]
    ChannelProducerConsumer,
    /// Work-stealing processing - fastest performing approach
    #[value(name = "work-stealing")]
    WorkStealing,
    /// Constant memory processing - constant RAM footprint for any file size
    #[value(name = "constant-memory")]
    ConstantMemory,
}

/// Primary interface for building flag indexes with different strategies
pub struct IndexBuilder {
    strategy: BuildStrategy,
}

impl BuildStrategy {
    /// Get the canonical name for this strategy (used in benchmarks and CLI)
    pub fn name(&self) -> &'static str {
        match self {
            BuildStrategy::ChannelProducerConsumer => "channel_producer_consumer",
            BuildStrategy::WorkStealing => "work_stealing",
            BuildStrategy::ConstantMemory => "memory_friendly",
        }
    }

    /// Get all available strategies for benchmarking
    pub fn all_strategies() -> Vec<BuildStrategy> {
        vec![
            BuildStrategy::ChannelProducerConsumer,
            BuildStrategy::WorkStealing,
            BuildStrategy::ConstantMemory,
        ]
    }

    /// Get strategies suitable for routine benchmarking (excludes slow ones)
    pub fn benchmark_strategies() -> Vec<BuildStrategy> {
        Self::all_strategies().into_iter().collect()
    }
}

impl Default for BuildStrategy {
    fn default() -> Self {
        BuildStrategy::WorkStealing
    }
}

impl IndexBuilder {
    /// Create a new index builder with default strategy
    pub fn new() -> Self {
        Self {
            strategy: BuildStrategy::default(),
        }
    }

    /// Create a new index builder with specified strategy
    pub fn with_strategy(strategy: BuildStrategy) -> Self {
        Self { strategy }
    }

    /// Build an index from a BAM file path
    pub fn build<P: AsRef<Path>>(&self, bam_path: P) -> Result<FlagIndex> {
        let path_str = bam_path
            .as_ref()
            .to_str()
            .ok_or_else(|| anyhow!("Invalid file path"))?;

        match self.strategy {
            BuildStrategy::ChannelProducerConsumer => {
                ChannelProducerConsumerStrategy.build(path_str)
            }
            BuildStrategy::WorkStealing => WorkStealingStrategy.build(path_str),
            BuildStrategy::ConstantMemory => ConstantMemoryStrategy.build(path_str),
        }
    }

    /// Get the current build strategy
    pub fn strategy(&self) -> BuildStrategy {
        self.strategy
    }
}

impl Default for IndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}
