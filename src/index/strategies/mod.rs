use crate::FlagIndex;
use anyhow::Result;

// Re-export all strategy implementations
pub mod channel_pc;
pub mod constant_memory;
pub mod shared;
pub mod work_stealing;

// Common types used across strategies
#[derive(Clone)]
pub struct BlockInfo {
    pub start_pos: usize,
    pub total_size: usize,
}

/// Common trait for all indexing strategies
pub trait IndexingStrategy {
    /// Build a flag index from a BAM file path
    fn build(&self, bam_path: &str) -> Result<FlagIndex>;
}
