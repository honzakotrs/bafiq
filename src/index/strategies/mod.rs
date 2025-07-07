use crate::FlagIndex;
use anyhow::Result;

// Re-export all strategy implementations
pub mod channel_pc;
pub mod constant_memory;
pub mod work_stealing;

pub trait IndexingStrategy {
    /// Build a flag index from a BAM file path
    fn build(&self, bam_path: &str) -> Result<FlagIndex>;
}
