use crate::FlagIndex;
use anyhow::Result;

// Re-export all strategy implementations
pub mod parallel_streaming;
pub mod rayon_wait_free;
pub mod sequential;
pub mod shared;

// Common types used across strategies
#[derive(Clone)]
pub struct BlockInfo {
    pub start_pos: usize,
    pub total_size: usize,
}

/// Strategy trait for different indexing approaches
pub trait IndexingStrategy {
    /// Build a flag index from a BAM file
    fn build(&self, bam_path: &str) -> Result<FlagIndex>;

    /// Get the strategy name for identification
    fn name(&self) -> &'static str;
}

// Constants shared across strategies
pub const BGZF_HEADER_SIZE: usize = 18;
pub const BGZF_FOOTER_SIZE: usize = 8;
pub const BGZF_BLOCK_MAX_SIZE: usize = 65536;
