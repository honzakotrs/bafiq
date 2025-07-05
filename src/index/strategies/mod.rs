use crate::FlagIndex;
use anyhow::Result;

// Re-export remaining strategy implementations
pub mod htslib;
pub mod parallel_chunk_streaming;
pub mod parallel_streaming;
pub mod rayon_streaming_optimized;
pub mod rayon_wait_free;
pub mod sequential;
pub mod shared;

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

// Constants shared across strategies
pub const BGZF_HEADER_SIZE: usize = 18;
pub const BGZF_FOOTER_SIZE: usize = 8;
pub const BGZF_BLOCK_MAX_SIZE: usize = 65536;
