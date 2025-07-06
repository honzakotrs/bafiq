use anyhow::Result;
use libdeflater::Decompressor;
use memmap2::Mmap;
use std::fs::File;
use std::sync::Arc;


use super::shared::extract_flags_from_block_pooled;
use super::{IndexingStrategy, BGZF_BLOCK_MAX_SIZE};
use crate::FlagIndex;

/// **ZERO-MERGE STRATEGY** - Optimized parallel merge tree + proven infrastructure
/// 
/// **BREAKTHROUGH APPROACH FOR LARGE FILES:**
/// - Uses the proven shared flag extraction infrastructure (no custom BAM parsing)
/// - Combines discover-all-first approach with optimized parallel merge tree
/// - Eliminates O(n²) sequential merge bottleneck via divide-and-conquer merging
/// - Scales logarithmically with thread count instead of linearly
/// 
/// **Performance Characteristics:**
/// - **Merge complexity**: O(log n) instead of O(n²) - massive improvement for many threads
/// - **Correctness**: Uses battle-tested shared infrastructure (no parsing bugs)
/// - **Scalability**: Better scaling on high core count systems
/// - **Reliability**: Consistent with other strategies (identical index output)
/// 
/// **Target Use Case:**
/// - High core count systems (16+ cores) where sequential merge becomes bottleneck
/// - Validation of parallel merge tree benefits
/// - Proof of concept for O(log n) merge scaling
pub struct ZeroMergeStrategy;

impl IndexingStrategy for ZeroMergeStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);
        
        // **ZERO-MERGE APPROACH: Use proven infrastructure + parallel merge tree**
        // Discovery phase: find all blocks first (like rayon_wait_free)
        let blocks = super::shared::discover_blocks_fast(&data)?;
        
        // **PARALLEL PROCESSING WITH PROVEN SHARED INFRASTRUCTURE**
        use rayon::prelude::*;
        
        let local_indexes: Vec<FlagIndex> = blocks
            .par_iter()
            .map(|block_info| -> Result<FlagIndex> {
                let mut local_index = FlagIndex::new();
                let block = &data[block_info.start_pos..block_info.start_pos + block_info.total_size];
                let block_offset = block_info.start_pos as i64;
                
                // **USE PROVEN SHARED INFRASTRUCTURE**
                // This is the same battle-tested code used by all other strategies
                thread_local! {
                    static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                    static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
                }
                
                BUFFER.with(|buf| {
                    DECOMPRESSOR.with(|decomp| {
                        let mut buffer = buf.borrow_mut();
                        let mut decompressor = decomp.borrow_mut();
                        extract_flags_from_block_pooled(
                            block,
                            &mut local_index,
                            block_offset,
                            &mut buffer,
                            &mut decompressor,
                        )
                    })
                })?;
                
                Ok(local_index)
            })
            .collect::<Result<Vec<_>, _>>()?;
        
        // **ZERO-MERGE FINAL STEP: Use optimized parallel merge tree**
        // This provides the scalability benefit without the custom parsing bugs
        let final_index = FlagIndex::merge_parallel(local_indexes);
        
        Ok(final_index)
    }
}

 