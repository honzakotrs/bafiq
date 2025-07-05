use crate::bgzf::BGZF_BLOCK_MAX_SIZE;
use crate::index::strategies::shared::{discover_blocks_fast, extract_flags_from_block_pooled};
use crate::index::strategies::IndexingStrategy;
use crate::FlagIndex;
use anyhow::Result;
use libdeflater::Decompressor;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::sync::Arc;

/// Rayon-based parallel processing strategy (inspired by fast-count 2.3s performance)
///
/// Uses rayon's parallel iterators to discover and process BGZF blocks, similar to the
/// fast-count methods that achieve 2.3s performance, but builds a complete index.
///
/// **Architecture:**
/// - Phase 1: Discover all BGZF blocks (fast, single-threaded)
/// - Phase 2: Process blocks in parallel using rayon (like fast-count methods)
/// - Phase 3: Merge all local indexes
///
/// **Characteristics:**
/// - Classic batch processing approach
/// - Memory usage: High (stores all block locations)
/// - Latency: Higher initial latency due to full discovery phase
/// - Throughput: Excellent parallel processing once blocks are discovered
/// - Suitable for: Large files where parallelism benefits outweigh memory usage
pub struct RayonOptimizedStrategy;

impl IndexingStrategy for RayonOptimizedStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);

        // Phase 1: Discover all BGZF blocks (fast, single-threaded)
        let blocks = discover_blocks_fast(&data)?;

        // Phase 2: Process blocks in parallel using rayon (like fast-count methods)
        let local_indexes: Vec<FlagIndex> = blocks
            .par_iter()
            .map(|block_info| -> Result<FlagIndex> {
                let mut local_index = FlagIndex::new();
                let block = &data[block_info.start_pos..block_info.start_pos + block_info.total_size];
                let block_offset = block_info.start_pos as i64;

                // Use thread-local buffers for efficiency (like fast-count)
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

        // Phase 3: Merge all local indexes
        let mut final_index = FlagIndex::new();
        for local_index in local_indexes {
            final_index.merge(local_index);
        }

        Ok(final_index)
    }
}
