use std::fs::File;
use std::sync::Arc;
use memmap2::Mmap;
use rayon::prelude::*;
use anyhow::Result;

use crate::FlagIndex;
use super::{IndexingStrategy, BGZF_BLOCK_MAX_SIZE};
use super::shared::{discover_blocks_fast, extract_flags_from_block_pooled};

/// **WAIT-FREE STRATEGY** - Eliminates the 51% __psynch_cvwait bottleneck
/// 
/// Key optimizations to remove blocking:
/// - NO channels, queues, or condition variables
/// - NO thread::yield_now() or blocking operations
/// - Pure work-stealing via rayon without additional synchronization
/// - Pre-discover all blocks to eliminate discovery/processing coordination
/// - Direct work division instead of work queues
pub struct RayonWaitFreeStrategy;

impl IndexingStrategy for RayonWaitFreeStrategy {
    fn name(&self) -> &'static str {
        "rayon-wait-free"
    }

    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);
        
        // **PHASE 1: SINGLE-THREADED BLOCK DISCOVERY** 
        // Pre-discover all blocks to eliminate coordination overhead
        let blocks = discover_blocks_fast(&data)?;
        
        // **PHASE 2: MAXIMUM PARALLEL PROCESSING** 
        // Scale to utilize all CPU cores effectively  
        let local_indexes: Vec<FlagIndex> = blocks
            .par_iter()  // Process each block in parallel across all cores
            .map(|block_info| -> Result<FlagIndex> {
                let mut local_index = FlagIndex::new();
                let block = &data[block_info.start_pos..block_info.start_pos + block_info.total_size];
                let block_offset = block_info.start_pos as i64;
               
               // **THREAD-LOCAL PROCESSING** - no cross-thread communication
               thread_local! {
                   static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                   static DECOMPRESSOR: std::cell::RefCell<libdeflater::Decompressor> = std::cell::RefCell::new(libdeflater::Decompressor::new());
               }
               
               // **WAIT-FREE PROCESSING** - no blocking operations
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
        
        // **PHASE 3: SINGLE-THREADED MERGE**
        // Simple sequential merge - no parallelization overhead
        let mut final_index = FlagIndex::new();
        for local_index in local_indexes {
            final_index.merge(local_index);
        }
        
        Ok(final_index)
    }
} 