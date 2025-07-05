use anyhow::{anyhow, Result};
use libdeflater::Decompressor;
use memmap2::Mmap;
use std::fs::File;

use super::shared::extract_flags_from_block_pooled;
use super::{IndexingStrategy, BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};
use crate::FlagIndex;

/// **SEQUENTIAL STRATEGY** - Single-threaded baseline for measuring parallel benefits
///
/// **Purpose as Reference Implementation:**
/// - Establishes baseline performance for measuring parallel strategy gains
/// - Simplest possible implementation for correctness verification
/// - Minimal memory usage footprint for resource-constrained environments
/// - Fallback option when parallel strategies fail or aren't available
///
/// **What We Learned from Parallel Strategies:**
/// - Best parallel strategies (rayon_wait_free) achieve ~1.5-2x speedup over sequential
/// - Complex "optimizations" can actually be slower than simple parallel approaches
/// - Memory usage scales with parallelization (1.3GB+ vs minimal for sequential)
/// - Thread coordination overhead can negate parallel benefits if not done carefully
///
/// **When to Use Sequential:**
/// - Single-core environments or extreme memory constraints
/// - Debugging and correctness verification (simplest code path)
/// - When predictable, steady performance is more important than speed
/// - Resource-limited environments where 1.3GB+ RAM isn't available
///
/// **Architecture:**
/// - Discovery: Inline block discovery while processing
/// - Processing: Single-threaded with thread-local buffer reuse
/// - Memory: Minimal allocation overhead
/// - Decompression: Single libdeflater instance with buffer reuse
///
/// **Expected Performance:**
/// - Time: ~5-7s (estimated 1.5-2x slower than fastest parallel)
/// - Memory: <500MB (minimal memory footprint)
/// - CPU: ~100% single-core utilization
/// - Suitable for: Baseline measurement and resource-constrained environments
pub struct SequentialStrategy;

impl IndexingStrategy for SequentialStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = &mmap[..];

        let mut index = FlagIndex::new();
        let mut _total_records = 0;
        let mut pos = 0;

        // PERFORMANCE FIX: Use thread-local buffers for sequential processing (single-threaded, but safe)
        thread_local! {
            static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
            static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
        }

        // Iterate over the memory-mapped file
        while pos < data.len() {
            // Ensure there is enough data for a full BGZF header
            if pos + BGZF_HEADER_SIZE > data.len() {
                break; // No more complete block header
            }
            let header = &data[pos..pos + BGZF_HEADER_SIZE];

            // Validate the GZIP magic bytes
            if header[0..2] != [0x1f, 0x8b] {
                return Err(anyhow!("Invalid GZIP header in BGZF block"));
            }

            // Extract BSIZE (bytes 16-17); BSIZE = total block size - 1
            let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
            let total_block_size = bsize + 1;

            // Sanity-check the block size
            if total_block_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_block_size > 65536 {
                return Err(anyhow!("Invalid BGZF block size: {}", total_block_size));
            }
            if pos + total_block_size > data.len() {
                break; // Incomplete block at the end
            }

            // Get the full BGZF block and extract flags using efficient buffer reuse
            let block = &data[pos..pos + total_block_size];
            let block_offset = pos as i64; // Virtual file offset for this block

            let count = BUFFER.with(|buf| {
                DECOMPRESSOR.with(|decomp| {
                    let mut buffer = buf.borrow_mut();
                    let mut decompressor = decomp.borrow_mut();
                    extract_flags_from_block_pooled(
                        block,
                        &mut index,
                        block_offset,
                        &mut buffer,
                        &mut decompressor,
                    )
                })
            })?;
            _total_records += count;

            pos += total_block_size;
        }

        Ok(index)
    }
}
