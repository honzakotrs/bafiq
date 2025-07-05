use anyhow::{anyhow, Result};
use libdeflater::Decompressor;
use memmap2::Mmap;
use std::fs::File;

use super::shared::extract_flags_from_block_pooled;
use super::{IndexingStrategy, BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};
use crate::FlagIndex;

/// Build index using sequential processing (fallback strategy)
///
/// Single-threaded approach that processes BGZF blocks one at a time.
/// Lower memory usage but slower performance.
pub struct SequentialStrategy;

impl IndexingStrategy for SequentialStrategy {
    fn name(&self) -> &'static str {
        "sequential"
    }

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
