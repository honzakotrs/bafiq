use crate::FlagIndex;
use crate::bgzf::{
    BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE, BlockInfo, is_bgzf_header,
};
use anyhow::{Result, anyhow};
use libdeflater::Decompressor;
use std::ptr;

pub fn discover_blocks_fast(data: &[u8]) -> Result<Vec<BlockInfo>> {
    let mut blocks = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        if pos + BGZF_HEADER_SIZE > data.len() {
            break;
        }

        let header = &data[pos..pos + BGZF_HEADER_SIZE];
        if !is_bgzf_header(header) {
            return Err(anyhow!("Invalid GZIP header at position {}", pos));
        }

        let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
        let total_size = bsize + 1;

        if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > BGZF_BLOCK_MAX_SIZE {
            return Err(anyhow!("Invalid BGZF block size: {}", total_size));
        }

        if pos + total_size > data.len() {
            break;
        }

        blocks.push(BlockInfo {
            start_pos: pos,
            total_size,
        });

        pos += total_size;
    }

    Ok(blocks)
}

/// Extract flags from BAM records using provided buffers (memory-efficient version)
///
/// Same as extract_flags_from_block but uses caller-provided buffers to avoid allocations
pub fn extract_flags_from_block_pooled(
    block: &[u8],
    index: &mut FlagIndex,
    block_offset: i64,
    output_buffer: &mut Vec<u8>,
    decompressor: &mut Decompressor,
) -> Result<usize> {
    let mut record_count = 0;
    let block_id = block_offset;

    // Decompress using provided buffer and decompressor
    let decompressed_size = decompressor
        .gzip_decompress(block, output_buffer)
        .map_err(|e| anyhow!("Decompression failed: {:?}", e))?;

    // Skip BAM header blocks (they don't contain read records)
    if decompressed_size >= 4 && &output_buffer[0..4] == b"BAM\x01" {
        return Ok(0);
    }

    // Optimized record parsing with better memory access patterns
    extract_flags_from_decompressed_simd_optimized(
        output_buffer,
        decompressed_size,
        index,
        block_id,
        &mut record_count,
    )?;

    Ok(record_count)
}

/// SIMD-optimized record parsing with prefetching and better memory access patterns
#[inline(always)]
pub fn extract_flags_from_decompressed_simd_optimized(
    output_buffer: &[u8],
    decompressed_size: usize,
    index: &mut FlagIndex,
    block_id: i64,
    record_count: &mut usize,
) -> Result<()> {
    let mut pos = 0;

    // Process records in chunks for better cache locality
    unsafe {
        let out_ptr = output_buffer.as_ptr();
        let end_ptr = out_ptr.add(decompressed_size);

        while pos + 4 <= decompressed_size {
            // Prefetch next cache line to improve memory access patterns (x86_64 only)
            #[cfg(target_arch = "x86_64")]
            {
                const PREFETCH_DISTANCE: usize = 64; // Cache line size for optimal prefetching
                if pos + PREFETCH_DISTANCE < decompressed_size {
                    // Manual prefetch hint for better cache performance
                    std::ptr::read_volatile(out_ptr.add(pos + PREFETCH_DISTANCE));
                }
            }

            let rec_size_ptr = out_ptr.add(pos) as *const u32;

            // Bounds check before reading
            if rec_size_ptr >= end_ptr as *const u32 {
                break;
            }

            let rec_size = u32::from_le(ptr::read_unaligned(rec_size_ptr)) as usize;

            // Validate record size
            if pos + 4 + rec_size > decompressed_size {
                break;
            }

            // We need at least 16 bytes to read the flag field at offset 14-15
            if rec_size >= 16 {
                let record_body_ptr = out_ptr.add(pos + 4);

                // Direct memory access to flags at offset 14-15
                let flags_ptr = record_body_ptr.add(14) as *const u16;
                let flags = u16::from_le(ptr::read_unaligned(flags_ptr));

                index.add_record_at_block(flags, block_id);
                *record_count += 1;
            }

            pos += 4 + rec_size;
        }
    }

    Ok(())
}
