use crate::FlagIndex;
use crate::bgzf::{
    BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE, BlockInfo, is_bgzf_header,
};
use anyhow::{Result, anyhow};
use libdeflater::Decompressor;
use std::ptr;

/// Information about an incomplete BAM record header at the end of a block
#[derive(Debug, Clone)]
pub struct IncompleteHeader {
    /// Partial header bytes (up to 16 bytes)
    pub partial_data: Vec<u8>,
    /// Number of bytes needed to complete the header (out of 16 total)
    pub bytes_needed: usize,
}

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
/// Returns (record_count, incomplete_header_info)
/// incomplete_header_info is Some if the last record header is incomplete
pub fn extract_flags_from_block_pooled(
    block: &[u8],
    index: &mut FlagIndex,
    block_offset: i64,
    output_buffer: &mut Vec<u8>,
    decompressor: &mut Decompressor,
) -> Result<(usize, Option<IncompleteHeader>)> {
    let mut record_count = 0;
    let block_id = block_offset;

    // Decompress using provided buffer and decompressor
    let decompressed_size = decompressor
        .gzip_decompress(block, output_buffer)
        .map_err(|e| anyhow!("Decompression failed: {:?}", e))?;

    // Skip BAM header blocks (they don't contain read records)
    if decompressed_size >= 4 && &output_buffer[0..4] == b"BAM\x01" {
        return Ok((0, None));
    }

    // Optimized record parsing with better memory access patterns
    let incomplete = extract_flags_from_decompressed_mem_optimized(
        output_buffer,
        decompressed_size,
        index,
        block_id,
        &mut record_count,
    )?;

    Ok((record_count, incomplete))
}

/// Memory-optimized record parsing with prefetching and better memory access patterns
/// Returns Some(IncompleteHeader) if a record header is split across block boundary
#[inline(always)]
pub fn extract_flags_from_decompressed_mem_optimized(
    output_buffer: &[u8],
    decompressed_size: usize,
    index: &mut FlagIndex,
    block_id: i64,
    record_count: &mut usize,
) -> Result<Option<IncompleteHeader>> {
    let mut pos = 0;
    // BAM record header size to read flags (SAM spec v1.6 Section 4.2):
    // block_size(4) + refID(4) + pos(4) + l_read_name(1) + mapq(1) +
    // bin(2) + n_cigar_op(2) + flag(2) = 20 bytes to read the flag field
    const HEADER_SIZE: usize = 20;

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

            // Check if we have enough space for the full record
            if pos + 4 + rec_size > decompressed_size {
                // INCOMPLETE RECORD: Check if we at least have the header (16 bytes total)
                let available = decompressed_size - pos;

                if available < HEADER_SIZE {
                    // Incomplete header - need to combine with next block
                    let partial_data = output_buffer[pos..decompressed_size].to_vec();
                    return Ok(Some(IncompleteHeader {
                        partial_data,
                        bytes_needed: HEADER_SIZE - available,
                    }));
                }
                // We have the header but not the full record - that's fine, we only need flags
                // Fall through to extract flags from the header
            }

            // We need at least 16 bytes to read the flag field at offset 14-15
            if rec_size >= 16 && pos + 4 + 16 <= decompressed_size {
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

    Ok(None)
}

/// Process an incomplete header by combining partial data from previous block
/// with decompressed data from the next block
pub fn process_incomplete_header(
    incomplete: &IncompleteHeader,
    next_block_decompressed_data: &[u8],
    index: &mut FlagIndex,
    block_id: i64,
) -> Result<()> {
    // BAM record header size to read flags (SAM spec v1.6 Section 4.2):
    // block_size(4) + refID(4) + pos(4) + l_read_name(1) + mapq(1) +
    // bin(2) + n_cigar_op(2) + flag(2) = 20 bytes to read the flag field
    const HEADER_SIZE: usize = 20;

    // Ensure we have enough bytes from next block
    if next_block_decompressed_data.len() < incomplete.bytes_needed {
        // Next block doesn't have enough data either - skip this record
        return Ok(());
    }

    // Combine partial header with data from next block
    let mut complete_header = incomplete.partial_data.clone();
    complete_header.extend_from_slice(&next_block_decompressed_data[..incomplete.bytes_needed]);

    if complete_header.len() < HEADER_SIZE {
        // Still not enough - skip
        return Ok(());
    }

    // Extract flags from complete header
    unsafe {
        let header_ptr = complete_header.as_ptr();

        // Read record size (first 4 bytes)
        let rec_size = u32::from_le(ptr::read_unaligned(header_ptr as *const u32)) as usize;

        // Validate that this is a reasonable record
        if rec_size >= 16 {
            // Extract flags at offset 14-15 (offset 10-11 in record body after 4-byte size)
            let flags_ptr = header_ptr.add(4 + 14) as *const u16;
            let flags = u16::from_le(ptr::read_unaligned(flags_ptr));

            index.add_record_at_block(flags, block_id);
        }
    }

    Ok(())
}

/// Helper function to process an incomplete header with a compressed next block
/// This decompresses the next block and processes the boundary record
pub fn process_incomplete_with_compressed_block(
    incomplete: &IncompleteHeader,
    compressed_next_block: &[u8],
    block_id: i64,
) -> Result<FlagIndex> {
    use crate::bgzf::BGZF_BLOCK_MAX_SIZE;

    let mut boundary_index = FlagIndex::new();

    thread_local! {
        static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
        static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
    }

    BUFFER.with(|buf| {
        DECOMPRESSOR.with(|decomp| {
            let mut buffer = buf.borrow_mut();
            let mut decompressor = decomp.borrow_mut();

            // Decompress next block
            let decompressed_size = decompressor
                .gzip_decompress(compressed_next_block, &mut buffer)
                .map_err(|e| anyhow!("Decompression failed: {:?}", e))?;

            // Process the incomplete header with decompressed data
            process_incomplete_header(
                incomplete,
                &buffer[..decompressed_size],
                &mut boundary_index,
                block_id,
            )
        })
    })?;

    Ok(boundary_index)
}
