use anyhow::{anyhow, Result};
use libdeflater::Decompressor;
use memmap2::Mmap;
use std::fs::File;

/// Constants for BGZF structure.
const BGZF_HEADER_SIZE: usize = 18;
const BGZF_FOOTER_SIZE: usize = 8;

/// Counts the number of BAM records in a full BGZF block
/// by minimally decompressing only the deflate portion of the block.
/// This routine extracts the raw deflate stream (skipping the
/// gzip header and footer) and uses libdeflater for fast decompression.
/// It then walks the decompressed bytes in an unsafe block to avoid
/// unnecessary bounds checks.
///
/// # Arguments
/// * `block` - A slice containing the full BGZF block.
///
/// # Returns
/// The number of BAM records (reads) in the block.
pub fn count_bam_records_in_bgzf_block_minimal(block: &[u8]) -> Result<usize> {
    // Ensure the block is long enough.
    if block.len() < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE {
        return Err(anyhow!("BGZF block is too short"));
    }

    // Extract only the deflate-compressed portion.
    let compressed_data = &block[BGZF_HEADER_SIZE..block.len() - BGZF_FOOTER_SIZE];

    // Use libdeflater for fast decompression.
    // Allocate a buffer large enough for the maximum uncompressed BGZF block size.
    let mut output = vec![0u8; 65536]; // 64 KB is the BGZF upper limit.
    let mut decompressor = Decompressor::new();
    let decompressed_size = decompressor
        .gzip_decompress(compressed_data, &mut output)
        .map_err(|e| anyhow!("Decompression failed: {:?}", e))?;

    // Count BAM records by walking the decompressed data.
    // Each record begins with a 4-byte little-endian length.
    let mut record_count = 0;
    let mut pos = 0;
    unsafe {
        // Get a raw pointer to the beginning of the decompressed data.
        let out_ptr = output.as_ptr();
        while pos + 4 <= decompressed_size {
            // Read 4 bytes unaligned and interpret as little-endian.
            let rec_size =
                u32::from_le(std::ptr::read_unaligned(out_ptr.add(pos) as *const u32)) as usize;
            // If we don’t have the full record, break.
            if pos + 4 + rec_size > decompressed_size {
                break;
            }
            pos += 4 + rec_size;
            record_count += 1;
        }
    }

    Ok(record_count)
}

/// Counts the total number of BAM records in a BGZF-compressed BAM file.
/// This version memory–maps the entire file (using memmap2) and then
/// iterates through it block–by–block, reassembling each BGZF block,
/// decompressing it with libdeflater, and counting the contained records.
pub fn count_bam_records_in_bam_file_minimal(bam_path: &str) -> Result<usize> {
    // Open and memory–map the BAM file.
    let file = File::open(bam_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let data = &mmap[..];

    let mut total_records = 0;
    let mut pos = 0;

    // Iterate over the memory–mapped file.
    while pos < data.len() {
        // Ensure there is enough data for a full BGZF header.
        if pos + BGZF_HEADER_SIZE > data.len() {
            break; // No more complete block header.
        }
        let header = &data[pos..pos + BGZF_HEADER_SIZE];

        // Validate the GZIP magic bytes.
        if header[0..2] != [0x1f, 0x8b] {
            return Err(anyhow!("Invalid GZIP header in BGZF block"));
        }

        // Extract BSIZE (bytes 16-17); BSIZE = total block size - 1.
        let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
        let total_block_size = bsize + 1;

        // Sanity-check the block size.
        if total_block_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_block_size > 65536 {
            return Err(anyhow!("Invalid BGZF block size: {}", total_block_size));
        }
        if pos + total_block_size > data.len() {
            break; // Incomplete block at the end.
        }

        // Get the full BGZF block.
        let block = &data[pos..pos + total_block_size];
        let count = count_bam_records_in_bgzf_block_minimal(block)?;
        total_records += count;

        pos += total_block_size;
    }

    Ok(total_records)
}
