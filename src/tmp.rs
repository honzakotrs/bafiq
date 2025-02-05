use anyhow::{anyhow, Result};
use libdeflater::Decompressor;
use memmap2::Mmap;
use std::fs::File;
use std::ptr;

const BGZF_BLOCK_MAX_SIZE: usize = 65536; // Maximum uncompressed BGZF block size
const BGZF_HEADER_SIZE: usize = 18;
const BGZF_FOOTER_SIZE: usize = 8;

pub fn count_bam_records_in_bgzf_block_minimal(block: &[u8]) -> Result<usize> {
    // Instead of stripping the header and footer, use the full block.
    let mut output = vec![0u8; BGZF_BLOCK_MAX_SIZE];
    let mut decompressor = Decompressor::new();
    let decompressed_size = decompressor
        .gzip_decompress(block, &mut output)
        .map_err(|e| anyhow!("Decompression failed: {:?}", e))?;

    // Now process the decompressed output to count BAM records.
    let mut record_count = 0;
    let mut pos = 0;
    unsafe {
        let out_ptr = output.as_ptr();
        while pos + 4 <= decompressed_size {
            let rec_size =
                u32::from_le(ptr::read_unaligned(out_ptr.add(pos) as *const u32)) as usize;
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
