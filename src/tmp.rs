use anyhow::{anyhow, Result};
use flate2::{Decompress, FlushDecompress, Status};
use std::fs::File;
use std::io::{BufReader, Read};

/// Constants for BGZF structure.
const BGZF_HEADER_SIZE: usize = 18;
const BGZF_FOOTER_SIZE: usize = 8;

/// Counts the number of BAM records in a full BGZF block
/// by minimally decompressing only the deflate portion of the block.
/// This routine extracts the raw deflate stream (skipping the
/// gzip header and footer) and uses a raw decompressor.
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

    // Extract only the deflate-compressed part.
    let compressed_data = &block[BGZF_HEADER_SIZE..block.len() - BGZF_FOOTER_SIZE];

    // Use a raw decompressor (no zlib header) since we removed the gzip header.
    let mut decompressor = Decompress::new(false);
    let mut in_offset = 0;
    let mut record_count = 0;
    let mut chunk = [0u8; 4096];
    let mut unconsumed = Vec::new();

    loop {
        // Determine input and flush mode.
        let input = &compressed_data[in_offset..];
        let flush_mode = if input.is_empty() {
            FlushDecompress::Finish
        } else {
            FlushDecompress::None
        };

        // eprintln!(
        //     "DEBUG: in_offset: {}, remaining input.len(): {}, flush_mode: {:?}",
        //     in_offset,
        //     input.len(),
        //     flush_mode
        // );

        let before_in = decompressor.total_in();
        let before_out = decompressor.total_out();
        let status = decompressor.decompress(input, &mut chunk, flush_mode)?;
        let consumed = (decompressor.total_in() - before_in) as usize;
        let produced = (decompressor.total_out() - before_out) as usize;
        in_offset += consumed;
        // eprintln!(
        //     "DEBUG: status: {:?}, consumed: {}, produced: {}",
        //     status,
        //     consumed, produced
        // );

        // If no progress is made, break to avoid an infinite loop.
        if produced == 0 && consumed == 0 {
            // eprintln!("DEBUG: No progress made; breaking out of the loop.");
            break;
        }

        unconsumed.extend_from_slice(&chunk[..produced]);

        // Process unconsumed decompressed bytes to count complete BAM records.
        let mut pos = 0;
        while pos + 4 <= unconsumed.len() {
            // Each record starts with a 4-byte little-endian record length.
            let rec_size = u32::from_le_bytes([
                unconsumed[pos],
                unconsumed[pos + 1],
                unconsumed[pos + 2],
                unconsumed[pos + 3],
            ]) as usize;
            // Check if we have the entire record (4-byte header + record body).
            if pos + 4 + rec_size > unconsumed.len() {
                break;
            }
            pos += 4 + rec_size;
            record_count += 1;
        }
        // Remove processed bytes.
        unconsumed.drain(0..pos);

        if status == Status::StreamEnd {
            // eprintln!("DEBUG: reached StreamEnd");
            break;
        }
    }

    Ok(record_count)
}

/// Streams through the BAM file (which is BGZF-compressed) block by block,
/// reassembles each BGZF block from its header, payload, and footer, then
/// uses `count_bam_records_in_bgzf_block_minimal` to count the number of records.
/// Debug prints are enabled to inspect block sizes.
pub fn count_bam_records_in_bam_file_minimal(bam_path: &str) -> Result<usize> {
    let file = File::open(bam_path)?;
    let mut reader = BufReader::new(file);
    let mut total_records = 0;

    loop {
        // Read the BGZF header.
        let mut header = [0u8; BGZF_HEADER_SIZE];
        if let Err(e) = reader.read_exact(&mut header) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break; // End of file reached.
            } else {
                return Err(e.into());
            }
        }

        // Validate the GZIP magic bytes.
        if header[0..2] != [0x1f, 0x8b] {
            return Err(anyhow!("Invalid GZIP header in BGZF block"));
        }

        // Extract BSIZE (bytes 16-17); BSIZE = total block size - 1.
        let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
        let total_block_size = bsize + 1;
        // eprintln!(
        //     "DEBUG: Read BGZF block with total_block_size: {}",
        //     total_block_size
        // );

        // Basic sanity check on block size.
        if total_block_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_block_size > 65536 {
            return Err(anyhow!("Invalid BGZF block size: {}", total_block_size));
        }

        // Read the remainder of the block (payload + footer).
        let remainder_size = total_block_size - BGZF_HEADER_SIZE;
        let mut remainder = vec![0u8; remainder_size];
        reader.read_exact(&mut remainder)?;

        // Reassemble the full BGZF block.
        let mut block = Vec::with_capacity(total_block_size);
        block.extend_from_slice(&header);
        block.extend_from_slice(&remainder);
        // eprintln!("DEBUG: Processing BGZF block of size: {}", block.len());

        let count = count_bam_records_in_bgzf_block_minimal(&block)?;
        // eprintln!("DEBUG: Block contained {} records", count);
        total_records += count;
    }

    Ok(total_records)
}
