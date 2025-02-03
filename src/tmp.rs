use anyhow::Result;
use flate2::{Decompress, FlushDecompress, Status};
use std::fs::File;
use std::io::{BufReader, Read};

/// Constants for BGZF parsing.
const BGZF_HEADER_SIZE: usize = 18;

/// Counts the number of BAM records in a full BGZF block (header + compressed payload + footer)
/// using minimal streaming decompression.
pub fn count_bam_records_in_bgzf_block_minimal(block: &[u8]) -> Result<usize> {
    // Initialize the decompressor with gzip header processing enabled.
    let mut decompressor = Decompress::new(true);
    let mut in_offset = 0;
    let mut record_count = 0;

    // Buffer to hold decompressed bytes from each call.
    let mut chunk = [0u8; 4096];
    // A vector to hold decompressed bytes that have not yet been processed.
    let mut unconsumed = Vec::new();

    loop {
        // If we've consumed all input, tell the decompressor there's no more coming.
        let input = &block[in_offset..];
        let flush_mode = if input.is_empty() {
            FlushDecompress::Finish
        } else {
            FlushDecompress::None
        };

        let before_in = decompressor.total_in();
        let before_out = decompressor.total_out();
        let status = decompressor.decompress(input, &mut chunk, flush_mode)?;
        let consumed = (decompressor.total_in() - before_in) as usize;
        in_offset += consumed;
        let produced = (decompressor.total_out() - before_out) as usize;
        unconsumed.extend_from_slice(&chunk[..produced]);

        // Process unconsumed bytes to extract complete BAM records.
        let mut pos = 0;
        while pos + 4 <= unconsumed.len() {
            // Read the record size (first 4 bytes, little-endian).
            let rec_size = u32::from_le_bytes([
                unconsumed[pos],
                unconsumed[pos + 1],
                unconsumed[pos + 2],
                unconsumed[pos + 3],
            ]) as usize;
            // Check if the full record (header + record body) is available.
            if pos + 4 + rec_size > unconsumed.len() {
                break;
            }
            pos += 4 + rec_size;
            record_count += 1;
        }
        // Remove the processed bytes.
        unconsumed.drain(0..pos);

        // When the decompressor signals StreamEnd, we can stop.
        if status == Status::StreamEnd {
            break;
        }
    }
    Ok(record_count)
}

/// This function iterates over all BGZF blocks in the given BAM file,
/// feeding each block into `count_bam_records_in_bgzf_block_minimal` to
/// count the number of BAM records without fully decompressing each block.
pub fn count_records(bam_path: &str) -> Result<usize> {
    let file = File::open(bam_path)?;
    let mut reader = BufReader::new(file);
    let mut total_records = 0;

    loop {
        // Read the 18-byte BGZF header.
        let mut header = [0u8; BGZF_HEADER_SIZE];
        if let Err(e) = reader.read_exact(&mut header) {
            // If we've reached EOF, we’re done.
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            } else {
                return Err(e.into());
            }
        }

        // Validate that the header starts with the GZIP magic numbers.
        if header[0..2] != [0x1f, 0x8b] {
            return Err(anyhow::anyhow!("Invalid GZIP header in BGZF block"));
        }

        // Extract BSIZE (stored in bytes 16 and 17) from the header.
        // BSIZE is defined as the total block size minus one.
        let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
        let total_block_size = bsize + 1;

        // Determine how many bytes remain in the block (payload + footer).
        let remainder_size = total_block_size
            .checked_sub(BGZF_HEADER_SIZE)
            .ok_or_else(|| anyhow::anyhow!("Invalid BGZF block size"))?;

        // Read the remaining bytes of this block.
        let mut remainder = vec![0u8; remainder_size];
        reader.read_exact(&mut remainder)?;

        // Reassemble the complete BGZF block.
        let mut block = Vec::with_capacity(total_block_size);
        block.extend_from_slice(&header);
        block.extend_from_slice(&remainder);

        // Use our minimal decompression function to count BAM records in the block.
        let count = count_bam_records_in_bgzf_block_minimal(&block)?;
        total_records += count;
    }

    Ok(total_records)
}
