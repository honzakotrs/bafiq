use anyhow::Result;
use flate2::read::GzDecoder;
use flate2::{Decompress, FlushDecompress, Status};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};

/// Constants for BGZF parsing.
const BGZF_HEADER_SIZE: usize = 18;
const BGZF_FOOTER_SIZE: usize = 8;
const BAM_HEADER_SIZE: usize = 4; // Record size field in BAM

/// Counts the number of BAM records in a full BGZF block (header + compressed payload + footer)
/// using minimal streaming decompression.
///
/// # Arguments
/// * `block` - A slice containing the full BGZF block.
///
/// # Returns
/// The number of BAM records (reads) in the block.
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
        // Decompress a chunk of data.
        let before_in = decompressor.total_in();
        let before_out = decompressor.total_out();
        let status =
            decompressor.decompress(&block[in_offset..], &mut chunk, FlushDecompress::None)?;
        let consumed = (decompressor.total_in() - before_in) as usize;
        in_offset += consumed;
        let produced = (decompressor.total_out() - before_out) as usize;
        unconsumed.extend_from_slice(&chunk[..produced]);

        // Process unconsumed bytes to extract complete records.
        let mut pos = 0;
        while pos + 4 <= unconsumed.len() {
            // Read the record size (4 bytes, little-endian)
            let rec_size = u32::from_le_bytes([
                unconsumed[pos],
                unconsumed[pos + 1],
                unconsumed[pos + 2],
                unconsumed[pos + 3],
            ]) as usize;
            // Check if the full record (header + body) is available.
            if pos + 4 + rec_size > unconsumed.len() {
                // Not enough data yet – break out and get more decompressed data.
                break;
            }
            // Skip over this record.
            pos += 4 + rec_size;
            record_count += 1;
        }
        // Remove processed bytes from the unconsumed buffer.
        unconsumed.drain(0..pos);

        // If we reached the end of the stream, break out.
        if status == Status::StreamEnd {
            break;
        }
    }
    Ok(record_count)
}

/// Reads the first BGZF block, decompresses it to count the number of BAM records, and analyzes compressed byte patterns.
///
/// # Arguments
/// * `bam_path` - Path to the BAM file.
///
/// # Returns
/// A result containing a list of byte signatures occurring as many times as the record count, or an error.
pub fn analyze_first_block(bam_path: &str) -> Result<()> {
    let mut file = File::open(bam_path)?;

    // Read the BGZF header
    let mut header = [0u8; BGZF_HEADER_SIZE];
    file.read_exact(&mut header)?;

    // Validate GZIP magic numbers
    if header[0..2] != [0x1f, 0x8b] {
        return Err(anyhow::anyhow!("Invalid GZIP header in BGZF block"));
    }

    // Extract block size from the header (16th and 17th bytes)
    let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
    println!("Block size (compressed): {}", bsize);

    // The actual on-disk total size for the entire BGZF block:
    let total_bgzf_block_size = bsize + 1;
    // Subtract the 18-byte BGZF header to get the compressed payload + footer:
    let compressed_block_size = total_bgzf_block_size - BGZF_HEADER_SIZE;

    // Read the rest of the BGZF block (compressed payload + footer)
    let mut compressed_block = vec![0u8; compressed_block_size];
    file.read_exact(&mut compressed_block)?;

    // Reassemble the full BGZF block (header + compressed payload + footer)
    let mut full_block = Vec::with_capacity(total_bgzf_block_size);
    full_block.extend_from_slice(&header);
    full_block.extend_from_slice(&compressed_block);

    // Extract the footer from the compressed block (last 8 bytes of the block)
    let footer_offset = compressed_block.len() - BGZF_FOOTER_SIZE;
    let footer = &compressed_block[footer_offset..];

    // Extract uncompressed block size from the footer (last 4 bytes)
    let uncompressed_size =
        u32::from_le_bytes([footer[4], footer[5], footer[6], footer[7]]) as usize;
    println!(
        "Uncompressed block size (from footer): {}",
        uncompressed_size
    );

    // Decompress the BGZF block using the full block (which includes the header)
    let mut decoder = GzDecoder::new(&full_block[..]);
    let mut decompressed_data = Vec::with_capacity(uncompressed_size);
    decoder.read_to_end(&mut decompressed_data)?;

    // Validate decompressed size
    if decompressed_data.len() != uncompressed_size {
        return Err(anyhow::anyhow!(
            "Mismatch between decompressed size ({}) and footer size ({})",
            decompressed_data.len(),
            uncompressed_size
        ));
    }

    println!("Decompressed block size: {}", decompressed_data.len());

    // Count the number of BAM records in the decompressed block
    let mut offset = 0;
    let mut record_count = 0;
    while offset + BAM_HEADER_SIZE <= decompressed_data.len() {
        // Read the record size (first 4 bytes of each record)
        let record_size = u32::from_le_bytes([
            decompressed_data[offset],
            decompressed_data[offset + 1],
            decompressed_data[offset + 2],
            decompressed_data[offset + 3],
        ]) as usize;

        // Move the offset to the next record
        offset += BAM_HEADER_SIZE + record_size;
        record_count += 1;
    }
    println!("Number of BAM records in the first block: {}", record_count);

    // Analyze compressed block for recurring byte patterns
    let mut pattern_counts: HashMap<Vec<u8>, usize> = HashMap::new();
    let pattern_size = 4; // Size of the byte patterns to analyze
    for i in 0..footer_offset.saturating_sub(pattern_size) {
        let pattern = compressed_block[i..i + pattern_size].to_vec();
        *pattern_counts.entry(pattern).or_insert(0) += 1;
    }

    // Find patterns that match the record count
    let matching_patterns: Vec<_> = pattern_counts
        .iter()
        .filter(|&(_, &count)| count == record_count)
        .collect();

    // Print the matching patterns
    println!("Matching byte patterns:");
    for (pattern, count) in matching_patterns {
        println!("{:?} occurs {} times", pattern, count);
    }

    Ok(())
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
