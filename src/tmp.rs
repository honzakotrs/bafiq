use anyhow::Result;
use flate2::read::GzDecoder;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};

/// Constants for BGZF parsing.
const BGZF_HEADER_SIZE: usize = 18;
const BGZF_FOOTER_SIZE: usize = 8;
const BAM_HEADER_SIZE: usize = 4; // Record size field in BAM

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
    let block_size = u16::from_le_bytes([header[16], header[17]]) as usize;

    println!("Block size (compressed): {}", block_size);

    // Read the entire BGZF block (excluding the header we already read)
    // BSIZE is total block size minus 1
    let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
    // The actual on-disk total size for the entire BGZF block:
    let total_bgzf_block_size = bsize + 1;
    // Subtract the 18-byte BGZF header to get the compressed payload + footer:
    let compressed_block_size = total_bgzf_block_size - BGZF_HEADER_SIZE;

    let mut compressed_block = vec![0u8; compressed_block_size];
    file.read_exact(&mut compressed_block)?;

    // Extract the footer from the compressed block
    let footer_offset = compressed_block.len() - BGZF_FOOTER_SIZE;
    let footer = &compressed_block[footer_offset..];

    // Extract uncompressed block size from the footer (last 2 bytes)
    let uncompressed_size = u16::from_le_bytes([footer[4], footer[5]]) as usize;
    println!(
        "Uncompressed block size (from footer): {}",
        uncompressed_size
    );

    // Decompress the BGZF block
    let footer_offset = compressed_block.len() - BGZF_FOOTER_SIZE;
    let compressed_data = &compressed_block[..footer_offset];
    let mut decoder = GzDecoder::new(compressed_data);
    let mut decompressed_data = Vec::new();
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

    for i in 0..footer_offset - pattern_size {
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

pub fn count_blocks(bam_path: &str) -> Result<u64> {
    /// Constants for BAM file parsing.
    const BGZF_HEADER_SIZE: usize = 18;
    const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

    let file = File::open(bam_path)?;
    let mut reader = BufReader::new(file);
    let mut block_count = 0;

    loop {
        // Read the BGZF header
        let mut header = [0u8; BGZF_HEADER_SIZE];
        if reader.read_exact(&mut header).is_err() {
            break; // EOF or error
        }

        // Validate GZIP magic numbers
        if header[0..2] != GZIP_MAGIC {
            return Err(anyhow::anyhow!(
                "Invalid BGZF block: missing GZIP magic numbers"
            ));
        }

        // Extract the total block size from the header (16th and 17th bytes)
        let block_size = u16::from_le_bytes([header[16], header[17]]) as usize + 1;

        // Skip the rest of the block
        let mut skip_buffer = vec![0; block_size - BGZF_HEADER_SIZE];
        reader.read_exact(&mut skip_buffer)?;

        // Increment block count
        block_count += 1;
    }

    Ok(block_count)
}
