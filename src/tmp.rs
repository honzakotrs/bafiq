use anyhow::Result;
use flate2::read::GzDecoder;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};

/// Constants for BGZF parsing.
const BGZF_HEADER_SIZE: usize = 18;
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

    // Extract block size from the header (16th and 17th bytes)
    let block_size = u16::from_le_bytes([header[16], header[17]]) as usize + 1;

    // Read the entire BGZF block (including compressed data and footer)
    let mut compressed_block = vec![0u8; block_size - BGZF_HEADER_SIZE];
    file.read_exact(&mut compressed_block)?;

    // Decompress the BGZF block
    let mut decoder = GzDecoder::new(&compressed_block[..]);
    let mut decompressed_data = Vec::new();
    decoder.read_to_end(&mut decompressed_data)?;

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

    for i in 0..=compressed_block.len() - pattern_size {
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
