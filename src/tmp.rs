use anyhow::Result;
use flate2::{Decompress, FlushDecompress, Status};
use std::fs::File;
use std::io::{BufReader, Read};

/// Counts the number of BAM records in a full BGZF block (header + compressed payload + footer)
/// using minimal streaming decompression. Debug prints are enabled to help trace state.
pub fn count_bam_records_in_bgzf_block_minimal(block: &[u8]) -> Result<usize> {
    let mut decompressor = Decompress::new(true);
    let mut in_offset = 0;
    let mut record_count = 0;
    let mut chunk = [0u8; 4096];
    let mut unconsumed = Vec::new();

    loop {
        // Determine how many input bytes remain and select flush mode.
        let input = &block[in_offset..];
        let flush_mode = if input.is_empty() {
            FlushDecompress::Finish
        } else {
            FlushDecompress::None
        };

        // Debug print current state before decompression.
        eprintln!(
            "DEBUG: in_offset: {}, remaining input.len(): {}, flush_mode: {:?}",
            in_offset,
            input.len(),
            flush_mode
        );

        let before_in = decompressor.total_in();
        let before_out = decompressor.total_out();
        let status = decompressor.decompress(input, &mut chunk, flush_mode)?;
        let consumed = (decompressor.total_in() - before_in) as usize;
        let produced = (decompressor.total_out() - before_out) as usize;
        in_offset += consumed;

        eprintln!(
            "DEBUG: status: {:?}, consumed: {}, produced: {}",
            status, consumed, produced
        );

        // If no progress is made, avoid an infinite loop.
        if produced == 0 && consumed == 0 {
            eprintln!("DEBUG: No progress made; breaking out of the loop.");
            break;
        }

        unconsumed.extend_from_slice(&chunk[..produced]);

        // Process unconsumed bytes to extract complete BAM records.
        let mut pos = 0;
        while pos + 4 <= unconsumed.len() {
            // Each record begins with a 4-byte little-endian length.
            let rec_size = u32::from_le_bytes([
                unconsumed[pos],
                unconsumed[pos + 1],
                unconsumed[pos + 2],
                unconsumed[pos + 3],
            ]) as usize;
            if pos + 4 + rec_size > unconsumed.len() {
                // Incomplete record: wait for more decompressed data.
                break;
            }
            pos += 4 + rec_size;
            record_count += 1;
        }
        // Remove processed bytes.
        unconsumed.drain(0..pos);

        // If the decompressor indicates the stream is finished, exit the loop.
        if status == Status::StreamEnd {
            eprintln!("DEBUG: reached StreamEnd");
            break;
        }
    }
    Ok(record_count)
}

/// Streams through the BAM file (which is BGZF-compressed) block by block,
/// reassembling each block from its header, payload, and footer, then using
/// `count_bam_records_in_bgzf_block_minimal` to count records. Debug prints
/// are included to inspect BGZF block sizes.
pub fn count_bam_records_in_bam_file_minimal(bam_path: &str) -> Result<usize> {
    const BGZF_HEADER_SIZE: usize = 18;
    const BGZF_FOOTER_SIZE: usize = 8;

    let file = File::open(bam_path)?;
    let mut reader = BufReader::new(file);
    let mut total_records = 0;

    loop {
        // Read the BGZF header.
        let mut header = [0u8; BGZF_HEADER_SIZE];
        if let Err(e) = reader.read_exact(&mut header) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                // End of file reached.
                break;
            } else {
                return Err(e.into());
            }
        }

        // Validate the GZIP magic bytes.
        if header[0..2] != [0x1f, 0x8b] {
            return Err(anyhow::anyhow!("Invalid GZIP header in BGZF block"));
        }

        // Extract BSIZE from the header (bytes 16 and 17). Note: BSIZE = total block size - 1.
        let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
        let total_block_size = bsize + 1;

        eprintln!(
            "DEBUG: Read BGZF block with total_block_size: {}",
            total_block_size
        );

        // Check for a valid block size.
        if total_block_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_block_size > 65536 {
            return Err(anyhow::anyhow!(
                "Invalid BGZF block size: {}",
                total_block_size
            ));
        }

        // Read the remainder of the block (payload + footer).
        let remainder_size = total_block_size - BGZF_HEADER_SIZE;
        let mut remainder = vec![0u8; remainder_size];
        reader.read_exact(&mut remainder)?;

        // Reassemble the full BGZF block.
        let mut block = Vec::with_capacity(total_block_size);
        block.extend_from_slice(&header);
        block.extend_from_slice(&remainder);

        eprintln!("DEBUG: Processing BGZF block of size: {}", block.len());
        let count = count_bam_records_in_bgzf_block_minimal(&block)?;
        eprintln!("DEBUG: Block contained {} records", count);
        total_records += count;
    }

    Ok(total_records)
}
