use anyhow::{anyhow, Result};
use libdeflater::Decompressor;
use memmap2::Mmap;
use std::fs::File;
use std::ptr;

const BGZF_BLOCK_MAX_SIZE: usize = 65536; // Maximum uncompressed BGZF block size
const BGZF_HEADER_SIZE: usize = 18;
const BGZF_FOOTER_SIZE: usize = 8;

/// Decompresses a BGZF block and iterates over its records, calling the provided
/// closure with the first `header_length` bytes of each record (the record header).
///
/// Each record in the decompressed data is assumed to be stored as:
///   [4 bytes: record length (little-endian)] [record body of length `rec_size`]
///
/// The closure `process_record_header` is called with a slice of length `header_length`
/// taken from the start of the record body. If a record’s body is smaller than
/// `header_length`, an error is returned.
///
/// # Parameters
///
/// - `block`: A byte slice containing the full BGZF block (including its gzip header/footer).
/// - `header_length`: The number of bytes at the start of each record (after the 4‑byte length)
///   to be considered the “header.”
/// - `process_record_header`: A closure that receives a record header slice and a mutable
///   reference to an accumulator. This allows you to process or accumulate information from
///   each header without copying extra data.
/// - `acc`: A mutable reference to an accumulator of an arbitrary type.
///
/// # Example Use–Case
///
/// Counting the number of records (ignoring the header content) is implemented simply by
/// incrementing a counter for each header.
pub fn read_record_headers<F, A>(
    block: &[u8],
    header_length: usize,
    mut process_record_header: F,
    acc: &mut A,
) -> Result<()>
where
    F: FnMut(&[u8], &mut A),
{
    // Instead of stripping the header and footer, use the full block.
    let mut output = vec![0u8; BGZF_BLOCK_MAX_SIZE];
    let mut decompressor = Decompressor::new();
    let decompressed_size = decompressor
        .gzip_decompress(block, &mut output)
        .map_err(|e| anyhow!("Decompression failed: {:?}", e))?;

    // Now process the decompressed output to count BAM records.
    let mut pos = 0;
    unsafe {
        let out_ptr = output.as_ptr();
        while pos + 4 <= decompressed_size {
            let rec_size =
                u32::from_le(ptr::read_unaligned(out_ptr.add(pos) as *const u32)) as usize;
            if pos + 4 + rec_size > decompressed_size {
                break;
            }
            // The record body starts immediately after the 4-byte length.
            let record_body = std::slice::from_raw_parts(out_ptr.add(pos + 4), rec_size);

            // Check that the record body is at least as long as the header we wish to process.
            if header_length > rec_size {
                return Err(anyhow!(
                    "Record body size {} is smaller than header length {}",
                    rec_size,
                    header_length
                ));
            }

            // Get the header slice (the first `header_length` bytes of the record body).
            let header_slice = &record_body[..header_length];

            // Call the provided closure with the header slice.
            process_record_header(header_slice, acc);

            // Move to the next record.
            pos += 4 + rec_size;
        }
    }
    Ok(())
}

/// A sample use–case that counts the number of records by processing only their headers.
pub fn count_bam_record_headers(block: &[u8], header_length: usize) -> Result<usize> {
    let mut count = 0;
    read_record_headers(
        block,
        header_length,
        |_: &[u8], counter: &mut usize| {
            *counter += 1;
        },
        &mut count,
    )?;
    Ok(count)
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
        let count = count_bam_record_headers(block, 16)?;
        total_records += count;

        pos += total_block_size;
    }

    Ok(total_records)
}
