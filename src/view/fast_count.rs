use anyhow::{anyhow, Result};
use crossbeam::channel::unbounded;
use libdeflater::Decompressor;
use memmap2::Mmap;
use std::fs::File;
use std::ptr;
use std::sync::Arc;
use std::thread as std_thread;

use crate::bgzf::{is_bgzf_header, BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};

/// Fast scan-only mode - count flags without building index
/// Single-pass streaming approach comparable to samtools performance
pub fn scan_count(
    bam_path: &str,
    required_flags: u16,
    forbidden_flags: u16,
    thread_count: Option<usize>,
) -> Result<u64> {
    let file = File::open(bam_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let data = Arc::new(mmap);

    // Use specified thread count or default to more workers than CPU cores for I/O bound workload
    let num_threads = thread_count.unwrap_or_else(|| (rayon::current_num_threads() * 2).max(8));

    // PERFORMANCE FIX: Use unbounded crossbeam channel for scan_count too!
    let (sender, receiver) = unbounded::<(usize, usize)>(); // (start_pos, total_size)

    // Producer thread: discover and stream blocks immediately
    let data_producer = Arc::clone(&data);
    let producer_handle = std_thread::spawn(move || -> Result<usize> {
        let mut pos = 0;
        let mut block_count = 0;
        let data_len = data_producer.len();

        while pos < data_len {
            if pos + BGZF_HEADER_SIZE > data_len {
                break;
            }

            let header = &data_producer[pos..pos + BGZF_HEADER_SIZE];

            // Validate GZIP magic
            if !is_bgzf_header(header) {
                return Err(anyhow!("Invalid GZIP header at position {}", pos));
            }

            // Extract block size
            let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
            let total_size = bsize + 1;

            // Validate block size
            if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > BGZF_BLOCK_MAX_SIZE
            {
                return Err(anyhow!("Invalid BGZF block size: {}", total_size));
            }

            if pos + total_size > data_len {
                break; // Incomplete block at end
            }

            // Send block immediately to workers (single pass!)
            if sender.send((pos, total_size)).is_err() {
                break; // Workers hung up
            }

            pos += total_size;
            block_count += 1;
        }

        Ok(block_count)
    });

    // Consumer threads: count flags as blocks arrive
    let mut worker_handles = Vec::new();
    // PERFORMANCE FIX: Remove mutex contention in scan_count too!

    for _thread_id in 0..num_threads {
        let receiver_clone = receiver.clone();
        let data_worker = Arc::clone(&data);

        let handle = std_thread::spawn(move || -> Result<u64> {
            let mut local_count = 0u64;

            loop {
                // PERFORMANCE FIX: Direct channel access - NO MUTEX CONTENTION!
                match receiver_clone.recv() {
                    Ok((start_pos, total_size)) => {
                        let block = &data_worker[start_pos..start_pos + total_size];
                        local_count +=
                            count_flags_in_block_optimized(block, required_flags, forbidden_flags)
                                .unwrap_or(0);
                    }
                    Err(_) => {
                        // Channel closed, no more blocks
                        break;
                    }
                }
            }

            Ok(local_count)
        });

        worker_handles.push(handle);
    }

    // Wait for producer
    let _total_blocks = producer_handle
        .join()
        .map_err(|e| anyhow!("Producer thread failed: {:?}", e))??;

    // Wait for all workers and sum their counts
    let mut total_count = 0u64;
    for handle in worker_handles.into_iter() {
        match handle.join() {
            Ok(Ok(local_count)) => {
                total_count += local_count;
            }
            Ok(Err(e)) => {
                eprintln!("Worker thread failed: {}", e);
            }
            Err(e) => {
                eprintln!("Worker thread panicked: {:?}", e);
            }
        }
    }

    Ok(total_count)
}

/// Optimized flag counting with thread-local buffer reuse
fn count_flags_in_block_optimized(
    block: &[u8],
    required_flags: u16,
    forbidden_flags: u16,
) -> Result<u64> {
    thread_local! {
        static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
        static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
    }

    BUFFER.with(|buf| {
        DECOMPRESSOR.with(|decomp| {
            let mut output = buf.borrow_mut();
            let mut decompressor = decomp.borrow_mut();

            let decompressed_size = decompressor
                .gzip_decompress(block, &mut output)
                .map_err(|e| anyhow!("Decompression failed: {:?}", e))?;

            // Skip BAM header blocks
            if decompressed_size >= 4 && &output[0..4] == b"BAM\x01" {
                return Ok(0);
            }

            let mut count = 0u64;
            let mut pos = 0;

            unsafe {
                let out_ptr = output.as_ptr();
                while pos + 4 <= decompressed_size {
                    let rec_size =
                        u32::from_le(ptr::read_unaligned(out_ptr.add(pos) as *const u32)) as usize;

                    if pos + 4 + rec_size > decompressed_size {
                        break;
                    }

                    // Extract flags at offset 14-15 in record body
                    if rec_size >= 16 {
                        let record_body =
                            std::slice::from_raw_parts(out_ptr.add(pos + 4), rec_size);
                        let flags = u16::from_le_bytes([record_body[14], record_body[15]]);

                        // Apply flag filters
                        if (flags & required_flags) == required_flags
                            && (flags & forbidden_flags) == 0
                        {
                            count += 1;
                        }
                    }

                    pos += 4 + rec_size;
                }
            }

            Ok(count)
        })
    })
}
