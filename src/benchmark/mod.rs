//! # Benchmark Module
//!
//! This module contains the various index building and flag counting implementations
//! used for performance benchmarking and comparison. These functions are specifically
//! designed for benchmarking different approaches to BAM file processing.

use crate::index::discovery::extract_flags_from_block_pooled;
use crate::FlagIndex;
use anyhow::{anyhow, Result};
use libdeflater::Decompressor;
use memmap2::Mmap;
use rayon::prelude::*;
use std::cell::RefCell;
use std::fs::File;
use std::ptr;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use crate::bgzf::{is_bgzf_header, BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};

/// Information about a BGZF block's location in the file
#[derive(Debug, Clone)]
struct BlockInfo {
    start_pos: usize,
    total_size: usize,
}

/// Phase 1: Fast discovery of all BGZF block boundaries
fn discover_all_blocks(data: &[u8]) -> Result<Vec<BlockInfo>> {
    let mut blocks = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        if pos + BGZF_HEADER_SIZE > data.len() {
            break;
        }

        let header = &data[pos..pos + BGZF_HEADER_SIZE];

        // Validate GZIP magic
        if !is_bgzf_header(header) {
            return Err(anyhow!("Invalid GZIP header at position {}", pos));
        }

        // Extract block size
        let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
        let total_size = bsize + 1;

        // Validate block size
        if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > BGZF_BLOCK_MAX_SIZE {
            return Err(anyhow!("Invalid BGZF block size: {}", total_size));
        }

        if pos + total_size > data.len() {
            break; // Incomplete block at end
        }

        blocks.push(BlockInfo {
            start_pos: pos,
            total_size,
        });

        pos += total_size;
    }

    Ok(blocks)
}

/// Process a chunk of blocks and build a local index
fn process_block_chunk(data: &[u8], blocks: &[BlockInfo]) -> Result<FlagIndex> {
    let mut local_index = FlagIndex::new();

    for block_info in blocks {
        let block = &data[block_info.start_pos..block_info.start_pos + block_info.total_size];
        let block_offset = block_info.start_pos as i64;

        // Use thread-local buffers for efficiency
        thread_local! {
            static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
            static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
        }

        BUFFER.with(|buf| {
            DECOMPRESSOR.with(|decomp| {
                let mut buffer = buf.borrow_mut();
                let mut decompressor = decomp.borrow_mut();
                extract_flags_from_block_pooled(
                    block,
                    &mut local_index,
                    block_offset,
                    &mut buffer,
                    &mut decompressor,
                )
            })
        })?;
    }

    Ok(local_index)
}

/// Build a complete FlagIndex using parallel BGZF parsing.
/// This leverages multiple cores to process blocks concurrently.
pub fn build_flag_index_parallel(bam_path: &str) -> Result<FlagIndex> {
    let file = File::open(bam_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let data = &mmap[..];

    let blocks = discover_all_blocks(data)?;
    let num_threads = rayon::current_num_threads();
    let chunk_size = (blocks.len() + num_threads - 1) / num_threads;

    // Process blocks in parallel
    let local_indexes: Vec<FlagIndex> = blocks
        .par_chunks(chunk_size)
        .map(|chunk| process_block_chunk(data, chunk).expect("Block processing failed"))
        .collect();

    // Merge all local indexes using parallel merge tree
    let final_index = FlagIndex::merge_parallel(local_indexes);

    Ok(final_index)
}

/// Build a complete FlagIndex using sequential low-level BGZF parsing.
/// This provides the baseline for comparison with the parallel approach.
pub fn build_flag_index_low_level(bam_path: &str) -> Result<FlagIndex> {
    let file = File::open(bam_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let data = &mmap[..];

    let mut index = FlagIndex::new();
    let mut _total_records = 0;
    let mut pos = 0;

    // Use thread-local buffers for sequential processing (single-threaded, but safe)
    thread_local! {
        static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
        static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
    }

    // Iterate over the memory-mapped file.
    while pos < data.len() {
        // Ensure there is enough data for a full BGZF header.
        if pos + BGZF_HEADER_SIZE > data.len() {
            break; // No more complete block header.
        }
        let header = &data[pos..pos + BGZF_HEADER_SIZE];

        // Validate the GZIP magic bytes.
        if !is_bgzf_header(header) {
            return Err(anyhow!("Invalid GZIP header in BGZF block"));
        }

        // Extract BSIZE (bytes 16-17); BSIZE = total block size - 1.
        let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
        let total_block_size = bsize + 1;

        // Sanity-check the block size.
        if total_block_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE
            || total_block_size > BGZF_BLOCK_MAX_SIZE
        {
            return Err(anyhow!("Invalid BGZF block size: {}", total_block_size));
        }
        if pos + total_block_size > data.len() {
            break; // Incomplete block at the end.
        }

        // Get the full BGZF block and extract flags using efficient buffer reuse
        let block = &data[pos..pos + total_block_size];
        let block_offset = pos as i64; // Virtual file offset for this block

        let count = BUFFER.with(|buf| {
            DECOMPRESSOR.with(|decomp| {
                let mut buffer = buf.borrow_mut();
                let mut decompressor = decomp.borrow_mut();
                extract_flags_from_block_pooled(
                    block,
                    &mut index,
                    block_offset,
                    &mut buffer,
                    &mut decompressor,
                )
            })
        })?;
        _total_records += count;

        pos += total_block_size;
    }

    Ok(index)
}

/// Build a complete FlagIndex using streaming parallel processing.
pub fn build_flag_index_streaming_parallel(bam_path: &str) -> Result<FlagIndex> {
    let file = File::open(bam_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let data = Arc::new(mmap);

    // Use more workers than CPU cores for I/O bound workload
    let num_threads = (rayon::current_num_threads() * 2).max(8);

    // PERFORMANCE FIX: Use unbounded crossbeam channel for benchmark streaming too!
    let (sender, receiver) = crossbeam::channel::unbounded::<(usize, usize)>(); // (start_pos, total_size)

    // Producer thread: discover and stream blocks immediately
    let data_producer = Arc::clone(&data);
    let producer_handle = thread::spawn(move || -> Result<usize> {
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

    // Consumer threads: process blocks as they arrive
    let mut worker_handles = Vec::new();
    // PERFORMANCE FIX: Remove mutex contention in benchmark module too!

    for _thread_id in 0..num_threads {
        let receiver_clone = receiver.clone();
        let data_worker = Arc::clone(&data);

        let handle = thread::spawn(move || -> Result<FlagIndex> {
            let mut local_index = FlagIndex::new();

            loop {
                // PERFORMANCE FIX: Direct channel access - NO MUTEX CONTENTION!
                match receiver_clone.recv() {
                    Ok((start_pos, total_size)) => {
                        let block = &data_worker[start_pos..start_pos + total_size];
                        let block_offset = start_pos as i64;

                        // Use thread-local buffers for efficiency
                        thread_local! {
                            static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                            static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
                        }

                        BUFFER.with(|buf| {
                            DECOMPRESSOR.with(|decomp| {
                                let mut buffer = buf.borrow_mut();
                                let mut decompressor = decomp.borrow_mut();
                                extract_flags_from_block_pooled(
                                    block,
                                    &mut local_index,
                                    block_offset,
                                    &mut buffer,
                                    &mut decompressor,
                                )
                            })
                        })?;
                    }
                    Err(_) => {
                        // Channel closed, no more blocks
                        break;
                    }
                }
            }

            Ok(local_index)
        });

        worker_handles.push(handle);
    }

    // PERFORMANCE FIX: No need to drop receiver manually - crossbeam handles it automatically

    // Wait for producer
    let _total_blocks = producer_handle
        .join()
        .map_err(|e| anyhow!("Producer thread failed: {:?}", e))??;

    // Wait for all workers and merge their indexes using parallel merge tree
    let mut local_indexes = Vec::new();
    for handle in worker_handles.into_iter() {
        match handle.join() {
            Ok(Ok(local_index)) => {
                local_indexes.push(local_index);
            }
            Ok(Err(e)) => {
                return Err(e);
            }
            Err(e) => {
                return Err(anyhow!("Worker thread panicked: {:?}", e));
            }
        }
    }

    let final_index = FlagIndex::merge_parallel(local_indexes);
    Ok(final_index)
}

/// Multi-threaded BGZF decompression with 3-stage pipeline for flag counting
pub fn count_flags_multithreaded_decompression(
    bam_path: &str,
    required_flags: u16,
    forbidden_flags: u16,
) -> Result<u64> {
    let file = File::open(bam_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let data = Arc::new(mmap);

    let num_cores = rayon::current_num_threads();
    let decompression_threads = num_cores;
    let processing_threads = (num_cores * 2).max(8);

    // Channels for 3-stage pipeline
    let (block_sender, block_receiver) = mpsc::channel::<(usize, usize)>();
    let (decomp_sender, decomp_receiver) = mpsc::channel::<(Vec<u8>, i64)>();

    // STAGE 1: Block Discovery
    let data_discovery = Arc::clone(&data);
    let discovery_handle = thread::spawn(move || -> Result<usize> {
        let mut pos = 0;
        let mut block_count = 0;
        let data_len = data_discovery.len();

        while pos < data_len {
            if pos + BGZF_HEADER_SIZE > data_len {
                break;
            }

            let header = &data_discovery[pos..pos + BGZF_HEADER_SIZE];
            if !is_bgzf_header(header) {
                return Err(anyhow!("Invalid GZIP header at position {}", pos));
            }

            let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
            let total_size = bsize + 1;

            if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > BGZF_BLOCK_MAX_SIZE
            {
                return Err(anyhow!("Invalid BGZF block size: {}", total_size));
            }

            if pos + total_size > data_len {
                break;
            }

            if block_sender.send((pos, total_size)).is_err() {
                break;
            }

            pos += total_size;
            block_count += 1;
        }

        drop(block_sender);
        Ok(block_count)
    });

    // STAGE 2: Parallel BGZF Decompression
    let mut decompression_handles = Vec::new();
    let shared_block_receiver = Arc::new(Mutex::new(block_receiver));

    for _thread_id in 0..decompression_threads {
        let block_receiver_clone = Arc::clone(&shared_block_receiver);
        let data_decomp = Arc::clone(&data);
        let decomp_sender_clone = decomp_sender.clone();

        let handle = thread::spawn(move || -> Result<()> {
            let mut decompressor = Decompressor::new();
            let mut output_buffer = vec![0u8; BGZF_BLOCK_MAX_SIZE];

            loop {
                let block_info = {
                    let receiver = block_receiver_clone.lock().unwrap();
                    receiver.recv()
                };

                match block_info {
                    Ok((start_pos, total_size)) => {
                        let block = &data_decomp[start_pos..start_pos + total_size];
                        let block_offset = start_pos as i64;

                        let decompressed_size = decompressor
                            .gzip_decompress(block, &mut output_buffer)
                            .map_err(|e| anyhow!("Decompression failed: {:?}", e))?;

                        let decompressed_data = output_buffer[..decompressed_size].to_vec();

                        if decomp_sender_clone
                            .send((decompressed_data, block_offset))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            Ok(())
        });

        decompression_handles.push(handle);
    }

    // STAGE 3: Parallel Record Processing
    let mut processing_handles = Vec::new();
    let shared_decomp_receiver = Arc::new(Mutex::new(decomp_receiver));

    for _thread_id in 0..processing_threads {
        let decomp_receiver_clone = Arc::clone(&shared_decomp_receiver);

        let handle = thread::spawn(move || -> Result<u64> {
            let mut local_count = 0u64;

            loop {
                let decomp_result = {
                    let receiver = decomp_receiver_clone.lock().unwrap();
                    receiver.recv()
                };

                match decomp_result {
                    Ok((decompressed_data, _block_offset)) => {
                        local_count += count_flags_in_decompressed_block(
                            &decompressed_data,
                            required_flags,
                            forbidden_flags,
                        )?;
                    }
                    Err(_) => break,
                }
            }

            Ok(local_count)
        });

        processing_handles.push(handle);
    }

    // Cleanup and wait for all stages
    drop(decomp_sender);
    drop(shared_decomp_receiver);

    let _total_blocks = discovery_handle
        .join()
        .map_err(|e| anyhow!("Discovery thread failed: {:?}", e))??;

    for handle in decompression_handles {
        handle
            .join()
            .map_err(|e| anyhow!("Decompression thread failed: {:?}", e))??;
    }

    let mut total_count = 0u64;
    for handle in processing_handles {
        total_count += handle
            .join()
            .map_err(|e| anyhow!("Processing thread failed: {:?}", e))??;
    }

    Ok(total_count)
}

/// Simplified multi-threaded BGZF decompression using direct Rayon parallelization
pub fn count_flags_simple_parallel_decompression(
    bam_path: &str,
    required_flags: u16,
    forbidden_flags: u16,
) -> Result<u64> {
    let file = File::open(bam_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let data = &mmap[..];

    // Phase 1: Discover all blocks (single-threaded, fast)
    let blocks = discover_all_blocks(data)?;

    // Phase 2: Parallel decompression and counting using Rayon
    let total_count: u64 = blocks
        .par_iter()
        .map(|block_info| -> Result<u64> {
            let block = &data[block_info.start_pos..block_info.start_pos + block_info.total_size];
            count_flags_in_block_simple(block, required_flags, forbidden_flags)
        })
        .try_reduce(|| 0, |a, b| Ok(a + b))?;

    Ok(total_count)
}

/// Thread pool based parallel decompression
pub fn count_flags_libdeflate_thread_pool(
    bam_path: &str,
    required_flags: u16,
    forbidden_flags: u16,
) -> Result<u64> {
    let file = File::open(bam_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let data = &mmap[..];

    let blocks = discover_all_blocks(data)?;

    let total_count: u64 = blocks
        .par_iter()
        .map(|block_info| -> Result<u64> {
            let block = &data[block_info.start_pos..block_info.start_pos + block_info.total_size];
            count_flags_in_block_simple(block, required_flags, forbidden_flags)
        })
        .try_reduce(|| 0, |a, b| Ok(a + b))?;

    Ok(total_count)
}

fn count_flags_in_decompressed_block(
    decompressed_data: &[u8],
    required_flags: u16,
    forbidden_flags: u16,
) -> Result<u64> {
    // Check if this is a header block
    if decompressed_data.len() >= 4 && &decompressed_data[0..4] == b"BAM\x01" {
        return Ok(0);
    }

    let mut count = 0;
    let mut pos = 0;

    unsafe {
        let data_ptr = decompressed_data.as_ptr();
        let data_len = decompressed_data.len();

        while pos + 4 <= data_len {
            let rec_size =
                u32::from_le(ptr::read_unaligned(data_ptr.add(pos) as *const u32)) as usize;

            if pos + 4 + rec_size > data_len {
                break;
            }

            let record_body = std::slice::from_raw_parts(data_ptr.add(pos + 4), rec_size);

            if rec_size >= 16 {
                let flags = u16::from_le_bytes([record_body[14], record_body[15]]);

                if (flags & required_flags) == required_flags && (flags & forbidden_flags) == 0 {
                    count += 1;
                }
            }

            pos += 4 + rec_size;
        }
    }

    Ok(count)
}

fn count_flags_in_block_simple(
    block: &[u8],
    required_flags: u16,
    forbidden_flags: u16,
) -> Result<u64> {
    thread_local! {
        static DECOMPRESSOR: RefCell<Decompressor> = RefCell::new(Decompressor::new());
        static OUTPUT_BUFFER: RefCell<Vec<u8>> = RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
    }

    DECOMPRESSOR.with(|decomp| {
        OUTPUT_BUFFER.with(|buf| {
            let mut decompressor = decomp.borrow_mut();
            let mut output_buffer = buf.borrow_mut();

            let decompressed_size = decompressor
                .gzip_decompress(block, &mut output_buffer)
                .map_err(|e| anyhow!("Decompression failed: {:?}", e))?;

            if decompressed_size >= 4 && &output_buffer[0..4] == b"BAM\x01" {
                return Ok(0);
            }

            let mut count = 0;
            let mut pos = 0;

            unsafe {
                let data_ptr = output_buffer.as_ptr();
                while pos + 4 <= decompressed_size {
                    let rec_size =
                        u32::from_le(ptr::read_unaligned(data_ptr.add(pos) as *const u32)) as usize;

                    if pos + 4 + rec_size > decompressed_size {
                        break;
                    }

                    let record_body = std::slice::from_raw_parts(data_ptr.add(pos + 4), rec_size);

                    if rec_size >= 16 {
                        let flags = u16::from_le_bytes([record_body[14], record_body[15]]);
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
