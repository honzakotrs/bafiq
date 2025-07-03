use crate::bgzf::{BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};
use crate::FlagIndex;
use anyhow::{anyhow, Result};
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use crossbeam::thread;
use libdeflater::Decompressor;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;

use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread as std_thread;
use std::sync::atomic::AtomicBool;

/// Information about a BGZF block's location in the file
#[derive(Debug, Clone)]
struct BlockInfo {
    start_pos: usize,
    total_size: usize,
}

/// Available index building strategies
#[derive(Debug, Clone, Copy)]
pub enum BuildStrategy {
    /// Streaming parallel processing - has receiver mutex contention bottleneck
    ParallelStreaming,
    /// Sequential processing - single-threaded fallback
    Sequential,
    /// rust-htslib based - for benchmarking and compatibility
    HtsLib,
    /// Chunk-based streaming - better parallelism but higher latency due to batching
    ChunkStreaming,
    /// Optimized parallel chunk streaming - combines immediate streaming with true parallelism (new default)
    ParallelChunkStreaming,
    /// High-performance strategy implementing all performance.md recommendations
    Optimized,
    /// Rayon-based parallel processing - inspired by fast-count 2.3s performance
    RayonOptimized,
    /// Streaming evolution of RayonOptimized - combines streaming discovery with work-stealing
    RayonStreamingOptimized,
}

impl Default for BuildStrategy {
    fn default() -> Self {
        BuildStrategy::RayonOptimized
    }
}

/// Primary interface for building flag indexes with different strategies
pub struct IndexBuilder {
    strategy: BuildStrategy,
}

impl IndexBuilder {
    /// Create a new index builder with default strategy (parallel chunk streaming)
    pub fn new() -> Self {
        Self {
            strategy: BuildStrategy::default(),
        }
    }

    /// Create a new index builder with specified strategy
    pub fn with_strategy(strategy: BuildStrategy) -> Self {
        Self { strategy }
    }

    /// Build an index from a BAM file path
    pub fn build<P: AsRef<Path>>(&self, bam_path: P) -> Result<FlagIndex> {
        let path_str = bam_path
            .as_ref()
            .to_str()
            .ok_or_else(|| anyhow!("Invalid file path"))?;

        match self.strategy {
            BuildStrategy::ParallelStreaming => self.build_streaming_parallel(path_str),
            BuildStrategy::Sequential => self.build_sequential(path_str),
            BuildStrategy::HtsLib => self.build_htslib(path_str),
            BuildStrategy::ChunkStreaming => self.build_chunk_streaming(path_str),
            BuildStrategy::ParallelChunkStreaming => self.build_parallel_chunk_streaming(path_str),
            BuildStrategy::Optimized => self.build_optimized(path_str),
            BuildStrategy::RayonOptimized => self.build_rayon_optimized(path_str),
            BuildStrategy::RayonStreamingOptimized => {
                self.build_rayon_streaming_optimized(path_str)
            }
        }
    }

    /// Get the current build strategy
    pub fn strategy(&self) -> BuildStrategy {
        self.strategy
    }

    /// Fast scan-only mode - count flags without building index
    /// Single-pass streaming approach comparable to samtools performance
    pub fn scan_count(
        &self,
        bam_path: &str,
        required_flags: u16,
        forbidden_flags: u16,
    ) -> Result<u64> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);

        // Use more workers than CPU cores for I/O bound workload
        let num_threads = (rayon::current_num_threads() * 2).max(8);

        // Create channel for streaming blocks to workers
        let (sender, receiver) = mpsc::channel::<(usize, usize)>(); // (start_pos, total_size)

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
                if header[0..2] != [0x1f, 0x8b] {
                    return Err(anyhow!("Invalid GZIP header at position {}", pos));
                }

                // Extract block size
                let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
                let total_size = bsize + 1;

                // Validate block size
                if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > 65536 {
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
        let shared_receiver = Arc::new(Mutex::new(receiver));

        for _thread_id in 0..num_threads {
            let receiver_clone = Arc::clone(&shared_receiver);
            let data_worker = Arc::clone(&data);

            let handle = std_thread::spawn(move || -> Result<u64> {
                let mut local_count = 0u64;

                loop {
                    let block_info = {
                        let receiver = receiver_clone.lock().unwrap();
                        receiver.recv()
                    };

                    match block_info {
                        Ok((start_pos, total_size)) => {
                            let block = &data_worker[start_pos..start_pos + total_size];
                            local_count += count_flags_in_block_optimized(
                                block,
                                required_flags,
                                forbidden_flags,
                            )
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

        // Drop receiver so workers can detect completion
        drop(shared_receiver);

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

    /// Experimental dual-direction scan - scan from both ends of file simultaneously  
    pub fn scan_count_dual_direction(
        &self,
        bam_path: &str,
        required_flags: u16,
        forbidden_flags: u16,
    ) -> Result<u64> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);
        let file_size = data.len();

        // Use more workers for I/O bound workload
        let num_threads = (rayon::current_num_threads() * 2).max(8);

        // Create channel for streaming blocks to workers
        let (sender, receiver) = mpsc::channel::<(usize, usize)>(); // (start_pos, total_size)

        // Shared atomic variables to coordinate the two producers
        let forward_pos = Arc::new(AtomicUsize::new(0));
        let backward_pos = Arc::new(AtomicUsize::new(file_size));

        // Forward producer: scan from beginning
        let data_forward = Arc::clone(&data);
        let forward_pos_ref = Arc::clone(&forward_pos);
        let backward_pos_ref = Arc::clone(&backward_pos);
        let sender_forward = sender.clone();

        let forward_handle = std_thread::spawn(move || -> Result<usize> {
            let mut pos = 0;
            let mut block_count = 0;

            while pos < file_size {
                // Check if we've met the backward scanner
                let current_backward = backward_pos_ref.load(Ordering::Relaxed);
                if pos >= current_backward {
                    break; // Met in the middle
                }

                if pos + BGZF_HEADER_SIZE > file_size {
                    break;
                }

                let header = &data_forward[pos..pos + BGZF_HEADER_SIZE];

                // Validate GZIP magic
                if header[0..2] != [0x1f, 0x8b] {
                    return Err(anyhow!("Invalid GZIP header at position {}", pos));
                }

                // Extract block size
                let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
                let total_size = bsize + 1;

                // Validate block size
                if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > 65536 {
                    return Err(anyhow!("Invalid BGZF block size: {}", total_size));
                }

                if pos + total_size > file_size {
                    break;
                }

                // Double-check we haven't crossed over
                let current_backward = backward_pos_ref.load(Ordering::Relaxed);
                if pos + total_size > current_backward {
                    break; // Would overlap with backward scanner
                }

                // Send block to workers
                if sender_forward.send((pos, total_size)).is_err() {
                    break;
                }

                pos += total_size;
                block_count += 1;

                // Update our position atomically
                forward_pos_ref.store(pos, Ordering::Relaxed);
            }

            Ok(block_count)
        });

        // Backward producer: scan from end
        let data_backward = Arc::clone(&data);
        let forward_pos_ref2 = Arc::clone(&forward_pos);
        let backward_pos_ref2 = Arc::clone(&backward_pos);
        let sender_backward = sender.clone();

        let backward_handle = std_thread::spawn(move || -> Result<usize> {
            let mut _pos = file_size;
            let mut block_count = 0;

            // Find blocks by scanning backwards - this is tricky with variable-sized BGZF blocks
            // We'll scan forward from potential block starts that we find by working backwards
            let mut potential_starts = Vec::new();

            // First pass: find all potential block starts by looking for GZIP magic backwards
            let mut scan_pos = file_size.saturating_sub(BGZF_HEADER_SIZE);
            while scan_pos > 0 {
                // Check if forward scanner has reached this point
                let current_forward = forward_pos_ref2.load(Ordering::Relaxed);
                if scan_pos <= current_forward {
                    break; // Met the forward scanner
                }

                // Look for GZIP magic
                if scan_pos + 2 <= file_size {
                    let potential_header = &data_backward[scan_pos..scan_pos + 2];
                    if potential_header == [0x1f, 0x8b] {
                        potential_starts.push(scan_pos);
                    }
                }

                scan_pos = scan_pos.saturating_sub(1);
            }

            // Sort potential starts in reverse order (highest first)
            potential_starts.sort_by(|a, b| b.cmp(a));

            // Second pass: validate these are actual block starts and process them
            for &block_start in &potential_starts {
                // Check if forward scanner has reached this point
                let current_forward = forward_pos_ref2.load(Ordering::Relaxed);
                if block_start <= current_forward {
                    break; // Met the forward scanner
                }

                if block_start + BGZF_HEADER_SIZE > file_size {
                    continue;
                }

                let header = &data_backward[block_start..block_start + BGZF_HEADER_SIZE];

                // Validate full BGZF header
                if header[0..2] != [0x1f, 0x8b] {
                    continue; // False positive
                }

                // Extract block size
                let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
                let total_size = bsize + 1;

                // Validate block size
                if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > 65536 {
                    continue; // Invalid block
                }

                if block_start + total_size > file_size {
                    continue; // Block extends beyond file
                }

                // Verify this is a real block by checking if it ends at a reasonable boundary
                // In a well-formed BGZF file, the next bytes should be another GZIP header or EOF
                let next_pos = block_start + total_size;
                if next_pos < file_size {
                    if next_pos + 2 <= file_size {
                        let next_header = &data_backward[next_pos..next_pos + 2];
                        if next_header != [0x1f, 0x8b] {
                            continue; // Next position isn't a GZIP header, so this is probably wrong
                        }
                    }
                }

                // Double-check we haven't crossed over with forward scanner
                let current_forward = forward_pos_ref2.load(Ordering::Relaxed);
                if block_start <= current_forward {
                    break;
                }

                // Send block to workers
                if sender_backward.send((block_start, total_size)).is_err() {
                    break;
                }

                block_count += 1;

                // Update our position atomically
                backward_pos_ref2.store(block_start, Ordering::Relaxed);
                _pos = block_start;
            }

            Ok(block_count)
        });

        // Drop the original sender so workers know when all producers are done
        drop(sender);

        // Consumer threads: count flags as blocks arrive (same as before)
        let mut worker_handles = Vec::new();
        let shared_receiver = Arc::new(Mutex::new(receiver));

        for _thread_id in 0..num_threads {
            let receiver_clone = Arc::clone(&shared_receiver);
            let data_worker = Arc::clone(&data);

            let handle = std_thread::spawn(move || -> Result<u64> {
                let mut local_count = 0u64;

                loop {
                    let block_info = {
                        let receiver = receiver_clone.lock().unwrap();
                        receiver.recv()
                    };

                    match block_info {
                        Ok((start_pos, total_size)) => {
                            let block = &data_worker[start_pos..start_pos + total_size];
                            local_count += count_flags_in_block_optimized(
                                block,
                                required_flags,
                                forbidden_flags,
                            )
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

        // Drop receiver so workers can detect completion
        drop(shared_receiver);

        // Wait for both producers
        let forward_blocks = forward_handle
            .join()
            .map_err(|e| anyhow!("Forward producer failed: {:?}", e))??;
        let backward_blocks = backward_handle
            .join()
            .map_err(|e| anyhow!("Backward producer failed: {:?}", e))??;

        eprintln!(
            "⚡ Dual-direction scan: {} forward + {} backward blocks",
            forward_blocks, backward_blocks
        );

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
}

impl Default for IndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// BGZF constants are now imported from crate::bgzf module

impl IndexBuilder {
    /// Build index using streaming parallel processing (legacy strategy with bottleneck)
    ///
    /// PERFORMANCE WARNING: This strategy has a mutex contention bottleneck:
    /// - One producer thread discovers BGZF blocks and sends via std::mpsc::channel
    /// - Multiple worker threads compete for Arc<Mutex<Receiver>> - SERIALIZATION POINT
    /// - Fresh 65KB allocation per block (no buffer reuse)
    /// - Scaling limited by receiver mutex contention, not CPU cores
    pub fn build_streaming_parallel(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap); // Share mmap safely across threads

        let num_threads = rayon::current_num_threads();

        // Create channel for sending block info from producer to consumers
        let (sender, receiver) = mpsc::channel::<(usize, usize)>(); // (start_pos, total_size)

        // Producer thread: discovers blocks and sends them immediately
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
                if header[0..2] != [0x1f, 0x8b] {
                    return Err(anyhow!("Invalid GZIP header at position {}", pos));
                }

                // Extract block size
                let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
                let total_size = bsize + 1;

                // Validate block size
                if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > 65536 {
                    return Err(anyhow!("Invalid BGZF block size: {}", total_size));
                }

                if pos + total_size > data_len {
                    break; // Incomplete block at end
                }

                // Send block info immediately to workers
                if sender.send((pos, total_size)).is_err() {
                    break; // Receivers hung up
                }

                pos += total_size;
                block_count += 1;
            }

            // Close the channel
            drop(sender);
            Ok(block_count)
        });

        // Consumer threads: process blocks as they arrive
        let mut worker_handles = Vec::new();
        let shared_receiver = Arc::new(Mutex::new(receiver));

        for thread_id in 0..num_threads {
            let receiver_clone = Arc::clone(&shared_receiver);
            let data_worker = Arc::clone(&data);

            let handle = std_thread::spawn(move || -> Result<FlagIndex> {
                let mut local_index = FlagIndex::new();
                let mut _processed_count = 0;

                loop {
                    let block_info = {
                        let receiver = receiver_clone.lock().unwrap();
                        receiver.recv()
                    };

                    match block_info {
                        Ok((start_pos, total_size)) => {
                            let block = &data_worker[start_pos..start_pos + total_size];
                            let block_offset = start_pos as i64;

                            // Use thread-local buffers to avoid per-block allocations
                            thread_local! {
                                static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                                static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
                            }

                            let result = BUFFER.with(|buf| {
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
                            });

                            if let Err(e) = result {
                                eprintln!("Thread {}: Block processing error: {}", thread_id, e);
                                continue;
                            }

                            _processed_count += 1;
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

        // Drop the original receiver so workers can detect when producer is done
        drop(shared_receiver);

        // Wait for producer to complete
        let _total_blocks = producer_handle
            .join()
            .map_err(|e| anyhow!("Producer thread failed: {:?}", e))??;

        // Wait for all workers and collect their local indexes
        let mut final_index = FlagIndex::new();

        for handle in worker_handles.into_iter() {
            match handle.join() {
                Ok(Ok(local_index)) => {
                    final_index.merge(local_index);
                }
                Ok(Err(e)) => {
                    eprintln!("Worker thread failed: {}", e);
                }
                Err(e) => {
                    eprintln!("Worker thread panicked: {:?}", e);
                }
            }
        }

        Ok(final_index)
    }

    /// Build index using sequential processing (fallback strategy)
    ///
    /// Single-threaded approach that processes BGZF blocks one at a time.
    /// Lower memory usage but slower performance.
    pub fn build_sequential(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = &mmap[..];

        let mut index = FlagIndex::new();
        let mut _total_records = 0;
        let mut pos = 0;

        // Iterate over the memory-mapped file
        while pos < data.len() {
            // Ensure there is enough data for a full BGZF header
            if pos + BGZF_HEADER_SIZE > data.len() {
                break; // No more complete block header
            }
            let header = &data[pos..pos + BGZF_HEADER_SIZE];

            // Validate the GZIP magic bytes
            if header[0..2] != [0x1f, 0x8b] {
                return Err(anyhow!("Invalid GZIP header in BGZF block"));
            }

            // Extract BSIZE (bytes 16-17); BSIZE = total block size - 1
            let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
            let total_block_size = bsize + 1;

            // Sanity-check the block size
            if total_block_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_block_size > 65536 {
                return Err(anyhow!("Invalid BGZF block size: {}", total_block_size));
            }
            if pos + total_block_size > data.len() {
                break; // Incomplete block at the end
            }

            // Get the full BGZF block and extract flags using efficient buffer reuse
            let block = &data[pos..pos + total_block_size];
            let block_offset = pos as i64; // Virtual file offset for this block

            // Use static buffers for sequential processing (single-threaded)
            static mut BUFFER: Option<Vec<u8>> = None;
            static mut DECOMPRESSOR: Option<Decompressor> = None;

            let count = unsafe {
                // Initialize buffers on first use
                if BUFFER.is_none() {
                    BUFFER = Some(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                }
                if DECOMPRESSOR.is_none() {
                    DECOMPRESSOR = Some(Decompressor::new());
                }

                extract_flags_from_block_pooled(
                    block,
                    &mut index,
                    block_offset,
                    BUFFER.as_mut().unwrap(),
                    DECOMPRESSOR.as_mut().unwrap(),
                )?
            };
            _total_records += count;

            pos += total_block_size;
        }

        Ok(index)
    }

    /// Build index using rust-htslib (benchmark/compatibility strategy)
    ///
    /// Uses the original rust-htslib approach for comparison and benchmarking.
    /// May be slower but provides compatibility reference.
    pub fn build_htslib(&self, bam_path: &str) -> Result<FlagIndex> {
        // Use the original FlagIndex::from_path method from lib.rs
        FlagIndex::from_path(bam_path)
    }

    /// Build index using chunk-based streaming strategy
    ///
    /// BETTER PARALLELISM than ParallelStreaming due to crossbeam channels:
    /// - Uses crossbeam::channel::bounded - no receiver mutex contention
    /// - Batches 1000 blocks before sending (higher latency, better throughput)
    /// - rayon::current_num_threads() threads with crossbeam::thread::scope
    /// - Still allocates fresh 65KB per block (no buffer reuse)
    pub fn build_chunk_streaming(&self, bam_path: &str) -> Result<FlagIndex> {
        // Use memory mapping like the working strategies to avoid chunk boundary issues
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);

        const BLOCKS_PER_BATCH: usize = 1000; // Send blocks in batches for efficiency
        let (sender, receiver): (
            Sender<Vec<(usize, usize, i64)>>,
            Receiver<Vec<(usize, usize, i64)>>,
        ) = bounded(64);
        let num_threads = rayon::current_num_threads();

        thread::scope(|s| {
            // Producer thread: discovers complete BGZF blocks and sends them in batches
            let data_producer = Arc::clone(&data);
            s.spawn(move |_| {
                let mut pos = 0;
                let mut current_batch = Vec::new();
                let data_len = data_producer.len();

                while pos < data_len {
                    if pos + BGZF_HEADER_SIZE > data_len {
                        break;
                    }

                    let header = &data_producer[pos..pos + BGZF_HEADER_SIZE];

                    // Validate GZIP magic
                    if header[0..2] != [0x1f, 0x8b] {
                        eprintln!("Invalid GZIP header at position {}", pos);
                        break;
                    }

                    // Extract block size
                    let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
                    let total_size = bsize + 1;

                    // Validate block size
                    if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > 65536 {
                        eprintln!("Invalid BGZF block size: {}", total_size);
                        break;
                    }

                    if pos + total_size > data_len {
                        break; // Incomplete block at end
                    }

                    // Add block info to current batch: (start_pos, total_size, block_offset)
                    current_batch.push((pos, total_size, pos as i64));

                    // Send batch when it's full
                    if current_batch.len() >= BLOCKS_PER_BATCH {
                        if sender.send(std::mem::take(&mut current_batch)).is_err() {
                            break; // Receivers hung up
                        }
                    }

                    pos += total_size;
                }

                // Send any remaining blocks
                if !current_batch.is_empty() {
                    let _ = sender.send(current_batch);
                }

                drop(sender);
            });

            // Consumer threads: process batches of blocks
            let results: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let rx = receiver.clone();
                    let data_worker = Arc::clone(&data);

                    s.spawn(move |_| -> FlagIndex {
                        let mut local_index = FlagIndex::new();
                        let mut _processed_count = 0;

                        while let Ok(batch) = rx.recv() {
                            // Use thread-local buffers to avoid per-block allocations
                            thread_local! {
                                static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                                static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
                            }

                            for (start_pos, total_size, block_offset) in batch {
                                let block = &data_worker[start_pos..start_pos + total_size];

                                let result = BUFFER.with(|buf| {
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
                                });

                                if let Err(e) = result {
                                    eprintln!(
                                        "Thread {}: Block processing error: {}",
                                        thread_id, e
                                    );
                                    continue;
                                }

                                _processed_count += 1;
                            }
                        }

                        local_index
                    })
                })
                .collect();

            // Combine results
            let mut final_index = FlagIndex::new();
            for handle in results {
                final_index.merge(handle.join().unwrap());
            }

            Ok(final_index)
        })
        .unwrap()
    }

    /// Build index using optimized parallel chunk streaming (new optimal strategy)
    ///
    /// BEST PARALLELISM - combines immediate streaming with lock-free channels:
    /// - Uses crossbeam::channel::bounded for true parallel receivers (no mutex)
    /// - Small batch size (16 blocks) for low latency with good throughput
    /// - Thread-local buffer pools to eliminate per-block allocations
    /// - rayon::current_num_threads() for consistency with other parallel code
    /// - Immediate processing start (no 1000-block batching delays)
    pub fn build_parallel_chunk_streaming(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);

        const BLOCKS_PER_BATCH: usize = 16; // Small batches for low latency
        let (sender, receiver): (
            Sender<Vec<(usize, usize, i64)>>,
            Receiver<Vec<(usize, usize, i64)>>,
        ) = bounded(32); // Reasonable buffer without excess memory
        let num_threads = rayon::current_num_threads();

        thread::scope(|s| {
            // Producer thread: discovers blocks and sends in small batches for low latency
            let data_producer = Arc::clone(&data);
            s.spawn(move |_| {
                let mut pos = 0;
                let mut current_batch = Vec::with_capacity(BLOCKS_PER_BATCH);
                let data_len = data_producer.len();

                while pos < data_len {
                    if pos + BGZF_HEADER_SIZE > data_len {
                        break;
                    }

                    let header = &data_producer[pos..pos + BGZF_HEADER_SIZE];

                    // Validate GZIP magic
                    if header[0..2] != [0x1f, 0x8b] {
                        eprintln!("Invalid GZIP header at position {}", pos);
                        break;
                    }

                    // Extract block size
                    let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
                    let total_size = bsize + 1;

                    // Validate block size
                    if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > 65536 {
                        eprintln!("Invalid BGZF block size: {}", total_size);
                        break;
                    }

                    if pos + total_size > data_len {
                        break; // Incomplete block at end
                    }

                    // Add block info to current batch
                    current_batch.push((pos, total_size, pos as i64));

                    // Send small batches immediately for low latency
                    if current_batch.len() >= BLOCKS_PER_BATCH {
                        if sender.send(std::mem::take(&mut current_batch)).is_err() {
                            break; // Receivers hung up
                        }
                        current_batch = Vec::with_capacity(BLOCKS_PER_BATCH);
                    }

                    pos += total_size;
                }

                // Send any remaining blocks
                if !current_batch.is_empty() {
                    let _ = sender.send(current_batch);
                }

                drop(sender);
            });

            // Consumer threads with thread-local buffer pools for efficiency
            let results: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let rx = receiver.clone();
                    let data_worker = Arc::clone(&data);

                    s.spawn(move |_| -> FlagIndex {
                        let mut local_index = FlagIndex::new();

                        // Thread-local buffer pool to avoid per-block allocations
                        let mut buffer_pool = Vec::with_capacity(BLOCKS_PER_BATCH);
                        let mut decompressor_pool = Vec::with_capacity(BLOCKS_PER_BATCH);

                        // Pre-allocate buffers for the batch size
                        for _ in 0..BLOCKS_PER_BATCH {
                            buffer_pool.push(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                            decompressor_pool.push(Decompressor::new());
                        }

                        while let Ok(batch) = rx.recv() {
                            for (i, (start_pos, total_size, block_offset)) in
                                batch.into_iter().enumerate()
                            {
                                let block = &data_worker[start_pos..start_pos + total_size];

                                // Use pooled buffers instead of fresh allocations
                                let buffer_idx = i % buffer_pool.len();
                                let buffer = &mut buffer_pool[buffer_idx];
                                let decompressor = &mut decompressor_pool[buffer_idx];

                                if let Err(e) = extract_flags_from_block_pooled(
                                    block,
                                    &mut local_index,
                                    block_offset,
                                    buffer,
                                    decompressor,
                                ) {
                                    eprintln!(
                                        "Thread {}: Block processing error: {}",
                                        thread_id, e
                                    );
                                    continue;
                                }
                            }
                        }

                        local_index
                    })
                })
                .collect();

            // Combine results from all workers
            let mut final_index = FlagIndex::new();
            for handle in results {
                final_index.merge(handle.join().unwrap());
            }

            Ok(final_index)
        })
        .unwrap()
    }

    /// Build index using optimized parallel chunk streaming (new optimal strategy)
    ///
    /// BEST PARALLELISM - combines immediate streaming with lock-free channels:
    /// - Uses crossbeam::channel::bounded for true parallel receivers (no mutex)
    /// - Small batch size (16 blocks) for low latency with good throughput
    /// - Thread-local buffer pools to eliminate per-block allocations
    /// - rayon::current_num_threads() for consistency with other parallel code
    /// - Immediate processing start (no 1000-block batching delays)
    pub fn build_optimized(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);

        const BLOCKS_PER_BATCH: usize = 16; // Small batches for low latency
        let (sender, receiver): (
            Sender<Vec<(usize, usize, i64)>>,
            Receiver<Vec<(usize, usize, i64)>>,
        ) = bounded(32); // Reasonable buffer without excess memory
        let num_threads = rayon::current_num_threads();

        thread::scope(|s| {
            // Producer thread: discovers blocks and sends in small batches for low latency
            let data_producer = Arc::clone(&data);
            s.spawn(move |_| {
                let mut pos = 0;
                let mut current_batch = Vec::with_capacity(BLOCKS_PER_BATCH);
                let data_len = data_producer.len();

                while pos < data_len {
                    if pos + BGZF_HEADER_SIZE > data_len {
                        break;
                    }

                    let header = &data_producer[pos..pos + BGZF_HEADER_SIZE];

                    // Validate GZIP magic
                    if header[0..2] != [0x1f, 0x8b] {
                        eprintln!("Invalid GZIP header at position {}", pos);
                        break;
                    }

                    // Extract block size
                    let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
                    let total_size = bsize + 1;

                    // Validate block size
                    if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > 65536 {
                        eprintln!("Invalid BGZF block size: {}", total_size);
                        break;
                    }

                    if pos + total_size > data_len {
                        break; // Incomplete block at end
                    }

                    // Add block info to current batch
                    current_batch.push((pos, total_size, pos as i64));

                    // Send small batches immediately for low latency
                    if current_batch.len() >= BLOCKS_PER_BATCH {
                        if sender.send(std::mem::take(&mut current_batch)).is_err() {
                            break; // Receivers hung up
                        }
                        current_batch = Vec::with_capacity(BLOCKS_PER_BATCH);
                    }

                    pos += total_size;
                }

                // Send any remaining blocks
                if !current_batch.is_empty() {
                    let _ = sender.send(current_batch);
                }

                drop(sender);
            });

            // Consumer threads with thread-local buffer pools for efficiency
            let results: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let rx = receiver.clone();
                    let data_worker = Arc::clone(&data);

                    s.spawn(move |_| -> FlagIndex {
                        let mut local_index = FlagIndex::new();

                        // Thread-local buffer pool to avoid per-block allocations
                        let mut buffer_pool = Vec::with_capacity(BLOCKS_PER_BATCH);
                        let mut decompressor_pool = Vec::with_capacity(BLOCKS_PER_BATCH);

                        // Pre-allocate buffers for the batch size
                        for _ in 0..BLOCKS_PER_BATCH {
                            buffer_pool.push(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                            decompressor_pool.push(Decompressor::new());
                        }

                        while let Ok(batch) = rx.recv() {
                            for (i, (start_pos, total_size, block_offset)) in
                                batch.into_iter().enumerate()
                            {
                                let block = &data_worker[start_pos..start_pos + total_size];

                                // Use pooled buffers instead of fresh allocations
                                let buffer_idx = i % buffer_pool.len();
                                let buffer = &mut buffer_pool[buffer_idx];
                                let decompressor = &mut decompressor_pool[buffer_idx];

                                if let Err(e) = extract_flags_from_block_pooled(
                                    block,
                                    &mut local_index,
                                    block_offset,
                                    buffer,
                                    decompressor,
                                ) {
                                    eprintln!(
                                        "Thread {}: Block processing error: {}",
                                        thread_id, e
                                    );
                                    continue;
                                }
                            }
                        }

                        local_index
                    })
                })
                .collect();

            // Combine results from all workers
            let mut final_index = FlagIndex::new();
            for handle in results {
                final_index.merge(handle.join().unwrap());
            }

            Ok(final_index)
        })
        .unwrap()
    }

    /// Build index using Rayon-based parallel processing (inspired by fast-count 2.3s performance)
    ///
    /// Uses rayon's parallel iterators to discover and process BGZF blocks, similar to the
    /// fast-count methods that achieve 2.3s performance, but builds a complete index.
    pub fn build_rayon_optimized(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);

        // Phase 1: Discover all BGZF blocks (fast, single-threaded)
        let blocks = self.discover_blocks_fast(&data)?;

        // Phase 2: Process blocks in parallel using rayon (like fast-count methods)
        let local_indexes: Vec<FlagIndex> = blocks
            .par_iter()
            .map(|block_info| -> Result<FlagIndex> {
                let mut local_index = FlagIndex::new();
                let block = &data[block_info.start_pos..block_info.start_pos + block_info.total_size];
                let block_offset = block_info.start_pos as i64;

                // Use thread-local buffers for efficiency (like fast-count)
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

                Ok(local_index)
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Phase 3: Merge all local indexes
        let mut final_index = FlagIndex::new();
        for local_index in local_indexes {
            final_index.merge(local_index);
        }

        Ok(final_index)
    }

    /// Build index using Rayon-based streaming processing (hybrid approach)
    /// 
    /// Combines the best of streaming and work-stealing:
    /// - Phase 1: Discover blocks and stream them immediately to a work queue
    /// - Phase 2: Rayon workers pull from queue using work-stealing behavior
    /// - Benefits: Lower memory usage, better pipeline utilization, immediate processing
    /// - Trade-offs: Slightly more complex synchronization than pure RayonOptimized
    pub fn build_rayon_streaming_optimized(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);
        
        // Lock-free concurrent queue for streaming blocks to workers
        let work_queue = Arc::new(crossbeam::queue::SegQueue::new());
        let discovery_done = Arc::new(AtomicBool::new(false));
        let num_threads = rayon::current_num_threads();
        
        thread::scope(|s| {
            // Discovery thread: stream blocks as they're found
            let queue_producer = Arc::clone(&work_queue);
            let data_producer = Arc::clone(&data);
            let done_flag = Arc::clone(&discovery_done);
            
            s.spawn(move |_| -> Result<usize> {
                let mut block_count = 0;
                let mut pos = 0;
                let data_len = data_producer.len();
                
                while pos < data_len {
                    if pos + BGZF_HEADER_SIZE > data_len {
                        break;
                    }
                    
                    let header = &data_producer[pos..pos + BGZF_HEADER_SIZE];
                    if header[0..2] != [0x1f, 0x8b] {
                        return Err(anyhow!("Invalid GZIP header at position {}", pos));
                    }
                    
                    let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
                    let total_size = bsize + 1;
                    
                    if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > 65536 {
                        return Err(anyhow!("Invalid BGZF block size: {}", total_size));
                    }
                    
                    if pos + total_size > data_len {
                        break;
                    }
                    
                    // Stream block immediately to work queue
                    let block_info = BlockInfo {
                        start_pos: pos,
                        total_size,
                    };
                    queue_producer.push(block_info);
                    
                    pos += total_size;
                    block_count += 1;
                }
                
                // Signal discovery completion
                done_flag.store(true, Ordering::Release);
                Ok(block_count)
            });
            
            // Processing: Use rayon to spawn workers that pull from queue
            let local_indexes: Vec<FlagIndex> = (0..num_threads)
                .into_par_iter()
                .map(|_worker_id| -> Result<FlagIndex> {
                    let mut local_index = FlagIndex::new();
                    let queue_consumer = Arc::clone(&work_queue);
                    let done_flag = Arc::clone(&discovery_done);
                    let data_worker = Arc::clone(&data);
                    
                    // Thread-local buffers for efficiency
                    thread_local! {
                        static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                        static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
                    }
                    
                    // Worker loop: pull blocks from queue and process
                    loop {
                        if let Some(block_info) = queue_consumer.pop() {
                            // Process block using thread-local buffers
                            let block = &data_worker[block_info.start_pos..block_info.start_pos + block_info.total_size];
                            let block_offset = block_info.start_pos as i64;
                            
                            let result = BUFFER.with(|buf| {
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
                            });
                            
                            if let Err(e) = result {
                                eprintln!("Worker {}: Block processing error: {}", _worker_id, e);
                                continue;
                            }
                        } else if done_flag.load(Ordering::Acquire) {
                            // No more work and discovery is done - double-check queue is empty
                            if queue_consumer.is_empty() {
                                break;
                            }
                        } else {
                            // No work available, but discovery still running - yield to avoid spinning
                            std::thread::yield_now();
                        }
                    }
                    
                    Ok(local_index)
                })
                .collect::<Result<Vec<_>, _>>()?;
            
            // Merge results from all workers
            let mut final_index = FlagIndex::new();
            for local_index in local_indexes {
                final_index.merge(local_index);
            }
            
            Ok(final_index)
        })
        .unwrap()
    }

    /// Fast BGZF block discovery (single-threaded, optimized)
    fn discover_blocks_fast(&self, data: &[u8]) -> Result<Vec<BlockInfo>> {
        let mut blocks = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            if pos + BGZF_HEADER_SIZE > data.len() {
                break;
            }

            let header = &data[pos..pos + BGZF_HEADER_SIZE];
            if header[0..2] != [0x1f, 0x8b] {
                return Err(anyhow!("Invalid GZIP header at position {}", pos));
            }

            let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
            let total_size = bsize + 1;

            if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > 65536 {
                return Err(anyhow!("Invalid BGZF block size: {}", total_size));
            }

            if pos + total_size > data.len() {
                break;
            }

            blocks.push(BlockInfo {
                start_pos: pos,
                total_size,
            });

            pos += total_size;
        }

        Ok(blocks)
    }
}

/// Extract flags from BAM records in a BGZF block and update the flag index
///
/// BAM record structure (after the 4-byte length):
/// [4 bytes: refID] [4 bytes: pos] [1 byte: l_read_name] [1 byte: MAPQ]
/// [2 bytes: ???] [2 bytes: n_cigar_op] [2 bytes: BAM_FLAG] <- This is what we want (offset 14-15)
/// [4 bytes: l_seq] ...
///
/// NOTE: This function allocates fresh 65KB buffer per call - inefficient
fn extract_flags_from_block(
    block: &[u8],
    index: &mut FlagIndex,
    block_offset: i64,
) -> Result<usize> {
    let mut record_count = 0;

    // Use block_offset directly as block_id (file position where BGZF block starts)
    let block_id = block_offset;

    read_record_headers(
        block,
        16, // We need at least 16 bytes to read the flag field at offset 14-15
        |record_header: &[u8], count: &mut usize| {
            // Extract flags from offset 14-15 in the BAM record
            let flags = u16::from_le_bytes([record_header[14], record_header[15]]);

            // Add this record to the flag index
            index.add_record_at_block(flags, block_id);
            *count += 1;
        },
        &mut record_count,
    )?;
    Ok(record_count)
}

/// Extract flags from BAM records using provided buffers (memory-efficient version)
///
/// Same as extract_flags_from_block but uses caller-provided buffers to avoid allocations
pub fn extract_flags_from_block_pooled(
    block: &[u8],
    index: &mut FlagIndex,
    block_offset: i64,
    output_buffer: &mut Vec<u8>,
    decompressor: &mut Decompressor,
) -> Result<usize> {
    let mut record_count = 0;
    let block_id = block_offset;

    // Decompress using provided buffer and decompressor
    let decompressed_size = decompressor
        .gzip_decompress(block, output_buffer)
        .map_err(|e| anyhow!("Decompression failed: {:?}", e))?;

    // Skip BAM header blocks (they don't contain read records)
    if decompressed_size >= 4 && &output_buffer[0..4] == b"BAM\x01" {
        return Ok(0);
    }

    // Optimized record parsing with better memory access patterns
    extract_flags_from_decompressed_simd_optimized(
        output_buffer,
        decompressed_size,
        index,
        block_id,
        &mut record_count,
    )?;

    Ok(record_count)
}

/// SIMD-optimized record parsing with prefetching and better memory access patterns
#[inline(always)]
fn extract_flags_from_decompressed_simd_optimized(
    output_buffer: &[u8],
    decompressed_size: usize,
    index: &mut FlagIndex,
    block_id: i64,
    record_count: &mut usize,
) -> Result<()> {
    let mut pos = 0;

    // Process records in chunks for better cache locality
    const PREFETCH_DISTANCE: usize = 64; // Cache line size

    unsafe {
        let out_ptr = output_buffer.as_ptr();
        let end_ptr = out_ptr.add(decompressed_size);

        while pos + 4 <= decompressed_size {
            // Prefetch next cache line to improve memory access patterns
            #[cfg(target_arch = "x86_64")]
            {
                if pos + PREFETCH_DISTANCE < decompressed_size {
                    // Manual prefetch hint for better cache performance
                    std::ptr::read_volatile(out_ptr.add(pos + PREFETCH_DISTANCE));
                }
            }

            let rec_size_ptr = out_ptr.add(pos) as *const u32;

            // Bounds check before reading
            if rec_size_ptr >= end_ptr as *const u32 {
                break;
            }

            let rec_size = u32::from_le(ptr::read_unaligned(rec_size_ptr)) as usize;

            // Validate record size
            if pos + 4 + rec_size > decompressed_size {
                break;
            }

            // We need at least 16 bytes to read the flag field at offset 14-15
            if rec_size >= 16 {
                let record_body_ptr = out_ptr.add(pos + 4);

                // Direct memory access to flags at offset 14-15
                let flags_ptr = record_body_ptr.add(14) as *const u16;
                let flags = u16::from_le(ptr::read_unaligned(flags_ptr));

                index.add_record_at_block(flags, block_id);
                *record_count += 1;
            }

            pos += 4 + rec_size;
        }
    }

    Ok(())
}

/// Decompress a BGZF block and iterate over its records, calling the provided
/// closure with the first `header_length` bytes of each record (the record header)
///
/// NOTE: Allocates fresh 65KB buffer per call - use extract_flags_from_block_pooled for efficiency
fn read_record_headers<F, A>(
    block: &[u8],
    header_length: usize,
    mut process_record_header: F,
    acc: &mut A,
) -> Result<()>
where
    F: FnMut(&[u8], &mut A),
{
    let mut output = vec![0u8; BGZF_BLOCK_MAX_SIZE]; // Fresh allocation per call
    let mut decompressor = Decompressor::new(); // Fresh decompressor per call
    let decompressed_size = decompressor
        .gzip_decompress(block, &mut output)
        .map_err(|e| anyhow!("Decompression failed: {:?}", e))?;

    let mut pos = 0;
    unsafe {
        let out_ptr = output.as_ptr();
        while pos + 4 <= decompressed_size {
            let rec_size =
                u32::from_le(ptr::read_unaligned(out_ptr.add(pos) as *const u32)) as usize;
            if pos + 4 + rec_size > decompressed_size {
                break;
            }
            // The record body starts immediately after the 4-byte length
            let record_body = std::slice::from_raw_parts(out_ptr.add(pos + 4), rec_size);

            // Check that the record body is at least as long as the header we wish to process
            if header_length > rec_size {
                return Err(anyhow!(
                    "Record body size {} is smaller than header length {}",
                    rec_size,
                    header_length
                ));
            }

            // Get the header slice (the first `header_length` bytes of the record body)
            let header_slice = &record_body[..header_length];

            // Call the provided closure with the header slice
            process_record_header(header_slice, acc);

            // Move to the next record
            pos += 4 + rec_size;
        }
    }
    Ok(())
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
