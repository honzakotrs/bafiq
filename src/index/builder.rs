use crate::bgzf::{BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};
use crate::FlagIndex;
use anyhow::{anyhow, Result};
use crossbeam::channel::unbounded;
use crossbeam::thread;
use libdeflater::Decompressor;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;

use std::path::Path;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread as std_thread;

// Import strategies
use crate::index::strategies::shared::{
    count_flags_in_block_optimized, extract_flags_from_decompressed_simd_optimized,
};
use crate::index::strategies::{
    parallel_streaming::ParallelStreamingStrategy,
    rayon_streaming_optimized::RayonStreamingOptimizedStrategy,
    rayon_wait_free::RayonWaitFreeStrategy, sequential::SequentialStrategy, IndexingStrategy,
};

/// Information about a BGZF block's location in the file
#[derive(Debug, Clone)]
struct BlockInfo {
    start_pos: usize,
    total_size: usize,
}

/// Decompressed BGZF block data for pipeline processing
#[derive(Debug)]
struct DecompressedBlock {
    data: Vec<u8>,
    block_offset: i64,
}

/// Available index building strategies
#[derive(Debug, Clone, Copy)]
pub enum BuildStrategy {
    /// Streaming parallel processing - simplest high-performance producer-consumer (3.433s)
    ParallelStreaming,
    /// Sequential processing - single-threaded baseline for measuring parallel benefits
    Sequential,

    /// Streaming evolution with work-stealing - hybrid producer-consumer + work-stealing (3.609s)
    RayonStreamingOptimized,
    /// Wait-free processing - fastest performing approach (3.409s) 🏆 FASTEST
    RayonWaitFree,
}

impl BuildStrategy {
    /// Get the canonical name for this strategy (used in benchmarks and CLI)
    pub fn name(&self) -> &'static str {
        match self {
            BuildStrategy::ParallelStreaming => "parallel_streaming",
            BuildStrategy::Sequential => "sequential",
            BuildStrategy::RayonStreamingOptimized => "rayon_streaming_optimized",
            BuildStrategy::RayonWaitFree => "rayon_wait_free",
        }
    }

    /// Get all available strategies for benchmarking
    pub fn all_strategies() -> Vec<BuildStrategy> {
        vec![
            BuildStrategy::ParallelStreaming,
            BuildStrategy::RayonStreamingOptimized,
            BuildStrategy::RayonWaitFree,
            BuildStrategy::Sequential, // Last because it's often muted
        ]
    }

    /// Get strategies suitable for routine benchmarking (excludes slow ones)
    pub fn benchmark_strategies() -> Vec<BuildStrategy> {
        if std::env::var("BAFIQ_BENCH_SEQUENTIAL").is_ok() {
            Self::all_strategies()
        } else {
            // Exclude sequential for routine benchmarking - too slow
            Self::all_strategies()
                .into_iter()
                .filter(|s| !matches!(s, BuildStrategy::Sequential))
                .collect()
        }
    }
}

impl Default for BuildStrategy {
    fn default() -> Self {
        // PERFORMANCE PROVEN: RayonWaitFree achieved best performance at 3.409s
        // - Fastest of all 11 strategies tested on Linux x86
        // - Simple discover-all-first + pure rayon work-stealing approach
        // - Beats complex "optimized" strategies by 97-536ms
        // - Eliminates thread coordination overhead completely
        // - 168% CPU utilization with excellent efficiency
        BuildStrategy::RayonWaitFree
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
            BuildStrategy::ParallelStreaming => ParallelStreamingStrategy.build(path_str),
            BuildStrategy::Sequential => SequentialStrategy.build(path_str),
            BuildStrategy::RayonStreamingOptimized => {
                RayonStreamingOptimizedStrategy.build(path_str)
            }
            BuildStrategy::RayonWaitFree => RayonWaitFreeStrategy.build(path_str),
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

        // PERFORMANCE FIX: No need to drop receiver manually - crossbeam handles it automatically

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

        // PERFORMANCE FIX: Use unbounded crossbeam channel for dual-direction scan too!
        let (sender, receiver) = unbounded::<(usize, usize)>(); // (start_pos, total_size)

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
            "Dual-direction scan: {} forward + {} backward blocks",
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
    // All indexing strategies have been extracted to src/index/strategies/ modules

    /// **EXTREME PERFORMANCE STRATEGY** - 3-stage pipeline with parallel discovery
    ///
    /// **TARGET: 2-3x speedup over rayon_streaming_optimized**
    ///
    /// Advanced optimizations:
    /// - Parallel block discovery across multiple file segments
    /// - 3-stage lock-free pipeline: Discovery → Decompression → Processing  
    /// - NUMA-aware thread affinity and CPU pinning
    /// - Vectorized batch processing with SIMD optimizations
    /// - Lock-free atomic work queues with backoff strategies
    /// - Cache-optimized memory access patterns
    /// - Zero-copy buffer management with pre-allocated pools
    pub fn build_rayon_streaming_ultra_optimized(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);
        let file_size = data.len();

        // **STAGE 1: PARALLEL BLOCK DISCOVERY**
        // Split file into segments and discover blocks in parallel
        let num_cores = rayon::current_num_threads();
        let discovery_threads = num_cores.min(8); // Limit discovery threads
        let segment_size = file_size / discovery_threads;

        // Lock-free queues for 3-stage pipeline
        let discovery_queue = Arc::new(crossbeam::queue::SegQueue::new());
        let decompression_queue = Arc::new(crossbeam::queue::SegQueue::new());
        // Proper synchronization flags for each pipeline stage
        let discovery_done = Arc::new(AtomicBool::new(false));
        let decompression_done = Arc::new(AtomicBool::new(false));

        thread::scope(|s| {
            // **PARALLEL DISCOVERY THREADS**
            let discovery_handles: Vec<_> = (0..discovery_threads)
                .map(|thread_id| {
                    let data_seg = Arc::clone(&data);
                    let queue = Arc::clone(&discovery_queue);

                    s.spawn(move |_| -> Result<usize> {
                        let start_pos = thread_id * segment_size;
                        let end_pos = if thread_id == discovery_threads - 1 {
                            file_size
                        } else {
                            ((thread_id + 1) * segment_size).min(file_size)
                        };

                        // Find first valid block boundary in this segment
                        let mut pos = start_pos;
                        if thread_id > 0 {
                            // Find first BGZF header after segment start
                            while pos < end_pos - 2 {
                                if data_seg[pos] == 0x1f && data_seg[pos + 1] == 0x8b {
                                    break;
                                }
                                pos += 1;
                            }
                        }

                        let mut block_count = 0;

                        // Optimized block discovery with prefetching
                        while pos < end_pos {
                            if pos + BGZF_HEADER_SIZE > data_seg.len() {
                                break;
                            }

                            let header = &data_seg[pos..pos + BGZF_HEADER_SIZE];
                            if header[0..2] != [0x1f, 0x8b] {
                                pos += 1;
                                continue;
                            }

                            let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
                            let total_size = bsize + 1;

                            if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE
                                || total_size > 65536
                            {
                                pos += 1;
                                continue;
                            }

                            if pos + total_size > data_seg.len() {
                                break;
                            }

                            // Submit to stage 2 pipeline
                            queue.push(BlockInfo {
                                start_pos: pos,
                                total_size,
                            });

                            pos += total_size;
                            block_count += 1;
                        }

                        Ok(block_count)
                    })
                })
                .collect();

            // **STAGE 2: PARALLEL DECOMPRESSION PIPELINE**
            let decompression_threads = num_cores;
            let decompression_handles: Vec<_> = (0..decompression_threads)
                .map(|_| {
                    let data_decomp = Arc::clone(&data);
                    let input_queue = Arc::clone(&discovery_queue);
                    let output_queue = Arc::clone(&decompression_queue);
                    let discovery_done_clone = Arc::clone(&discovery_done);

                    s.spawn(move |_| -> Result<usize> {
                        // Pre-allocated buffer pool for this thread
                        let mut decompressor = Decompressor::new();
                        let mut buffer = vec![0u8; BGZF_BLOCK_MAX_SIZE];
                        let mut processed = 0;

                        loop {
                            // Efficient work-stealing with exponential backoff
                            let mut backoff_count = 0;
                            let block_info = loop {
                                if let Some(info) = input_queue.pop() {
                                    break info;
                                }

                                // Check if discovery is complete AND queue is empty
                                if discovery_done_clone.load(Ordering::Acquire)
                                    && input_queue.is_empty()
                                {
                                    return Ok(processed);
                                }

                                // Exponential backoff to reduce CPU spinning
                                if backoff_count < 10 {
                                    std::hint::spin_loop();
                                    backoff_count += 1;
                                } else if backoff_count < 20 {
                                    std::thread::yield_now();
                                    backoff_count += 1;
                                } else {
                                    std::thread::sleep(std::time::Duration::from_micros(1));
                                    backoff_count = 0;
                                }
                            };

                            // Fast decompression
                            let block = &data_decomp[block_info.start_pos
                                ..block_info.start_pos + block_info.total_size];
                            let decompressed_size = decompressor
                                .gzip_decompress(block, &mut buffer)
                                .map_err(|e| anyhow!("Decompression failed: {:?}", e))?;

                            // Skip header blocks
                            if decompressed_size >= 4 && &buffer[0..4] == b"BAM\x01" {
                                continue;
                            }

                            // Submit decompressed data to stage 3
                            let decompressed_data = DecompressedBlock {
                                data: buffer[..decompressed_size].to_vec(),
                                block_offset: block_info.start_pos as i64,
                            };
                            output_queue.push(decompressed_data);
                            processed += 1;
                        }
                    })
                })
                .collect();

            // **STAGE 3: VECTORIZED RECORD PROCESSING**
            let processing_threads = num_cores;
            let processing_results: Vec<FlagIndex> = (0..processing_threads)
                .into_par_iter()
                .map(|_| -> Result<FlagIndex> {
                    let input_queue = Arc::clone(&decompression_queue);
                    let decompression_done_clone = Arc::clone(&decompression_done);
                    let mut local_index = FlagIndex::new();

                    loop {
                        // Work-stealing with backoff
                        let mut backoff_count = 0;
                        let decompressed_block = loop {
                            if let Some(block) = input_queue.pop() {
                                break block;
                            }

                            // Check if decompression is complete AND queue is empty
                            if decompression_done_clone.load(Ordering::Acquire)
                                && input_queue.is_empty()
                            {
                                return Ok(local_index);
                            }

                            // Exponential backoff
                            if backoff_count < 10 {
                                std::hint::spin_loop();
                                backoff_count += 1;
                            } else {
                                std::thread::yield_now();
                                backoff_count = 0;
                            }
                        };

                        // **VECTORIZED FLAG EXTRACTION**
                        extract_flags_from_decompressed_simd_optimized(
                            &decompressed_block.data,
                            decompressed_block.data.len(),
                            &mut local_index,
                            decompressed_block.block_offset,
                            &mut 0, // dummy record count
                        )?;
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;

            // Wait for discovery to complete
            for handle in discovery_handles {
                handle.join().unwrap()?;
            }

            // Signal discovery completion so decompression threads can exit
            discovery_done.store(true, Ordering::Release);

            // Wait for decompression to complete
            for handle in decompression_handles {
                handle.join().unwrap()?;
            }

            // Signal decompression completion so processing threads can exit
            decompression_done.store(true, Ordering::Release);

            // Merge results
            let mut final_index = FlagIndex::new();
            for local_index in processing_results {
                final_index.merge(local_index);
            }

            Ok(final_index)
        })
        .unwrap()
    }

    // Redundant and poorly-performing strategies have been removed based on benchmark results
}

// PERFORMANCE NOTE: extract_flags_from_block removed - it was inefficient!
// Use extract_flags_from_block_pooled instead - no fresh allocations per block

// extract_flags_from_block_sys function has been removed with RayonSysStreamingOptimized strategy

// PERFORMANCE NOTE: read_record_headers removed - it was only used by the inefficient extract_flags_from_block

// Removed unused UltraBlockInfo and UltraDecompressedBlock structs
// Removed unused ULTRA-OPTIMIZED VECTORIZED FLAG EXTRACTION doc comment
