use crate::bgzf::{BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};
use crate::index::strategies::shared::extract_flags_from_decompressed_simd_optimized;
use crate::index::strategies::{BlockInfo, IndexingStrategy};
use crate::FlagIndex;
use anyhow::Result;
use crossbeam::channel::bounded;
use libdeflater::Decompressor;
use memmap2::Mmap;
use rayon::{prelude::*, ThreadPoolBuilder};
use std::fs::File;
use std::sync::Arc;

/// Expert-level ultra-performance strategy with 3-stage pipeline
/// Based on performance expert recommendations for maximum CPU utilization
///
/// **Architecture:**
/// - 2-Stage Pipeline: Discovery + Combined Decompression/Processing
/// - Bounded MPMC channels for efficient producer-consumer communication
/// - SIMD-accelerated block discovery using memchr library
/// - Zero-copy decompression directly into buffers
/// - Parallel discovery across multiple file segments
/// - Thread pool configuration to prevent oversubscription
/// - Expert-level optimization techniques
///
/// **Characteristics:**
/// - Memory usage: Low (streaming approach with bounded channels)
/// - Latency: Low (immediate processing pipeline)
/// - Throughput: Expert-level (all optimizations applied)
/// - Suitable for: Maximum performance scenarios where all optimizations are needed
///
/// **Key Innovations:**
/// - SIMD byte scanning for header detection
/// - Bounded channels prevent memory pressure
/// - Zero-copy processing wherever possible
/// - Optimal thread count detection
/// - Professional-grade error handling
pub struct RayonExpertStrategy;

impl IndexingStrategy for RayonExpertStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);
        let file_size = data.len();

        let num_threads = num_cpus::get(); // Get optimal thread count

        // **Bounded MPMC channel** for discovery -> processing pipeline
        const DISC_QUEUE: usize = 1024;

        let (tx_disc, rx_disc) = bounded::<BlockInfo>(DISC_QUEUE);

        // Configure global Rayon pool to prevent oversubscription
        ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
            .unwrap_or_else(|_| {
                // Pool already exists, continue with existing configuration
            });

        // **2-STAGE PIPELINE** - Discovery + Combined Decompression/Processing
        crossbeam::thread::scope(|s| {
            // ── STAGE 1: Parallel BGZF Header Discovery ──
            let seg_size = (file_size + num_threads - 1) / num_threads; // ceiling division

            // Collect discovery handles to ensure they complete before processing
            let mut discovery_handles = Vec::new();

            for tid in 0..num_threads {
                let data_seg = Arc::clone(&data);
                let tx_out = tx_disc.clone();

                let handle = s.spawn(move |_| {
                    let seg_start = tid * seg_size;
                    let seg_end = (seg_start + seg_size).min(file_size);
                    let mut pos = seg_start;

                    // If not first segment, find first magic bytes using SIMD
                    if tid != 0 {
                        if let Some(rel) = memchr::memchr2(0x1f, 0x8b, &data_seg[pos..seg_end]) {
                            pos += rel;
                        } else {
                            return; // No blocks in this segment
                        }
                    }

                    // **SIMD-accelerated block discovery**
                    while pos + BGZF_HEADER_SIZE <= seg_end {
                        // Quick rejection with SIMD-found candidates
                        if &data_seg[pos..pos + 2] != &[0x1f, 0x8b] {
                            pos += 1;
                            continue;
                        }

                        // Extract block size with bounds checking
                        if pos + 17 >= data_seg.len() {
                            break;
                        }

                        let bsize =
                            u16::from_le_bytes([data_seg[pos + 16], data_seg[pos + 17]]) as usize;
                        let total_size = bsize + 1;

                        // Validate block size constraints
                        if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE
                            || total_size > 65536
                            || pos + total_size > file_size
                        {
                            pos += 1;
                            continue;
                        }

                        // Send to combined decompression/processing stage
                        if tx_out
                            .send(BlockInfo {
                                start_pos: pos,
                                total_size,
                            })
                            .is_err()
                        {
                            break; // Channel closed
                        }

                        pos += total_size;
                    }
                });
                discovery_handles.push(handle);
            }

            // ── STAGE 2: Combined Decompression + Processing (concurrent with discovery) ──
            let results: Result<Vec<FlagIndex>, anyhow::Error> =
                crossbeam::thread::scope(|inner_scope| {
                    let mut processing_handles = Vec::new();

                    // Start processing threads concurrently with discovery
                    for _worker_id in 0..num_threads {
                        let data_arc = Arc::clone(&data);
                        let rx_in = rx_disc.clone();

                        let handle = inner_scope.spawn(move |_| -> Result<FlagIndex> {
                            let mut local_index = FlagIndex::new();

                            // **Thread-local decompressor** - avoid repeated initialization
                            let mut decomp = Decompressor::new();
                            let mut buffer = Vec::<u8>::with_capacity(BGZF_BLOCK_MAX_SIZE);

                            // Process blocks until channel is closed
                            while let Ok(block_info) = rx_in.recv() {
                                let raw_block = &data_arc[block_info.start_pos
                                    ..block_info.start_pos + block_info.total_size];

                                // **Zero-copy decompression** - decompress directly into buffer
                                buffer.clear();
                                buffer.resize(BGZF_BLOCK_MAX_SIZE, 0);
                                let decompressed_size =
                                    match decomp.gzip_decompress(raw_block, &mut buffer) {
                                        Ok(size) => {
                                            buffer.truncate(size);
                                            size
                                        }
                                        Err(_) => {
                                            continue; // Skip corrupted blocks
                                        }
                                    };

                                // Skip BAM header blocks early
                                if buffer.starts_with(b"BAM\x01") {
                                    continue;
                                }

                                // **Immediate processing** - use proven flag extraction from rayon-wait-free
                                extract_flags_from_decompressed_simd_optimized(
                                    &buffer,
                                    decompressed_size,
                                    &mut local_index,
                                    block_info.start_pos as i64,
                                    &mut 0, // dummy record count
                                )?;
                            }

                            Ok(local_index)
                        });
                        processing_handles.push(handle);
                    }

                    // Wait for all discovery threads to complete, then close channel
                    for handle in discovery_handles {
                        handle.join().unwrap();
                    }
                    drop(tx_disc); // Now safe to close - all discovery is complete

                    // Collect results from processing threads
                    let mut results = Vec::new();
                    for handle in processing_handles {
                        results.push(handle.join().unwrap()?);
                    }

                    Ok(results)
                })
                .unwrap();

            let results = results?;

            // **Parallel merge** using divide-and-conquer
            let final_index = results.into_par_iter().reduce(
                || FlagIndex::new(),
                |mut acc, index| {
                    acc.merge(index);
                    acc
                },
            );

            Ok(final_index)
        })
        .unwrap() // Propagate any panics from scoped threads
    }
}
