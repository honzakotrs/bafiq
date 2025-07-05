use anyhow::Result;
use crossbeam::channel::{bounded, Receiver, Sender};
use crossbeam::thread;
use libdeflater::Decompressor;
use memmap2::Mmap;
use rayon::current_num_threads;
use std::fs::File;
use std::sync::Arc;

use super::shared::extract_flags_from_block_pooled;
use super::{IndexingStrategy, BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};
use crate::FlagIndex;

/// Build index using optimized parallel chunk streaming (new optimal strategy)
///
/// BEST PARALLELISM - combines immediate streaming with lock-free channels:
/// - Uses crossbeam::channel::bounded for true parallel receivers (no mutex)
/// - Small batch size (16 blocks) for low latency with good throughput
/// - Thread-local buffer pools to eliminate per-block allocations
/// - rayon::current_num_threads() for consistency with other parallel code
/// - Immediate processing start (no 1000-block batching delays)
pub struct ParallelChunkStreamingStrategy;

impl IndexingStrategy for ParallelChunkStreamingStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);

        const BLOCKS_PER_BATCH: usize = 16; // Small batches for low latency
        let (sender, receiver): (
            Sender<Vec<(usize, usize, i64)>>,
            Receiver<Vec<(usize, usize, i64)>>,
        ) = bounded(32); // Reasonable buffer without excess memory
        let num_threads = current_num_threads();

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
}
