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

/// Build index using chunk-based streaming strategy
///
/// BETTER PARALLELISM than ParallelStreaming due to crossbeam channels:
/// - Uses crossbeam::channel::bounded - no receiver mutex contention
/// - Batches 1000 blocks before sending (higher latency, better throughput)
/// - rayon::current_num_threads() threads with crossbeam::thread::scope
/// - Still allocates fresh 65KB per block (no buffer reuse)
pub struct ChunkStreamingStrategy;

impl IndexingStrategy for ChunkStreamingStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        // Use memory mapping like the working strategies to avoid chunk boundary issues
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);

        const BLOCKS_PER_BATCH: usize = 1000; // Send blocks in batches for efficiency
        let (sender, receiver): (
            Sender<Vec<(usize, usize, i64)>>,
            Receiver<Vec<(usize, usize, i64)>>,
        ) = bounded(64);
        let num_threads = current_num_threads();

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
}
