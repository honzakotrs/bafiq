use anyhow::{anyhow, Result};
use crossbeam::channel::unbounded;
use libdeflater::Decompressor;
use memmap2::Mmap;
use rayon::current_num_threads;
use std::fs::File;
use std::sync::Arc;
use std::thread as std_thread;

use super::shared::extract_flags_from_block_pooled;
use super::{IndexingStrategy, BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};
use crate::FlagIndex;

/// Build index using streaming parallel processing (legacy strategy with bottleneck)
///
/// PERFORMANCE WARNING: This strategy has a mutex contention bottleneck:
/// - One producer thread discovers BGZF blocks and sends via std::mpsc::channel
/// - Multiple worker threads compete for Arc<Mutex<Receiver>> - SERIALIZATION POINT
/// - Fresh 65KB allocation per block (no buffer reuse)
/// - Scaling limited by receiver mutex contention, not CPU cores
pub struct ParallelStreamingStrategy;

impl IndexingStrategy for ParallelStreamingStrategy {
    fn name(&self) -> &'static str {
        "parallel-streaming"
    }

    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap); // Share mmap safely across threads

        let num_threads = current_num_threads();

        // PERFORMANCE FIX: Use unbounded crossbeam channel - eliminates mutex contention!
        let (sender, receiver) = unbounded::<(usize, usize)>(); // (start_pos, total_size)

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
        // PERFORMANCE FIX: No more shared mutex! Each worker gets its own receiver clone

        for thread_id in 0..num_threads {
            let receiver_clone = receiver.clone();
            let data_worker = Arc::clone(&data);

            let handle = std_thread::spawn(move || -> Result<FlagIndex> {
                let mut local_index = FlagIndex::new();
                let mut _processed_count = 0;

                loop {
                    // PERFORMANCE FIX: Direct channel access - NO MUTEX CONTENTION!
                    match receiver_clone.recv() {
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

        // PERFORMANCE FIX: No need to drop receiver manually - crossbeam handles it automatically

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
}
