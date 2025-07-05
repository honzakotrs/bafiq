use crate::bgzf::BGZF_BLOCK_MAX_SIZE;
use crate::index::strategies::shared::{discover_blocks_streaming, extract_flags_from_block_pooled};
use crate::index::strategies::IndexingStrategy;
use crate::FlagIndex;
use anyhow::Result;
use crossbeam::thread;
use libdeflater::Decompressor;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Rayon-based streaming processing strategy (hybrid approach)
/// 
/// Combines the best of streaming and work-stealing:
/// - Phase 1: Discover blocks and stream them immediately to a work queue
/// - Phase 2: Rayon workers pull from queue using work-stealing behavior
/// - Benefits: Lower memory usage, better pipeline utilization, immediate processing
/// - Trade-offs: Slightly more complex synchronization than pure RayonOptimized
/// 
/// **Architecture:**
/// - Discovery thread: Streams blocks as they're found to a lock-free work queue
/// - Processing workers: Rayon workers that pull from queue using work-stealing
/// - Lock-free concurrent queue: Efficient work distribution without contention
/// - Thread-local buffers: Efficient decompression with thread-local storage
/// 
/// **Characteristics:**
/// - Memory usage: Low (streams blocks immediately, no storage of all blocks)
/// - Latency: Low (immediate processing as blocks are discovered)
/// - Throughput: High (work-stealing parallelism)
/// - Suitable for: Large files where memory usage and latency are important
pub struct RayonStreamingOptimizedStrategy;

impl IndexingStrategy for RayonStreamingOptimizedStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
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
                // Use shared streaming discovery function
                let block_count = discover_blocks_streaming(&data_producer, |block_info| {
                    queue_producer.push(block_info);
                    Ok(())
                })?;
                
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
} 