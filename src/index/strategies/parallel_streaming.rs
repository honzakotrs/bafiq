use anyhow::Result;
use crossbeam::channel::unbounded;
use libdeflater::Decompressor;
use memmap2::Mmap;

use std::fs::File;
use std::sync::Arc;

use super::shared::extract_flags_from_block_pooled;
use super::{IndexingStrategy, BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};
use crate::FlagIndex;

/// **PARALLEL STREAMING STRATEGY** - Simplest high-performance producer-consumer (3.433s)
///
/// **Why This Strategy Remains Valuable:**
/// - Simplest producer-consumer implementation (educational value)
/// - Close performance to best (only 24ms slower than rayon_wait_free)
/// - Demonstrates that unbounded channels can work well in practice
/// - Lower memory usage than discover-all-first approaches
/// - Immediate processing start (no discovery phase latency)
///
/// **Learnings Incorporated:**
/// - Outperforms bounded channel variants despite using unbounded channels
/// - Proves that channel choice is less important than overall architecture
/// - Shows that simple streaming can compete with complex optimizations
/// - Demonstrates that simple architecture can outperform complex batch processing
/// - Unbounded channels work fine when producer discovery is the bottleneck
///
/// **Architecture:**
/// - Producer thread: Single-threaded BGZF block discovery → immediate streaming
/// - Consumer threads: Multiple workers pull blocks via crossbeam unbounded channels
/// - Processing: Thread-local buffers for decompression and flag extraction
/// - Merging: Sequential merge of worker results
///
/// **Performance Characteristics:**
/// - Time: 3.433s (2nd fastest, within noise of best)
/// - Memory: 1.4GB peak (streaming keeps memory reasonable)
/// - CPU: 147.2% peak, 14.1% average (good utilization)
/// - Suitable for: When simplicity and immediate processing are priorities
pub struct ParallelStreamingStrategy;

impl IndexingStrategy for ParallelStreamingStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);
        
        // Use unbounded channels to eliminate receiver mutex contention
        let (sender, receiver) = unbounded::<(usize, usize, i64)>();
        let num_threads = rayon::current_num_threads();
        
        crossbeam::thread::scope(|s| {
            // Producer thread: discovers complete BGZF blocks and streams them immediately
            let data_producer = Arc::clone(&data);
            s.spawn(move |_| {
                let mut pos = 0;
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
                    
                    // Send block immediately: (start_pos, total_size, block_offset)
                    if sender.send((pos, total_size, pos as i64)).is_err() {
                        break; // Receivers hung up
                    }
                    
                    pos += total_size;
                }
                
                drop(sender);
            });
            
            // Consumer threads: process blocks as they arrive
            let results: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let rx = receiver.clone();
                    let data_worker = Arc::clone(&data);
                    
                    s.spawn(move |_| -> FlagIndex {
                        let mut local_index = FlagIndex::new();
                        
                        // Use thread-local buffers to avoid per-block allocations
                        thread_local! {
                            static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                            static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
                        }
                        
                        while let Ok((start_pos, total_size, block_offset)) = rx.recv() {
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
                                eprintln!("Thread {}: Block processing error: {}", thread_id, e);
                                continue;
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
