use anyhow::Result;
use libdeflater::Decompressor;
use memmap2::Mmap;
use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, Mutex};


use super::{IndexingStrategy, BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};
use crate::FlagIndex;

/// **ZERO-MERGE STRATEGY** - Build index directly with shared HashMaps
/// 
/// **BREAKTHROUGH APPROACH FOR LARGE FILES:**
/// - Completely eliminates the O(n²) merge bottleneck
/// - Uses shared concurrent HashMap instead of per-thread local indexes
/// - No merging phase needed - results are accumulated directly
/// - Scales linearly with file size and thread count
/// 
/// **Performance Characteristics:**
/// - **Merge complexity**: O(1) - no merging needed!
/// - **Memory efficiency**: Single shared index structure
/// - **Scalability**: Linear with file size and cores
/// - **Cache efficiency**: Single index reduces memory pressure
/// 
/// **Target Use Case:**
/// - Large BAM files (>1GB) where merge time dominates
/// - High core count systems where merge becomes the bottleneck
/// - Production systems that need predictable linear scaling
pub struct ZeroMergeStrategy;

impl IndexingStrategy for ZeroMergeStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);
        
        // **SHARED CONCURRENT INDEX STRUCTURE**
        // All threads contribute directly to this shared HashMap
        // Avoids the need for merging entirely
        let shared_index_data = Arc::new(Mutex::new(HashMap::<u16, HashMap<i64, u64>>::new()));
        let shared_total_records = Arc::new(Mutex::new(0u64));
        
        // Use rayon for work-stealing parallel processing
        let blocks = super::shared::discover_blocks_fast(&data)?;
        
        // **PARALLEL PROCESSING WITH DIRECT ACCUMULATION**
        use rayon::prelude::*;
        
        let results: Vec<Result<()>> = blocks
            .par_iter()
            .map(|block_info| -> Result<()> {
                // Process block with thread-local buffers
                let block = &data[block_info.start_pos..block_info.start_pos + block_info.total_size];
                let block_offset = block_info.start_pos as i64;
                
                // Thread-local processing
                thread_local! {
                    static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                    static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
                }
                
                // Build local results first to minimize lock contention
                let mut local_flags: HashMap<u16, u64> = HashMap::new();
                let mut local_total_records = 0u64;
                
                BUFFER.with(|buf| {
                    DECOMPRESSOR.with(|decomp| {
                        let mut buffer = buf.borrow_mut();
                        let mut decompressor = decomp.borrow_mut();
                        
                        // Decompress block
                        let decompressed_size = decompressor
                            .deflate_decompress(
                                &block[BGZF_HEADER_SIZE..block_info.total_size - BGZF_FOOTER_SIZE],
                                &mut buffer,
                            )
                            .map_err(|e| anyhow::anyhow!("Decompression failed: {:?}", e))?;
                        
                        // Extract flags from decompressed data
                        if decompressed_size > 0 {
                            let mut pos = 0;
                            
                            // Skip BAM header blocks
                            if decompressed_size >= 4 && &buffer[0..4] == b"BAM\x01" {
                                return Ok::<(), anyhow::Error>(());
                            }
                            
                            // Process BAM records
                            while pos < decompressed_size {
                                if pos + 4 > decompressed_size {
                                    break;
                                }
                                
                                // Read record length
                                let record_len = u32::from_le_bytes([
                                    buffer[pos],
                                    buffer[pos + 1],
                                    buffer[pos + 2],
                                    buffer[pos + 3],
                                ]) as usize;
                                
                                if pos + 4 + record_len > decompressed_size {
                                    break;
                                }
                                
                                if record_len >= 20 {
                                    // Extract flags (bytes 18-19 of record)
                                    let flags = u16::from_le_bytes([
                                        buffer[pos + 4 + 18],
                                        buffer[pos + 4 + 19],
                                    ]);
                                    
                                    // Accumulate locally
                                    *local_flags.entry(flags).or_insert(0) += 1;
                                    local_total_records += 1;
                                }
                                
                                pos += 4 + record_len;
                            }
                        }
                        
                        Ok::<(), anyhow::Error>(())
                    })
                })?;
                
                // **CRITICAL SECTION: Minimal lock contention**
                // Batch update shared index with local results
                if !local_flags.is_empty() {
                    let mut shared_data = shared_index_data.lock().unwrap();
                    for (flags, count) in local_flags {
                        shared_data
                            .entry(flags)
                            .or_insert_with(HashMap::new)
                            .entry(block_offset)
                            .and_modify(|existing| *existing += count)
                            .or_insert(count);
                    }
                }
                
                if local_total_records > 0 {
                    let mut total = shared_total_records.lock().unwrap();
                    *total += local_total_records;
                }
                
                Ok(())
            })
            .collect();
        
        // Check for errors
        for result in results {
            result?;
        }
        
        // **FINAL STEP: Convert HashMap to FlagIndex**
        let shared_data = shared_index_data.lock().unwrap();
        let total_records = *shared_total_records.lock().unwrap();
        
        let final_index = FlagIndex::from_hashmap_data(shared_data.clone(), total_records);
        
        Ok(final_index)
    }
}

 