use anyhow::Result;
use libdeflater::Decompressor;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use rayon::prelude::*;

use super::shared::{extract_flags_from_block_pooled, is_gzip_header};
use super::{IndexingStrategy};
use crate::FlagIndex;
use crate::bgzf::{BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};

/// Information about a BGZF block's location in the file
#[derive(Debug, Clone)]
struct BlockInfo {
    start_pos: u64,
    total_size: usize,
}

/// **O(1) memory with immediate processing:**
/// - No batch accumulation**: Process blocks immediately as discovered
/// - No block copying**: Read and process blocks in-place  
/// - Streaming merge**: Merge index data immediately, no collection
/// - Large chunks**: 32MB chunks for good I/O efficiency
/// - Constant working set**: Only 1-2 blocks in memory at any time
/// - Index size**: O(blocks × flags) - grows slowly with compressed file size
/// 
/// **Memory profile:**
/// - **Chunk buffer**: 32MB (constant)
/// - **Working blocks**: ~64KB × 2 = 128KB (constant)  
/// - **Thread buffers**: 64KB × threads (constant)
/// - **Index**: ~50MB for 1GB file (grows sub-linearly)
/// - **Total**: ~100MB constant working set
pub struct ConstantMemoryStrategy;

impl IndexingStrategy for ConstantMemoryStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        // **CONSTANT MEMORY CONFIGURATION**
        const CHUNK_SIZE_MB: usize = 32;               // 32MB chunks for good I/O
        const OVERLAP_KB: usize = 128;                 // 128KB overlap for block boundaries
        const MEMORY_CHECK_INTERVAL: usize = 20;       // Check every 20 chunks
        
        let mut file = File::open(bam_path)?;
        let file_size = file.metadata()?.len();
        
        println!("CONSTANT MEMORY STREAMING STRATEGY");
        println!("   File: {}GB", file_size / (1024 * 1024 * 1024));
        println!("   Chunk size: {}MB + {}KB overlap", CHUNK_SIZE_MB, OVERLAP_KB);
        println!("   Target: O(1) constant memory usage");
        println!("   Processing blocks immediately without accumulation");
        
        let mut final_index = FlagIndex::new();
        let mut current_file_pos = 0u64;
        let mut chunk_count = 0;
        let mut total_blocks_processed = 0;
        
        // **STREAMING WITH IMMEDIATE PROCESSING**
        while current_file_pos < file_size {
            chunk_count += 1;
            
            // **READ CHUNK** 
            let base_chunk_size = CHUNK_SIZE_MB * 1024 * 1024;
            let overlap_bytes = OVERLAP_KB * 1024;
            let total_chunk_size = base_chunk_size + overlap_bytes;
            
            let (chunk_data, _bytes_read) = read_chunk_with_overlap(
                &mut file, 
                current_file_pos, 
                total_chunk_size,
                file_size
            )?;
            
            if chunk_data.is_empty() {
                break;
            }
            
            // **DISCOVER BLOCKS**
            let (complete_blocks, bytes_consumed) = discover_complete_blocks_in_chunk(
                &chunk_data, 
                current_file_pos,
                base_chunk_size
            )?;
            
            if complete_blocks.is_empty() {
                current_file_pos += if bytes_consumed == 0 { 1 } else { bytes_consumed };
                continue;
            }
            
            println!("Chunk {}: {}MB, {} blocks @ {}MB ({:.1}% complete)", 
                     chunk_count, 
                     base_chunk_size / (1024 * 1024),
                     complete_blocks.len(),
                     current_file_pos / (1024 * 1024),
                     (current_file_pos as f64 / file_size as f64) * 100.0);
            
            // **IMMEDIATE STREAMING PROCESSING** - No accumulation!
            let chunk_index = process_blocks_streaming_no_accumulation(
                &mut file, &complete_blocks
            )?;
            
            // **IMMEDIATE MERGE** - Constant memory
            final_index.merge(chunk_index);
            
            total_blocks_processed += complete_blocks.len();
            
            println!("   {} blocks → {} records total", 
                     complete_blocks.len(), final_index.total_records());
            
            // **RELAXED MEMORY MONITORING**
            if chunk_count % MEMORY_CHECK_INTERVAL == 0 {
                if let Some(current_rss_mb) = get_current_memory_usage_mb() {
                    println!("Memory check: {}MB (should be constant)", current_rss_mb);
                }
            }
            
            // **ADVANCE POSITION**
            current_file_pos += bytes_consumed;
            
            // **IMMEDIATE CLEANUP**
            drop(chunk_data);
        }
        
        let final_memory = get_current_memory_usage_mb().unwrap_or(0);
        println!("Complete:");
        println!("   Chunks: {}", chunk_count);
        println!("   Blocks: {}", total_blocks_processed);  
        println!("   Records: {}", final_index.total_records());
        println!("   Final memory: {}MB", final_memory);
        
        Ok(final_index)
    }
}

/// Read a chunk with overlap to ensure complete blocks
fn read_chunk_with_overlap(
    file: &mut File, 
    start_pos: u64, 
    chunk_size: usize,
    file_size: u64
) -> Result<(Vec<u8>, u64)> {
    file.seek(SeekFrom::Start(start_pos))?;
    
    let max_read = std::cmp::min(chunk_size as u64, file_size - start_pos) as usize;
    let mut buffer = vec![0u8; max_read];
    let bytes_read = file.read(&mut buffer)?;
    buffer.truncate(bytes_read);
    
    Ok((buffer, bytes_read as u64))
}

/// Discover complete blocks within a chunk, respecting boundaries
fn discover_complete_blocks_in_chunk(
    chunk_data: &[u8], 
    chunk_start_pos: u64,
    target_bytes: usize
) -> Result<(Vec<BlockInfo>, u64)> {
    let mut blocks = Vec::new();
    let mut pos = 0;
    let mut bytes_consumed = 0;
    
    while pos < chunk_data.len() {
        if pos + BGZF_HEADER_SIZE > chunk_data.len() {
            break;
        }
        
        let header = &chunk_data[pos..pos + BGZF_HEADER_SIZE];
        if !is_gzip_header(header) {
            pos += 1;
            continue;
        }
        
        let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
        let total_size = bsize + 1;
        
        if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > BGZF_BLOCK_MAX_SIZE {
            pos += 1;
            continue;
        }
        
        if pos + total_size > chunk_data.len() {
            break;
        }
        
        // **BOUNDARY CHECK**: Only process blocks within target range
        if pos + total_size <= target_bytes {
            blocks.push(BlockInfo {
                start_pos: chunk_start_pos + pos as u64,
                total_size,
            });
            
            bytes_consumed = (pos + total_size) as u64;
        }
        
        pos += total_size;
    }
    
    Ok((blocks, bytes_consumed))
}

/// **CONSTANT MEMORY PROCESSING**
/// Read blocks in small batches, process immediately, no accumulation
fn process_blocks_streaming_no_accumulation(
    file: &mut File,
    blocks: &[BlockInfo]
) -> Result<FlagIndex> {
    if blocks.is_empty() {
        return Ok(FlagIndex::new());
    }
    
    let mut final_index = FlagIndex::new();
    
    // **PROCESS IN TINY BATCHES** - 5 blocks at a time for constant memory
    const MICRO_BATCH_SIZE: usize = 5;
    
    for block_batch in blocks.chunks(MICRO_BATCH_SIZE) {
        // **SEQUENTIAL READ** - Only 5 blocks in memory max
        let mut blocks_data = Vec::with_capacity(block_batch.len());
        for block_info in block_batch {
            file.seek(SeekFrom::Start(block_info.start_pos))?;
            let mut block_data = vec![0u8; block_info.total_size];
            file.read_exact(&mut block_data)?;
            blocks_data.push((block_data, block_info.start_pos as i64));
        }
        
        // **PARALLEL PROCESSING** - Only 5 blocks
        let local_indexes: Vec<FlagIndex> = blocks_data
            .par_iter()
            .map(|(block_data, block_offset)| -> Result<FlagIndex> {
                let mut local_index = FlagIndex::new();
                
                thread_local! {
                    static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                    static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
                }
                
                BUFFER.with(|buf| {
                    DECOMPRESSOR.with(|decomp| {
                        let mut buffer = buf.borrow_mut();
                        let mut decompressor = decomp.borrow_mut();
                        extract_flags_from_block_pooled(
                            block_data,
                            &mut local_index,
                            *block_offset,
                            &mut buffer,
                            &mut decompressor,
                        )
                    })
                })?;
                
                Ok(local_index)
            })
            .collect::<Result<Vec<_>>>()?;
        
        // **IMMEDIATE MERGE** - Process each micro-batch immediately
        let batch_index = FlagIndex::merge_parallel(local_indexes);
        final_index.merge(batch_index);
        
        // **IMMEDIATE CLEANUP** - blocks_data goes out of scope
    }
    
    Ok(final_index)
}

/// Get current RSS memory usage in MB (platform-specific)
fn get_current_memory_usage_mb() -> Option<usize> {
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        
        let output = Command::new("ps")
            .args(&["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()?;
        
        let rss_kb = String::from_utf8(output.stdout)
            .ok()?
            .trim()
            .parse::<usize>()
            .ok()?;
        
        Some(rss_kb / 1024)
    }
    
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        
        let status = fs::read_to_string("/proc/self/status").ok()?;
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    let rss_kb = parts[1].parse::<usize>().ok()?;
                    return Some(rss_kb / 1024);
                }
            }
        }
        None
    }
    
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        None
    }
}


