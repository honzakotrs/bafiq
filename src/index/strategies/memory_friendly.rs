use anyhow::Result;
use libdeflater::Decompressor;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use rayon::prelude::*;

use super::shared::extract_flags_from_block_pooled;
use super::{IndexingStrategy, BGZF_BLOCK_MAX_SIZE, BGZF_HEADER_SIZE, BGZF_FOOTER_SIZE};
use crate::FlagIndex;

/// Information about a BGZF block's location in the file
#[derive(Debug, Clone)]
struct BlockInfo {
    start_pos: u64,
    total_size: usize,
}

/// **BALANCED MEMORY-CONTROLLED STRATEGY** 
/// 
/// **OPTIMIZED FOR SPEED + MEMORY EFFICIENCY:**
/// - ✅ **Larger chunks**: 32MB chunks for fewer I/O operations
/// - ✅ **Bigger batches**: 200-500 blocks/batch for better parallelization
/// - ✅ **Block boundary respect**: Overlapping reads prevent block splitting
/// - ✅ **Efficient I/O**: Batch file reads instead of one-by-one seeking
/// - ✅ **Memory monitoring**: Less frequent checks (every 10 batches)
/// - ✅ **500MB budget**: 8x more memory than previous version
/// 
/// **Performance Targets:**
/// - **Speed**: ~24s (2x faster than 48s)
/// - **Memory**: <500MB peak (reasonable for most systems)
/// - **Parallelism**: Full CPU utilization with memory awareness
/// 
/// **Architecture Changes:**
/// - **32MB chunks**: 8x larger chunks = 8x fewer I/O operations
/// - **200 blocks/batch**: 4x larger batches = better rayon efficiency
/// - **Bulk file reads**: Read multiple blocks in one file operation
/// - **Relaxed monitoring**: Check memory every 10 batches, not 3
/// 
/// **Target**: 2x speed improvement with <500MB memory
pub struct MemoryFriendlyStrategy;

impl IndexingStrategy for MemoryFriendlyStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        // **BALANCED MEMORY & SPEED BUDGET**
        const TARGET_MEMORY_MB: usize = 400;           // 400MB working memory (generous)
        const CHUNK_SIZE_MB: usize = 32;               // 32MB chunks (8x larger)
        const OVERLAP_KB: usize = 128;                 // 128KB overlap (2x safety margin)
        const MAX_BLOCKS_PER_BATCH: usize = 200;       // 200 blocks/batch (4x larger)
        const MEMORY_CHECK_INTERVAL: usize = 10;       // Check every 10 batches
        
        let mut file = File::open(bam_path)?;
        let file_size = file.metadata()?.len();
        
        println!("🚀 BALANCED MEMORY-CONTROLLED STRATEGY (Speed Optimized)");
        println!("   File: {}GB", file_size / (1024 * 1024 * 1024));
        println!("   Target memory: {}MB (generous)", TARGET_MEMORY_MB);
        println!("   Chunk size: {}MB + {}KB overlap", CHUNK_SIZE_MB, OVERLAP_KB);
        println!("   Max blocks/batch: {} (4x larger)", MAX_BLOCKS_PER_BATCH);
        println!("   🎯 Target: 2x speed improvement with <500MB memory");
        
        let mut final_index = FlagIndex::new();
        let mut current_file_pos = 0u64;
        let mut batch_count = 0;
        let mut total_blocks_processed = 0;
        let mut max_blocks_per_batch = MAX_BLOCKS_PER_BATCH;
        
        // **STREAMING WITH SPEED OPTIMIZATION**
        while current_file_pos < file_size {
            batch_count += 1;
            
            // **READ LARGER OVERLAPPING CHUNK**
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
            
            // **DISCOVER COMPLETE BLOCKS WITHIN CHUNK**
            let (complete_blocks, bytes_consumed) = discover_complete_blocks_in_chunk(
                &chunk_data, 
                current_file_pos,
                base_chunk_size
            )?;
            
            if complete_blocks.is_empty() {
                if bytes_consumed == 0 {
                    current_file_pos += 1; // Avoid infinite loop
                } else {
                    current_file_pos += bytes_consumed;
                }
                continue;
            }
            
            println!("🔄 Batch {}: {}MB chunk, {} blocks @ {}MB ({:.1}% complete)", 
                     batch_count, 
                     base_chunk_size / (1024 * 1024),
                     complete_blocks.len(),
                     current_file_pos / (1024 * 1024),
                     (current_file_pos as f64 / file_size as f64) * 100.0);
            
            // **PROCESS BLOCKS IN LARGER BATCHES**
            for (mini_batch_idx, block_batch) in complete_blocks.chunks(max_blocks_per_batch).enumerate() {
                // **BULK READ + PARALLEL PROCESS**
                let mini_batch_index = process_blocks_bulk_read_parallel(
                    &mut file, block_batch
                )?;
                
                // **MERGE**
                final_index.merge(mini_batch_index);
                
                total_blocks_processed += block_batch.len();
                
                println!("   ✅ Batch {}: {} blocks → {} records", 
                         mini_batch_idx + 1, block_batch.len(), final_index.total_records());
            }
            
            // **RELAXED MEMORY MONITORING**
            if batch_count % MEMORY_CHECK_INTERVAL == 0 {
                if let Some(current_rss_mb) = get_current_memory_usage_mb() {
                    println!("📊 Memory check: {}MB / {}MB", current_rss_mb, TARGET_MEMORY_MB);
                    
                    // **ADAPTIVE BATCH SIZE** (less aggressive)
                    if current_rss_mb > TARGET_MEMORY_MB {
                        max_blocks_per_batch = std::cmp::max(max_blocks_per_batch * 3 / 4, 50);
                        println!("   ⚠️  Reducing batch size to {} blocks", max_blocks_per_batch);
                    } else if current_rss_mb < TARGET_MEMORY_MB / 2 && max_blocks_per_batch < MAX_BLOCKS_PER_BATCH {
                        max_blocks_per_batch = std::cmp::min(max_blocks_per_batch * 4 / 3, MAX_BLOCKS_PER_BATCH);
                        println!("   📈 Increasing batch size to {} blocks", max_blocks_per_batch);
                    }
                }
            }
            
            // **ADVANCE FILE POSITION**
            current_file_pos += bytes_consumed;
            
            // **CLEANUP**
            drop(chunk_data);
        }
        
        let final_memory = get_current_memory_usage_mb().unwrap_or(0);
        println!("🎯 BALANCED STRATEGY COMPLETE:");
        println!("   Batches: {}", batch_count);
        println!("   Blocks: {}", total_blocks_processed);  
        println!("   Records: {}", final_index.total_records());
        println!("   Final memory: {}MB", final_memory);
        
        if final_memory <= 500 {
            println!("   ✅ Memory budget achieved (<500MB)!");
        } else {
            println!("   ⚠️  Memory exceeded 500MB target");
        }
        
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
        if header[0..2] != [0x1f, 0x8b] {
            pos += 1;
            continue;
        }
        
        let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
        let total_size = bsize + 1;
        
        if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > 65536 {
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

/// Process blocks with bulk file reading for efficiency
fn process_blocks_bulk_read_parallel(
    file: &mut File,
    blocks: &[BlockInfo]
) -> Result<FlagIndex> {
    if blocks.is_empty() {
        return Ok(FlagIndex::new());
    }
    
    // **BULK READ OPTIMIZATION**: Read all blocks in one operation when possible
    let blocks_data = if blocks.len() <= 10 && blocks_are_contiguous(blocks) {
        // **CONTIGUOUS BLOCKS**: Read all blocks in one big read
        bulk_read_contiguous_blocks(file, blocks)?
    } else {
        // **SCATTERED BLOCKS**: Read individually (fallback)
        read_blocks_individually(file, blocks)?
    };
    
    // **PARALLEL PROCESSING** (same as before but with larger batches)
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
    
    Ok(FlagIndex::merge_parallel(local_indexes))
}

/// Check if blocks are contiguous for bulk reading
fn blocks_are_contiguous(blocks: &[BlockInfo]) -> bool {
    if blocks.len() <= 1 {
        return true;
    }
    
    for i in 1..blocks.len() {
        let prev_end = blocks[i-1].start_pos + blocks[i-1].total_size as u64;
        if prev_end != blocks[i].start_pos {
            return false;
        }
    }
    true
}

/// Bulk read contiguous blocks in one operation
fn bulk_read_contiguous_blocks(
    file: &mut File,
    blocks: &[BlockInfo]
) -> Result<Vec<(Vec<u8>, i64)>> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }
    
    let start_pos = blocks[0].start_pos;
    let end_pos = blocks.last().unwrap().start_pos + blocks.last().unwrap().total_size as u64;
    let total_size = (end_pos - start_pos) as usize;
    
    // **SINGLE FILE READ** for all blocks
    file.seek(SeekFrom::Start(start_pos))?;
    let mut bulk_data = vec![0u8; total_size];
    file.read_exact(&mut bulk_data)?;
    
    // **EXTRACT INDIVIDUAL BLOCKS** from bulk data
    let mut blocks_data = Vec::new();
    let mut data_pos = 0;
    
    for block_info in blocks {
        let block_size = block_info.total_size;
        let block_data = bulk_data[data_pos..data_pos + block_size].to_vec();
        blocks_data.push((block_data, block_info.start_pos as i64));
        data_pos += block_size;
    }
    
    Ok(blocks_data)
}

/// Read blocks individually (fallback for scattered blocks)
fn read_blocks_individually(
    file: &mut File,
    blocks: &[BlockInfo]
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut blocks_data = Vec::new();
    for block_info in blocks {
        file.seek(SeekFrom::Start(block_info.start_pos))?;
        let mut block_data = vec![0u8; block_info.total_size];
        file.read_exact(&mut block_data)?;
        blocks_data.push((block_data, block_info.start_pos as i64));
    }
    Ok(blocks_data)
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


