// Query engine for efficient BAM record retrieval using block-based indexes

use crate::index::format::{IndexAccessor, SerializableIndex};
use crate::{FLAG_MASK, FlagIndex, N_FLAGS};
use anyhow::Result;
use rust_htslib::bam::{self, Read, Writer};
use std::collections::HashSet;
use std::path::Path;

/// Query request specifying the flag criteria
#[derive(Debug, Clone)]
pub struct QueryRequest {
    /// Required flag bits (must all be set)
    pub required_flags: u16,
    /// Forbidden flag bits (must all be unset)
    pub forbidden_flags: u16,
}

impl QueryRequest {
    /// Create a new query request
    #[allow(dead_code)]
    pub fn new(required_flags: u16, forbidden_flags: u16) -> Self {
        Self {
            required_flags,
            forbidden_flags,
        }
    }

    /// Create a query for unmapped reads (flag 0x4 set)
    #[allow(dead_code)]
    pub fn unmapped() -> Self {
        Self::new(0x4, 0)
    }

    /// Create a query for mapped reads (flag 0x4 unset)
    #[allow(dead_code)]
    pub fn mapped() -> Self {
        Self::new(0, 0x4)
    }

    /// Create a query for paired reads (flag 0x1 set)
    #[allow(dead_code)]
    pub fn paired() -> Self {
        Self::new(0x1, 0)
    }

    /// Create a query for properly paired reads (flags 0x1 and 0x2 set)
    #[allow(dead_code)]
    pub fn properly_paired() -> Self {
        Self::new(0x1 | 0x2, 0)
    }

    /// Check if a flag value matches this query
    pub fn matches(&self, flags: u16) -> bool {
        let masked_flags = flags & FLAG_MASK;
        (masked_flags & self.required_flags) == self.required_flags
            && (masked_flags & self.forbidden_flags) == 0
    }

    /// Check if a bin index matches this query
    #[allow(dead_code)]
    pub fn matches_bin(&self, bin_idx: u16) -> bool {
        self.matches(bin_idx)
    }
}

/// Query result containing count and block information
#[derive(Debug)]
#[allow(dead_code)]
pub struct QueryResult {
    /// Total number of matching reads
    pub count: u64,
    /// Block IDs that contain matching reads (sorted)
    pub blocks: Vec<i64>,
    /// Number of blocks that need to be scanned
    pub blocks_to_scan: usize,
    /// Flag combinations that matched the query
    pub matching_flags: Vec<u16>,
}

impl QueryResult {
    /// Create a new empty query result
    #[allow(dead_code)]
    pub fn empty() -> Self {
        Self {
            count: 0,
            blocks: Vec::new(),
            blocks_to_scan: 0,
            matching_flags: Vec::new(),
        }
    }

    /// Print a summary of the query result
    #[allow(dead_code)]
    pub fn print_summary(&self) {
        println!("Query Results:");
        println!("   Total matching reads: {}", self.count);
        println!("   Blocks to scan: {}", self.blocks_to_scan);
        println!(
            "   Flag combinations matched: {}",
            self.matching_flags.len()
        );
        if !self.matching_flags.is_empty() {
            print!("   Flags: ");
            for (i, &flag) in self.matching_flags.iter().take(10).enumerate() {
                if i > 0 {
                    print!(", ");
                }
                print!("0x{:03x}", flag);
            }
            if self.matching_flags.len() > 10 {
                print!(" ... and {} more", self.matching_flags.len() - 10);
            }
            println!();
        }
    }
}

/// Unified query engine that works with any index type
#[allow(dead_code)]
pub struct QueryEngine;

impl QueryEngine {
    /// Execute a query against a serializable index
    #[allow(dead_code)]
    pub fn execute_query(index: &SerializableIndex, request: &QueryRequest) -> QueryResult {
        match index.get_index() {
            IndexAccessor::Uncompressed(flag_index) => {
                Self::execute_query_uncompressed(flag_index, request)
            }
            IndexAccessor::Compressed(compressed_index) => {
                Self::execute_query_compressed(compressed_index, request)
            }
        }
    }

    /// Execute a query against an uncompressed index
    #[allow(dead_code)]
    pub fn execute_query_uncompressed(index: &FlagIndex, request: &QueryRequest) -> QueryResult {
        let mut count = 0;
        let mut block_set = HashSet::new();
        let mut matching_flags = Vec::new();

        // Scan all bins to find matches
        for bin in 0..N_FLAGS {
            let bin_flag = bin as u16;
            if request.matches_bin(bin_flag) {
                if let Some(summaries) = index.bin_block_summaries(bin) {
                    if !summaries.is_empty() {
                        matching_flags.push(bin_flag);

                        // Add to count and collect blocks
                        for &(block_id, read_count) in summaries {
                            count += read_count;
                            block_set.insert(block_id);
                        }
                    }
                }
            }
        }

        // Sort blocks for efficient seeking
        let mut blocks: Vec<i64> = block_set.into_iter().collect();
        blocks.sort_unstable();

        QueryResult {
            count,
            blocks_to_scan: blocks.len(),
            blocks,
            matching_flags,
        }
    }

    /// Execute a query against a compressed index
    #[allow(dead_code)]
    pub fn execute_query_compressed(
        index: &crate::CompressedFlagIndex,
        request: &QueryRequest,
    ) -> QueryResult {
        // Use the compressed index's count method
        let count = index.count(request.required_flags, request.forbidden_flags);

        // Get used flags and filter for matches
        let used_flags = index.used_flags();
        let matching_flags: Vec<u16> = used_flags
            .into_iter()
            .filter(|&flag| request.matches_bin(flag))
            .collect();

        // Collect blocks from matching flags
        let mut block_set = HashSet::new();
        for &flag in &matching_flags {
            let block_summaries = index.get_bin_blocks(flag);
            for (block_id, _count) in block_summaries {
                block_set.insert(block_id);
            }
        }

        let mut blocks: Vec<i64> = block_set.into_iter().collect();
        blocks.sort_unstable();

        QueryResult {
            count,
            blocks_to_scan: blocks.len(),
            blocks,
            matching_flags,
        }
    }

    /// Count matching reads without building full result
    #[allow(dead_code)]
    pub fn count_reads(index: &SerializableIndex, request: &QueryRequest) -> u64 {
        match index.get_index() {
            IndexAccessor::Uncompressed(flag_index) => {
                flag_index.count(request.required_flags, request.forbidden_flags)
            }
            IndexAccessor::Compressed(compressed_index) => {
                compressed_index.count(request.required_flags, request.forbidden_flags)
            }
        }
    }

    /// Get block IDs for a query without full result
    #[allow(dead_code)]
    pub fn get_blocks(index: &SerializableIndex, request: &QueryRequest) -> Vec<i64> {
        let result = Self::execute_query(index, request);
        result.blocks
    }

    /// Extract matching reads and write them to a BAM writer
    #[allow(dead_code)]
    pub fn extract_reads<P: AsRef<Path>>(
        index: &SerializableIndex,
        request: &QueryRequest,
        bam_path: P,
        writer: &mut Writer,
    ) -> Result<u64> {
        let result = Self::execute_query(index, request);
        let mut extracted_count = 0;

        let mut reader = bam::Reader::from_path(bam_path)?;
        let mut record = bam::Record::new();

        for &block_id in &result.blocks {
            let offset = block_id << 16;
            reader.seek(offset)?;

            while let Some(res) = reader.read(&mut record) {
                res?;
                let cur_block_id = reader.tell() >> 16;
                if cur_block_id != block_id {
                    break; // Moved to next block
                }

                let flags = record.flags();
                if request.matches(flags) {
                    writer.write(&record)?;
                    extracted_count += 1;
                }
            }
        }

        Ok(extracted_count)
    }

    /// Analyze a query's efficiency metrics
    #[allow(dead_code)]
    pub fn analyze_query_efficiency(
        index: &SerializableIndex,
        request: &QueryRequest,
    ) -> QueryEfficiencyAnalysis {
        let result = Self::execute_query(index, request);
        let total_records = match index.get_index() {
            IndexAccessor::Uncompressed(flag_index) => flag_index.total_records(),
            IndexAccessor::Compressed(compressed_index) => {
                // Estimate total records from compressed index
                (compressed_index.compression_stats().original_size / 32) as u64
                // Rough estimate
            }
        };

        let selectivity = if total_records > 0 {
            result.count as f64 / total_records as f64
        } else {
            0.0
        };

        QueryEfficiencyAnalysis {
            matching_reads: result.count,
            total_reads: total_records,
            selectivity_ratio: selectivity,
            blocks_to_scan: result.blocks_to_scan,
            flag_combinations_matched: result.matching_flags.len(),
            estimated_io_operations: result.blocks_to_scan, // One seek per block
        }
    }
}

/// Analysis of query efficiency for optimization
#[derive(Debug)]
#[allow(dead_code)]
pub struct QueryEfficiencyAnalysis {
    pub matching_reads: u64,
    pub total_reads: u64,
    pub selectivity_ratio: f64, // 0.0 to 1.0
    pub blocks_to_scan: usize,
    pub flag_combinations_matched: usize,
    pub estimated_io_operations: usize,
}

impl QueryEfficiencyAnalysis {
    #[allow(dead_code)]
    pub fn print_analysis(&self) {
        println!("Query Efficiency Analysis:");
        println!("{}", "=".repeat(40));
        println!("   Matching reads: {}", self.matching_reads);
        println!("   Total reads: {}", self.total_reads);
        println!("   Selectivity: {:.2}%", self.selectivity_ratio * 100.0);
        println!("   Blocks to scan: {}", self.blocks_to_scan);
        println!("   Flag combinations: {}", self.flag_combinations_matched);
        println!("   Estimated I/O ops: {}", self.estimated_io_operations);

        // Efficiency recommendations
        if self.selectivity_ratio > 0.5 {
            println!("   High selectivity - consider scanning entire file");
        } else if self.blocks_to_scan > 1000 {
            println!("   Many blocks to scan - query may be slow");
        } else {
            println!("   Efficient query - good index utilization");
        }
    }
}
