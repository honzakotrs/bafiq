use crate::FlagIndex;
use crate::compression::{DeltaDecoder, DeltaEncoder, SparseStorage};
use anyhow::Result;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Compressed flag index:
/// - sparsity optimization (store only used flag combinations)
/// - delta compression on block ID sequences  
/// - compatibility with uncompressed queries
#[derive(Debug, Serialize, Deserialize)]
pub struct CompressedFlagIndex {
    /// Sparse storage for flag combinations - only stores used flags
    sparse_storage: SparseStorage<CompressedBinData>,

    /// Compression statistics for analysis
    stats: CompressionStats,
}

/// Compressed storage for a single flag combination's data
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompressedBinData {
    /// Delta-compressed block IDs
    pub delta_compressed_blocks: Vec<u8>,

    /// Read counts for each block (parallel to blocks)
    pub read_counts: Vec<u64>,

    /// Original number of blocks for validation
    pub original_block_count: usize,
}

/// Statistics about compression efficiency
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStats {
    pub original_size: usize,
    pub compressed_size: usize,
    pub compression_ratio: f64,
    pub sparsity_savings: usize,
    pub delta_savings: usize,
    pub total_flags_used: usize,
}

impl CompressionStats {
    pub fn compression_percentage(&self) -> f64 {
        (1.0 - (self.compressed_size as f64 / self.original_size as f64)) * 100.0
    }

    pub fn print_analysis(&self) {
        println!("COMPRESSION ANALYSIS RESULTS:");
        println!("{}", "=".repeat(50));
        println!(
            "Original size: {} bytes ({:.1} MB)",
            self.original_size,
            self.original_size as f64 / 1_048_576.0
        );
        println!(
            "Compressed size: {} bytes ({:.1} MB)",
            self.compressed_size,
            self.compressed_size as f64 / 1_048_576.0
        );
        println!("Compression ratio: {:.2}x", self.compression_ratio);
        println!("Space saved: {:.1}%", self.compression_percentage());
        println!();
        println!("Breakdown by technique:");

        // Calculate individual technique contributions as percentages of total technique savings
        let total_technique_savings = self.sparsity_savings + self.delta_savings;

        if total_technique_savings > 0 {
            println!(
                "   - Sparse storage: {} bytes saved ({:.1}%)",
                self.sparsity_savings,
                self.sparsity_savings as f64 / total_technique_savings as f64 * 100.0
            );
            println!(
                "   - Delta encoding: {} bytes saved ({:.1}%)",
                self.delta_savings,
                self.delta_savings as f64 / total_technique_savings as f64 * 100.0
            );
            println!();
            println!(
                "Note: Individual technique savings total {} bytes",
                total_technique_savings
            );
            println!("Actual file size reduction may differ due to serialization overhead");
        } else {
            println!("   - No compression technique savings calculated");
        }

        println!();
        println!(
            "Flag usage: {}/4096 ({:.1}% sparse)",
            self.total_flags_used,
            (4096 - self.total_flags_used) as f64 / 4096.0 * 100.0
        );
    }
}

impl CompressedFlagIndex {
    /// Create a compressed index from a regular FlagIndex using the new compression modules
    pub fn from_uncompressed(index: &FlagIndex) -> Result<Self> {
        println!("Compressing FlagIndex using sparse + delta strategy...");

        // Step 1: Create sparse storage for used flag combinations
        let mut sparse_storage = SparseStorage::from_flag_index(index);

        // Step 2: Collect bin data for compression
        let mut bin_data_map = HashMap::new();

        // Iterate over actual bins with data instead of all possible flag values
        for (_bin_idx, bin) in index.bins().iter().enumerate() {
            if !bin.blocks.is_empty() {
                let blocks: Vec<i64> = bin.blocks.iter().map(|(block_id, _)| *block_id).collect();
                let counts: Vec<u64> = bin.blocks.iter().map(|(_, count)| *count).collect();

                // Use the actual flag value from the bin, not the bin index
                bin_data_map.insert(bin.bin, (blocks, counts));
            }
        }

        // Step 3: Compress each bin's data using
        sparse_storage.ensure_data_capacity();

        let total_bins = bin_data_map.len();
        println!("   Parallelizing compression of {} bins...", total_bins);

        // Progress counter for compression
        let progress_counter = AtomicUsize::new(0);

        // Process all bins in parallel
        let compression_results: Vec<
            Result<(u16, CompressedBinData, usize, usize, usize), anyhow::Error>,
        > = bin_data_map
            .par_iter()
            .map(|(&flag, (blocks, counts))| {
                let original_size = blocks.len() * 8 + counts.len() * 8;

                // Delta compress the block IDs
                let mut delta_encoder = DeltaEncoder::new();
                delta_encoder.encode(blocks)?;
                let delta_bytes = delta_encoder.to_bytes();

                let compressed_bin = CompressedBinData {
                    delta_compressed_blocks: delta_bytes,
                    read_counts: counts.clone(),
                    original_block_count: blocks.len(),
                };

                let compressed_size = Self::estimate_bin_size(&compressed_bin);

                // Track delta compression savings
                let delta_size = delta_encoder.compressed_size();
                let original_block_size = blocks.len() * 8;
                let delta_savings = if delta_size < original_block_size {
                    original_block_size - delta_size
                } else {
                    0
                };

                // Progress reporting
                let completed = progress_counter.fetch_add(1, Ordering::Relaxed) + 1;
                if completed % 10 == 0 || completed == total_bins {
                    println!(
                        "   Compressed {}/{} bins ({:.1}%)",
                        completed,
                        total_bins,
                        (completed as f64 / total_bins as f64) * 100.0
                    );
                }

                Ok((
                    flag,
                    compressed_bin,
                    original_size,
                    compressed_size,
                    delta_savings,
                ))
            })
            .collect();

        // Step 5: Aggregate results and handle errors
        let mut total_original_size = 0;
        let mut total_compressed_size = 0;
        let mut total_delta_savings = 0;

        for result in compression_results {
            let (flag, compressed_bin, original_size, compressed_size, delta_savings) = result?;

            total_original_size += original_size;
            total_compressed_size += compressed_size;
            total_delta_savings += delta_savings;

            sparse_storage.set_data(flag, compressed_bin)?;
        }

        // Step 6: Create compression statistics
        let sparse_stats = sparse_storage.stats();

        let compression_stats = CompressionStats {
            original_size: total_original_size,
            compressed_size: total_compressed_size,
            compression_ratio: if total_compressed_size > 0 {
                total_original_size as f64 / total_compressed_size as f64
            } else {
                1.0
            },
            sparsity_savings: (4096 - sparse_stats.used_flags) * 32, // Estimated bytes per unused bin
            delta_savings: total_delta_savings,
            total_flags_used: sparse_stats.used_flags,
        };

        println!("   Parallel compression complete!");
        println!(
            "   Compression ratio: {:.2}x ({:.1}% savings)",
            compression_stats.compression_ratio,
            compression_stats.compression_percentage()
        );

        let compressed = Self {
            sparse_storage,
            stats: compression_stats,
        };

        compressed.stats.print_analysis();

        Ok(compressed)
    }

    /// Estimate the size of compressed bin data
    fn estimate_bin_size(bin: &CompressedBinData) -> usize {
        bin.delta_compressed_blocks.len() + bin.read_counts.len() * 8 + 8 // overhead
    }

    /// Query the compressed index (interface matching FlagIndex)
    pub fn count(&self, required_bits: u16, forbidden_bits: u16) -> u64 {
        let mut total = 0;

        // Check each stored flag combination in sparse storage
        for flag in self.sparse_storage.used_flags() {
            let flag = *flag;
            if (flag & required_bits) == required_bits && (flag & forbidden_bits) == 0 {
                if let Some(bin_data) = self.sparse_storage.get_data(flag) {
                    total += bin_data.read_counts.iter().sum::<u64>();
                }
            }
        }

        total
    }

    /// Get compression statistics
    pub fn compression_stats(&self) -> &CompressionStats {
        &self.stats
    }

    /// Expand a compressed bin back to (block_id, count) pairs for compatibility
    pub fn get_bin_blocks(&self, flag: u16) -> Vec<(i64, u64)> {
        if let Some(bin_data) = self.sparse_storage.get_data(flag) {
            // Decompress the delta-encoded blocks
            if let Ok(decoder) = DeltaDecoder::from_bytes(&bin_data.delta_compressed_blocks) {
                if let Ok(blocks) = decoder.decode() {
                    return blocks
                        .into_iter()
                        .zip(bin_data.read_counts.iter())
                        .map(|(block, &count)| (block, count))
                        .collect();
                }
            }
        }

        Vec::new() // Flag not used or decompression failed
    }

    /// Get list of flags that have data (for sparse iteration)
    pub fn used_flags(&self) -> Vec<u16> {
        let mut flags: Vec<u16> = self.sparse_storage.used_flags().to_vec();
        flags.sort_unstable();
        flags
    }

    /// Get block IDs that contain reads matching the given criteria
    pub fn blocks_for(&self, required_bits: u16, forbidden_bits: u16) -> Vec<i64> {
        let mut block_ids = std::collections::HashSet::new();

        // Check each stored flag combination
        for flag in self.sparse_storage.used_flags() {
            let flag = *flag;
            if (flag & required_bits) == required_bits && (flag & forbidden_bits) == 0 {
                let blocks = self.get_bin_blocks(flag);
                for (block_id, _) in blocks {
                    block_ids.insert(block_id);
                }
            }
        }

        let mut result: Vec<i64> = block_ids.into_iter().collect();
        result.sort();
        result
    }

    /// Convert back to uncompressed FlagIndex for compatibility
    pub fn to_uncompressed(&self) -> FlagIndex {
        let mut index = FlagIndex::new();

        for &flag in self.used_flags().iter() {
            let blocks = self.get_bin_blocks(flag);
            for (block_id, count) in blocks {
                for _ in 0..count {
                    index.add_record_at_block(flag, block_id);
                }
            }
        }

        index
    }
}
