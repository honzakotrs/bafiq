use crate::compression::{
    CompressedSequence, DeltaDecoder, DeltaEncoder, DictionaryCompressor, SparseStorage,
};
use crate::FlagIndex;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Compressed flag index that implements the compression insights discovered
///
/// This incorporates:
/// - 98.6% sparsity optimization (store only used flag combinations)
/// - 61.7% delta compression on block ID sequences  
/// - Dictionary compression for shared subsequences (930+ block IDs saved)
/// - Backwards compatibility with uncompressed queries
#[derive(Debug, Serialize, Deserialize)]
pub struct CompressedFlagIndex {
    /// Sparse storage for flag combinations - only stores used flags
    sparse_storage: SparseStorage<CompressedBinData>,

    /// Dictionary compressor for common block subsequences
    dictionary: DictionaryCompressor,

    /// Compression statistics for analysis
    stats: CompressionStats,
}

/// Compressed storage for a single flag combination's data
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompressedBinData {
    /// Delta-compressed block IDs
    pub delta_compressed_blocks: Vec<u8>,

    /// Dictionary-compressed block sequences  
    pub dict_compressed_sequences: CompressedSequence,

    /// Read counts for each block (parallel to blocks)
    pub read_counts: Vec<u64>,

    /// Original number of blocks for validation
    pub original_block_count: usize,
}

// Note: Old DeltaValue and RunSegment types removed - using new compression modules

/// Statistics about compression efficiency
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStats {
    pub original_size: usize,
    pub compressed_size: usize,
    pub compression_ratio: f64,
    pub sparsity_savings: usize,
    pub delta_savings: usize,
    pub dictionary_savings: usize,
    pub runlength_savings: usize,
    pub total_flags_used: usize,
    pub total_literals: usize,
}

impl CompressionStats {
    pub fn compression_percentage(&self) -> f64 {
        (1.0 - (self.compressed_size as f64 / self.original_size as f64)) * 100.0
    }

    pub fn print_analysis(&self) {
        println!("🗜️ COMPRESSION ANALYSIS RESULTS:");
        println!("{}", "=".repeat(50));
        println!(
            "📊 Original size: {} bytes ({:.1} MB)",
            self.original_size,
            self.original_size as f64 / 1_048_576.0
        );
        println!(
            "📊 Compressed size: {} bytes ({:.1} MB)",
            self.compressed_size,
            self.compressed_size as f64 / 1_048_576.0
        );
        println!("📈 Compression ratio: {:.2}x", self.compression_ratio);
        println!("📈 Space saved: {:.1}%", self.compression_percentage());
        println!();
        println!("🔍 Breakdown by technique:");
        println!(
            "   • Sparse storage: {} bytes saved ({:.1}%)",
            self.sparsity_savings,
            self.sparsity_savings as f64 / (self.original_size - self.compressed_size) as f64
                * 100.0
        );
        println!(
            "   • Delta encoding: {} bytes saved ({:.1}%)",
            self.delta_savings,
            self.delta_savings as f64 / (self.original_size - self.compressed_size) as f64 * 100.0
        );
        println!(
            "   • Dictionary compression: {} bytes saved ({:.1}%)",
            self.dictionary_savings,
            self.dictionary_savings as f64 / (self.original_size - self.compressed_size) as f64
                * 100.0
        );
        println!(
            "   • Run-length encoding: {} bytes saved ({:.1}%)",
            self.runlength_savings,
            self.runlength_savings as f64 / (self.original_size - self.compressed_size) as f64
                * 100.0
        );
        println!();
        println!(
            "📋 Flag usage: {}/4096 ({:.1}% sparse)",
            self.total_flags_used,
            (4096 - self.total_flags_used) as f64 / 4096.0 * 100.0
        );
        println!("📋 Shared literals: {} patterns found", self.total_literals);
    }
}

impl CompressedFlagIndex {
    /// Create a compressed index from a regular FlagIndex using the new compression modules
    pub fn from_uncompressed(index: &FlagIndex) -> Result<Self> {
        println!("🗜️ Compressing FlagIndex using multi-level strategy...");

        // Step 1: Create sparse storage for used flag combinations
        let mut sparse_storage = SparseStorage::from_flag_index(index);

        // Step 2: Collect all block sequences for dictionary analysis
        let mut all_sequences = Vec::new();
        let mut bin_data_map = HashMap::new();

        for bin_idx in 0..4096 {
            if let Some(summaries) = index.bin_block_summaries(bin_idx) {
                if !summaries.is_empty() {
                    let blocks: Vec<i64> =
                        summaries.iter().map(|(block_id, _)| *block_id).collect();
                    let counts: Vec<u64> = summaries.iter().map(|(_, count)| *count).collect();

                    all_sequences.push(blocks.clone());
                    bin_data_map.insert(bin_idx as u16, (blocks, counts));
                }
            }
        }

        // Step 3: Build dictionary from all sequences
        let mut dictionary = DictionaryCompressor::default();
        dictionary.build_dictionary(&all_sequences)?;

        // Step 4: Compress each bin's data using delta + dictionary compression
        sparse_storage.ensure_data_capacity();
        let mut total_original_size = 0;
        let mut total_compressed_size = 0;
        let mut delta_savings = 0;
        let mut dict_savings = 0;

        for (flag, (blocks, counts)) in bin_data_map {
            let original_size = blocks.len() * 8 + counts.len() * 8;
            total_original_size += original_size;

            // Delta compress the block IDs
            let mut delta_encoder = DeltaEncoder::new();
            delta_encoder.encode(&blocks)?;
            let delta_bytes = delta_encoder.to_bytes();

            // Dictionary compress the sequence
            let dict_compressed = dictionary.compress(&blocks);

            let compressed_bin = CompressedBinData {
                delta_compressed_blocks: delta_bytes,
                dict_compressed_sequences: dict_compressed,
                read_counts: counts,
                original_block_count: blocks.len(),
            };

            let compressed_size = Self::estimate_bin_size(&compressed_bin);
            total_compressed_size += compressed_size;

            // Track savings
            let delta_size = delta_encoder.compressed_size();
            let original_block_size = blocks.len() * 8;
            if delta_size < original_block_size {
                delta_savings += original_block_size - delta_size;
            }

            let dict_ratio = compressed_bin.dict_compressed_sequences.compression_ratio(blocks.len());
            if dict_ratio < 1.0 {
                dict_savings += (original_block_size as f64 * (1.0 - dict_ratio)) as usize;
            }

            sparse_storage.set_data(flag, compressed_bin)?;
        }

        // Step 5: Create compression statistics
        let sparse_stats = sparse_storage.stats();
        let dict_stats = dictionary.stats();

        let compression_stats = CompressionStats {
            original_size: total_original_size,
            compressed_size: total_compressed_size,
            compression_ratio: if total_compressed_size > 0 {
                total_original_size as f64 / total_compressed_size as f64
            } else {
                1.0
            },
            sparsity_savings: (4096 - sparse_stats.used_flags) * 32, // Estimated bytes per unused bin
            delta_savings,
            dictionary_savings: dict_savings,
            runlength_savings: 0, // Not implemented in this version
            total_flags_used: sparse_stats.used_flags,
            total_literals: dict_stats.num_subsequences,
        };

        println!("   ✅ Compression complete!");
        println!(
            "   📊 Compression ratio: {:.2}x ({:.1}% savings)",
            compression_stats.compression_ratio,
            compression_stats.compression_percentage()
        );

        let compressed = Self {
            sparse_storage,
            dictionary,
            stats: compression_stats,
        };

        compressed.stats.print_analysis();

        Ok(compressed)
    }

    /// Estimate the size of compressed bin data
    fn estimate_bin_size(bin: &CompressedBinData) -> usize {
        bin.delta_compressed_blocks.len() +
        bin.dict_compressed_sequences.tokens.len() * 4 + // Rough estimate for tokens
        bin.read_counts.len() * 8 +
        8 // overhead
    }

    // Note: Old compression methods removed - now using new compression modules

    /// Query the compressed index (maintaining same interface as FlagIndex)
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

            // Fallback: try dictionary decompression
            let dict_blocks = self
                .dictionary
                .decompress(&bin_data.dict_compressed_sequences);
            {
                return dict_blocks
                    .into_iter()
                    .zip(bin_data.read_counts.iter())
                    .map(|(block, &count)| (block, count))
                    .collect();
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
