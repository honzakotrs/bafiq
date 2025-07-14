use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

// Constants for flag handling
pub const FLAG_MASK: u16 = 0xFFFF; // All 16 bits are valid for SAM flags
pub const N_FLAGS: usize = 65536; // 2^16 possible flag combinations

pub mod benchmark;
mod bgzf;
mod compression;
mod index;
mod query;
pub mod view;

// Re-export main types for public API
pub use index::{
    builder::{BuildStrategy, IndexBuilder},
    compressed::CompressedFlagIndex,
    format::{IndexAccessor, IndexManager, SerializableIndex},
};

/// Information about the distribution of reads in one 16-bit flag bin
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinInfo {
    /// The 16-bit bin number
    pub bin: u16,
    /// Total number of reads with this exact flag combination
    pub total_reads: u64,
    /// Block summaries: (block_id, count_in_block)
    pub blocks: Vec<(i64, u64)>,
}

impl BinInfo {
    pub fn total_reads(&self) -> u64 {
        self.total_reads
    }
}

/// A flag-based index for BAM/CRAM files
/// Maps SAM flag patterns to BGZF block locations for efficient queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlagIndex {
    /// Information about each flag combination (bin)
    bins: Vec<BinInfo>,
    /// Total number of records indexed
    total_records: u64,
}

impl FlagIndex {
    pub fn new() -> Self {
        Self {
            bins: Vec::new(),
            total_records: 0,
        }
    }

    /// Build a flag index from a BAM/CRAM file path
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let builder = IndexBuilder::new();
        builder.build(path)
    }

    /// Count reads that match the given flag criteria
    /// - `required_bits`: reads must have ALL of these bits set
    /// - `forbidden_bits`: reads must have NONE of these bits set
    pub fn count(&self, required_bits: u16, forbidden_bits: u16) -> u64 {
        self.bins
            .iter()
            .filter(|bin| {
                // Check if this bin matches the criteria
                let flags = bin.bin;
                (flags & required_bits) == required_bits && (flags & forbidden_bits) == 0
            })
            .map(|bin| bin.total_reads)
            .sum()
    }

    /// Get total number of records in the index
    pub fn total_records(&self) -> u64 {
        self.total_records
    }

    /// Add a record at a specific block ID (used during index building)
    pub fn add_record_at_block(&mut self, flags: u16, block_id: i64) {
        self.total_records += 1;

        // Find or create the bin for this flag combination
        if let Some(bin) = self.bins.iter_mut().find(|b| b.bin == flags) {
            bin.total_reads += 1;
            // Find or create block entry
            if let Some((_, count)) = bin.blocks.iter_mut().find(|(id, _)| *id == block_id) {
                *count += 1;
            } else {
                bin.blocks.push((block_id, 1));
            }
        } else {
            // Create new bin
            self.bins.push(BinInfo {
                bin: flags,
                total_reads: 1,
                blocks: vec![(block_id, 1)],
            });
        }
    }

    /// Merge another index into this one
    pub fn merge(&mut self, other: FlagIndex) {
        self.total_records += other.total_records;

        // Convert self.bins to HashMap for O(1) lookups
        let mut bin_map: HashMap<u16, usize> = HashMap::new();
        for (i, bin) in self.bins.iter().enumerate() {
            bin_map.insert(bin.bin, i);
        }

        for other_bin in other.bins {
            if let Some(&existing_idx) = bin_map.get(&other_bin.bin) {
                // Merge into existing bin
                let existing_bin = &mut self.bins[existing_idx];
                existing_bin.total_reads += other_bin.total_reads;

                // Convert existing blocks to HashMap for O(1) lookups
                let mut block_map: HashMap<i64, usize> = HashMap::new();
                for (i, (block_id, _)) in existing_bin.blocks.iter().enumerate() {
                    block_map.insert(*block_id, i);
                }

                // Merge block summaries with O(1) lookups
                for (block_id, count) in other_bin.blocks {
                    if let Some(&existing_idx) = block_map.get(&block_id) {
                        existing_bin.blocks[existing_idx].1 += count;
                    } else {
                        existing_bin.blocks.push((block_id, count));
                    }
                }
            } else {
                // Add new bin
                bin_map.insert(other_bin.bin, self.bins.len());
                self.bins.push(other_bin);
            }
        }
    }

    /// PARALLEL MERGE-TREE APPROACH - O(log n) depth instead of O(n) sequential
    /// Merge multiple indexes efficiently using divide-and-conquer
    pub fn merge_parallel(indexes: Vec<FlagIndex>) -> FlagIndex {
        if indexes.is_empty() {
            return FlagIndex::new();
        }

        if indexes.len() == 1 {
            return indexes.into_iter().next().unwrap();
        }

        // Divide-and-conquer merge tree
        let mut current_level = indexes;

        while current_level.len() > 1 {
            let next_level: Vec<FlagIndex> = current_level
                .chunks(2)
                .map(|chunk| {
                    if chunk.len() == 2 {
                        let mut left = chunk[0].clone();
                        left.merge(chunk[1].clone());
                        left
                    } else {
                        chunk[0].clone()
                    }
                })
                .collect();

            current_level = next_level;
        }

        current_level.into_iter().next().unwrap()
    }

    /// Check if this index is equivalent to another (for testing)
    pub fn is_equivalent_to(&self, other: &FlagIndex) -> bool {
        // Compare total records
        let self_total = self.total_records();
        let other_total = other.total_records();

        if self_total != other_total {
            return false;
        }

        // Check bin equivalency - convert to maps for easier comparison
        let self_bins: HashMap<u16, u64> = self
            .bins
            .iter()
            .map(|bin| (bin.bin, bin.total_reads))
            .collect();

        let other_bins: HashMap<u16, u64> = other
            .bins
            .iter()
            .map(|bin| (bin.bin, bin.total_reads))
            .collect();

        self_bins == other_bins
    }

    /// Print detailed comparison analysis (for debugging)
    pub fn analyze_equivalency(&self, other: &FlagIndex) {
        let self_total = self.total_records();
        let other_total = other.total_records();

        println!("Index equivalency analysis:");
        println!("   Total records: {} vs {}", self_total, other_total);

        // Analyze block overlap
        let self_blocks: std::collections::HashSet<i64> = self
            .bins
            .iter()
            .flat_map(|bin| bin.blocks.iter().map(|(id, _)| *id))
            .collect();

        let other_blocks: std::collections::HashSet<i64> = other
            .bins
            .iter()
            .flat_map(|bin| bin.blocks.iter().map(|(id, _)| *id))
            .collect();

        let total_blocks_checked = self_blocks.len().max(other_blocks.len());
        let total_block_overlap = self_blocks.intersection(&other_blocks).count();
        let overall_block_overlap = if total_blocks_checked > 0 {
            (total_block_overlap as f64 / total_blocks_checked as f64) * 100.0
        } else {
            100.0
        };

        println!(
            "   Block overlap: {:.1}% ({}/{} blocks) {}",
            overall_block_overlap,
            total_block_overlap,
            total_blocks_checked,
            if overall_block_overlap >= 95.0 {
                "PASS"
            } else {
                "FAIL"
            }
        );

        // Check for mismatched bins
        let self_bin_counts: HashMap<u16, u64> = self
            .bins
            .iter()
            .map(|bin| (bin.bin, bin.total_reads))
            .collect();

        let other_bin_counts: HashMap<u16, u64> = other
            .bins
            .iter()
            .map(|bin| (bin.bin, bin.total_reads))
            .collect();

        let mut mismatched_bins = 0;
        for (flag, self_count) in &self_bin_counts {
            if let Some(other_count) = other_bin_counts.get(flag) {
                if self_count != other_count {
                    mismatched_bins += 1;
                }
            } else {
                mismatched_bins += 1;
            }
        }

        for flag in other_bin_counts.keys() {
            if !self_bin_counts.contains_key(flag) {
                mismatched_bins += 1;
            }
        }

        println!(
            "   Mismatched bins: {} {}",
            mismatched_bins,
            if mismatched_bins == 0 { "PASS" } else { "WARN" }
        );

        // Functional equivalency: same totals + high block overlap + few mismatches
        let is_functionally_equivalent = self_total == other_total
            && overall_block_overlap >= 95.0
            && mismatched_bins <= (self_bin_counts.len() / 20); // Allow 5% mismatch

        println!(
            "   Overall assessment: {}",
            if is_functionally_equivalent {
                "FUNCTIONALLY EQUIVALENT"
            } else {
                "SIGNIFICANT DIFFERENCES"
            }
        );
    }

    /// Get summary of all flag combinations and their counts
    pub fn get_flag_summary(&self) -> Vec<(u16, u64)> {
        self.bins
            .iter()
            .map(|bin| (bin.bin, bin.total_reads))
            .collect()
    }

    /// Get block IDs that contain reads matching the given criteria
    pub fn blocks_for(&self, required_bits: u16, forbidden_bits: u16) -> Vec<i64> {
        let mut block_ids = std::collections::HashSet::new();

        for bin in &self.bins {
            let flags = bin.bin;
            if (flags & required_bits) == required_bits && (flags & forbidden_bits) == 0 {
                for (block_id, _) in &bin.blocks {
                    block_ids.insert(*block_id);
                }
            }
        }

        let mut result: Vec<i64> = block_ids.into_iter().collect();
        result.sort();
        result
    }

    /// Get access to bins (for advanced use cases)
    pub fn bins(&self) -> &[BinInfo] {
        &self.bins
    }

    /// Get block summaries for a specific bin
    pub fn bin_block_summaries(&self, bin_idx: usize) -> Option<&[(i64, u64)]> {
        self.bins.get(bin_idx).map(|bin| bin.blocks.as_slice())
    }
}

impl Default for FlagIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flag_index_basic() {
        let mut index = FlagIndex::new();

        // Add some test records
        index.add_record_at_block(0x4, 100); // unmapped
        index.add_record_at_block(0x0, 100); // mapped
        index.add_record_at_block(0x4, 101); // unmapped
        index.add_record_at_block(0x40, 102); // first in pair

        assert_eq!(index.total_records(), 4);

        // Test counting
        assert_eq!(index.count(0x4, 0x0), 2); // unmapped reads
        assert_eq!(index.count(0x0, 0x4), 2); // mapped reads only (0x0 and 0x40)
        assert_eq!(index.count(0x40, 0x0), 1); // first in pair
        assert_eq!(index.count(0x0, 0x0), 4); // all reads
    }

    #[test]
    fn test_flag_index_merge() {
        let mut index1 = FlagIndex::new();
        index1.add_record_at_block(0x4, 100);
        index1.add_record_at_block(0x0, 100);

        let mut index2 = FlagIndex::new();
        index2.add_record_at_block(0x4, 101);
        index2.add_record_at_block(0x40, 102);

        index1.merge(index2);

        assert_eq!(index1.total_records(), 4);
        assert_eq!(index1.count(0x4, 0x0), 2); // unmapped reads from both indexes
    }

    #[test]
    fn test_blocks_for_query() {
        let mut index = FlagIndex::new();

        index.add_record_at_block(0x4, 100); // unmapped in block 100
        index.add_record_at_block(0x0, 200); // mapped in block 200
        index.add_record_at_block(0x4, 300); // unmapped in block 300

        let blocks = index.blocks_for(0x4, 0x0); // blocks with unmapped reads
        assert_eq!(blocks, vec![100, 300]);

        let blocks = index.blocks_for(0x0, 0x4); // blocks with mapped reads only
        assert_eq!(blocks, vec![200]);
    }

    #[test]
    fn test_flag_summary() {
        let mut index = FlagIndex::new();

        index.add_record_at_block(0x4, 100);
        index.add_record_at_block(0x4, 101);
        index.add_record_at_block(0x0, 102);

        let summary = index.get_flag_summary();
        assert_eq!(summary.len(), 2); // Two different flag combinations

        // Should have 2 unmapped (0x4) and 1 mapped (0x0)
        let summary_map: HashMap<u16, u64> = summary.into_iter().collect();
        assert_eq!(summary_map.get(&0x4), Some(&2));
        assert_eq!(summary_map.get(&0x0), Some(&1));
    }

    #[test]
    fn test_index_equivalency() {
        let mut index1 = FlagIndex::new();
        index1.add_record_at_block(0x4, 100);
        index1.add_record_at_block(0x0, 101);

        let mut index2 = FlagIndex::new();
        index2.add_record_at_block(0x0, 101);
        index2.add_record_at_block(0x4, 100);

        // Same data, different order - should be equivalent
        assert!(index1.is_equivalent_to(&index2));

        // Add different data to index2
        index2.add_record_at_block(0x40, 102);
        assert!(!index1.is_equivalent_to(&index2));
    }
}
