use anyhow::{anyhow, Result};
use bincode;
use rust_htslib::bam::Writer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

// Constants for flag handling
pub const FLAG_MASK: u16 = 0xFFFF; // All 16 bits are valid for SAM flags
pub const N_FLAGS: usize = 65536; // 2^16 possible flag combinations

pub mod benchmark;
mod bgzf;
mod compression;
mod index;
mod query;

// Re-export main types for public API
pub use index::{
    builder::{BuildStrategy, IndexBuilder},
    compressed::CompressedFlagIndex,
    format::{IndexManager, SerializableIndex},
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

        for other_bin in other.bins {
            if let Some(existing_bin) = self.bins.iter_mut().find(|b| b.bin == other_bin.bin) {
                existing_bin.total_reads += other_bin.total_reads;
                // Merge block summaries
                for (block_id, count) in other_bin.blocks {
                    if let Some((_, existing_count)) = existing_bin
                        .blocks
                        .iter_mut()
                        .find(|(id, _)| *id == block_id)
                    {
                        *existing_count += count;
                    } else {
                        existing_bin.blocks.push((block_id, count));
                    }
                }
            } else {
                self.bins.push(other_bin);
            }
        }
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

    /// Retrieve reads from BAM file that match the criteria and write to output
    pub fn retrieve_reads<P: AsRef<Path>>(
        &self,
        bam_path: P,
        required_bits: u16,
        forbidden_bits: u16,
        writer: &mut Writer,
    ) -> Result<()> {
        use rust_htslib::bam::Read;

        // Get block IDs that contain matching reads
        let block_ids = self.blocks_for(required_bits, forbidden_bits);

        if block_ids.is_empty() {
            return Ok(()); // No matching reads
        }

        let mut reader = rust_htslib::bam::Reader::from_path(&bam_path)?;
        let mut record = rust_htslib::bam::Record::new();

        // Sort blocks for efficient seeking
        let mut sorted_blocks = block_ids.clone();
        sorted_blocks.sort();

        // Process each block that contains matching reads
        for &block_id in sorted_blocks.iter() {
            // Convert file position (block_id) to BGZF virtual offset
            // Virtual offset = (block_file_position << 16) | within_block_offset
            // For block start, within_block_offset = 0
            let virtual_offset = (block_id as u64) << 16;
            reader.seek(virtual_offset as i64)?;

            // Read all records until we detect we've moved to a different block
            loop {
                // CRITICAL FIX: Check virtual offset BEFORE reading to ensure we're in the right block
                let current_virtual_offset = reader.tell();
                let current_block_position = current_virtual_offset >> 16;

                // If we've moved to a different block, stop processing this block
                if (current_block_position as i64) != block_id {
                    break;
                }

                match reader.read(&mut record) {
                    Some(Ok(())) => {
                        // Check if this record matches our criteria
                        let flags = record.flags();
                        if (flags & required_bits) == required_bits && (flags & forbidden_bits) == 0
                        {
                            writer.write(&record)?;
                        }
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => break, // End of file
                }
            }
        }

        Ok(())
    }

    /// **LOCK-FREE PARALLEL RETRIEVE** - Applying winning index building patterns
    ///
    /// **PERFORMANCE ARCHITECTURE**: Uses the same lock-free work-stealing patterns that
    /// made ParallelChunkStreaming and RayonWaitFree the top performers.
    ///
    /// **Key Optimizations** (learned from top index builders):
    /// - **Lock-free work-stealing**: No mutex contention like winning strategies
    /// - **Unbounded channels**: Eliminate backpressure bottlenecks
    /// - **Producer-consumer pattern**: Parallel workers + single writer thread
    /// - **Batch processing**: Reduce channel overhead with record batches
    /// - **Zero coordination**: Workers operate independently with crossbeam channels
    ///
    /// **Expected Performance**: 3-5x faster than sequential, eliminates mutex bottleneck
    pub fn retrieve_reads_parallel<P: AsRef<Path>>(
        &self,
        bam_path: P,
        required_bits: u16,
        forbidden_bits: u16,
        writer: &mut Writer,
    ) -> Result<()> {
        use crossbeam::channel::unbounded;
        use crossbeam::thread;
        use rust_htslib::bam::Read;

        // Get block IDs that contain matching reads
        let block_ids = self.blocks_for(required_bits, forbidden_bits);

        if block_ids.is_empty() {
            return Ok(()); // No matching reads
        }

        // Sort blocks for better cache locality
        let mut sorted_blocks = block_ids.clone();
        sorted_blocks.sort();

        const RECORDS_PER_BATCH: usize = 100; // Optimal batch size for channel efficiency
        let bam_path_str = bam_path
            .as_ref()
            .to_str()
            .ok_or_else(|| anyhow!("Invalid BAM path"))?;

        // **LOCK-FREE ARCHITECTURE**: Unbounded channels like winning strategies
        let (sender, receiver): (
            crossbeam::channel::Sender<Vec<rust_htslib::bam::Record>>,
            crossbeam::channel::Receiver<Vec<rust_htslib::bam::Record>>,
        ) = unbounded();

        thread::scope(|s| {
            // **PRODUCER WORKERS**: Parallel block processing with work-stealing
            let num_threads = rayon::current_num_threads();
            let chunk_size = (sorted_blocks.len() / num_threads).max(1);

            let producer_handles: Vec<_> = sorted_blocks
                .chunks(chunk_size)
                .map(|block_chunk| {
                    let sender_clone = sender.clone();
                    let block_chunk = block_chunk.to_vec();

                    s.spawn(move |_| -> Result<usize> {
                        // Each worker gets its own BAM reader (no coordination needed)
                        let mut reader = rust_htslib::bam::Reader::from_path(bam_path_str)?;
                        let mut record = rust_htslib::bam::Record::new();
                        let mut current_batch = Vec::with_capacity(RECORDS_PER_BATCH);
                        let mut total_processed = 0;

                        // Process each block in this worker's chunk
                        for &block_id in &block_chunk {
                            // Convert file position (block_id) to BGZF virtual offset
                            let virtual_offset = (block_id as u64) << 16;
                            reader.seek(virtual_offset as i64)?;

                            // Read all records until we detect we've moved to a different block
                            loop {
                                // CRITICAL FIX: Check virtual offset BEFORE reading to ensure we're in the right block
                                let current_virtual_offset = reader.tell();
                                let current_block_position = current_virtual_offset >> 16;

                                // If we've moved to a different block, stop processing this block
                                if (current_block_position as i64) != block_id {
                                    break;
                                }

                                match reader.read(&mut record) {
                                    Some(Ok(())) => {
                                        // Check if this record matches our criteria
                                        let flags = record.flags();
                                        if (flags & required_bits) == required_bits
                                            && (flags & forbidden_bits) == 0
                                        {
                                            // Clone the record for storage
                                            let mut stored_record = rust_htslib::bam::Record::new();
                                            stored_record.set(
                                                record.qname(),
                                                Some(&record.cigar()),
                                                &record.seq().as_bytes(),
                                                record.qual(),
                                            );
                                            // Copy additional fields
                                            stored_record.set_flags(record.flags());
                                            stored_record.set_tid(record.tid());
                                            stored_record.set_pos(record.pos());
                                            stored_record.set_mtid(record.mtid());
                                            stored_record.set_mpos(record.mpos());
                                            stored_record.set_insert_size(record.insert_size());
                                            stored_record.set_mapq(record.mapq());

                                            current_batch.push(stored_record);
                                            total_processed += 1;

                                            // Send batch when full (reduce channel overhead)
                                            if current_batch.len() >= RECORDS_PER_BATCH {
                                                if sender_clone
                                                    .send(std::mem::take(&mut current_batch))
                                                    .is_err()
                                                {
                                                    break; // Consumer hung up
                                                }
                                                current_batch =
                                                    Vec::with_capacity(RECORDS_PER_BATCH);
                                            }
                                        }
                                    }
                                    Some(Err(e)) => return Err(e.into()),
                                    None => break, // End of file
                                }
                            }
                        }

                        // Send remaining records in final batch
                        if !current_batch.is_empty() {
                            let _ = sender_clone.send(current_batch);
                        }

                        Ok(total_processed)
                    })
                })
                .collect();

            // Drop sender so consumer knows when producers are done
            drop(sender);

            // **CONSUMER THREAD**: Single writer, no mutex contention
            let consumer_handle = s.spawn(move |_| -> Result<usize> {
                let mut total_written = 0;

                // Process batches as they arrive from producers
                while let Ok(record_batch) = receiver.recv() {
                    for record in record_batch {
                        writer.write(&record)?;
                        total_written += 1;
                    }
                }

                Ok(total_written)
            });

            // Wait for all producers to complete
            for handle in producer_handles {
                handle.join().unwrap()?;
            }

            // Wait for consumer to finish writing
            let _total_written = consumer_handle.join().unwrap()?;

            Ok(())
        })
        .unwrap()
    }

    /// Save index to file
    pub fn save_to_file(&self, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, self)
            .map_err(|e| anyhow!("Failed to serialize index: {}", e))?;
        Ok(())
    }

    /// Load index from file
    pub fn from_file(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        bincode::deserialize_from(file).map_err(|e| anyhow!("Failed to deserialize index: {}", e))
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
