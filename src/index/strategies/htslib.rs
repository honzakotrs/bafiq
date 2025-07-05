use crate::index::strategies::IndexingStrategy;
use crate::FlagIndex;
use anyhow::Result;
use rust_htslib::bam::Read;

/// HtsLib-based indexing strategy for benchmarking and compatibility
///
/// Uses rust-htslib to read BAM files and build the flag index.
/// This provides a reference implementation that can be used for:
/// - Benchmarking against optimized strategies
/// - Compatibility verification
/// - Fallback when other strategies fail
///
/// This strategy is single-threaded and may be slower than optimized implementations,
/// but it's robust and uses the well-tested rust-htslib library.
///
/// TODO: CRITICAL - This strategy does NOT match the final index shape 1:1!
/// It uses record positions as block IDs instead of actual BGZF block offsets.
/// This will cause different index contents compared to other strategies.
/// Must be fixed before final benchmarking to ensure fair comparison.
pub struct HtsLibStrategy;

impl IndexingStrategy for HtsLibStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        let mut reader = rust_htslib::bam::Reader::from_path(bam_path)?;
        let mut index = FlagIndex::new();

        // Read records one by one using rust-htslib
        for result in reader.records() {
            match result {
                Ok(record) => {
                    // Extract flags from the record
                    let flags = record.flags();

                    // Use position as a simple block ID (not ideal but works for comparison)
                    // In a real implementation, we'd need to map positions to BGZF block offsets
                    let block_id = record.pos() as i64;

                    // Add to index
                    index.add_record_at_block(flags, block_id);
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Error reading BAM record: {}", e));
                }
            }
        }

        Ok(index)
    }
}
