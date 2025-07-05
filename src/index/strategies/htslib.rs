use crate::index::strategies::IndexingStrategy;
use crate::FlagIndex;
use anyhow::Result;
use rust_htslib::bam::Read;

/// **HTSLIB STRATEGY** - Reference implementation using rust-htslib (3.888s)
///
/// **Role as Reference Implementation:**
/// - Provides independent verification of other strategies' correctness
/// - Uses well-tested, mature rust-htslib library for BAM parsing
/// - Demonstrates performance baseline for library-based approaches
/// - Fallback implementation when custom strategies fail
///
/// **Performance Insights:**
/// - 3.888s performance shows rust-htslib is competitive but not fastest
/// - Single-threaded nature limits scalability vs parallel strategies
/// - 479ms slower than fastest strategy (rayon_wait_free at 3.409s)
/// - Proves that custom implementations can meaningfully outperform libraries
///
/// **Critical Index Compatibility Issue:**
/// - ⚠️ **WARNING**: This strategy produces INCOMPATIBLE indexes!
/// - Uses BAM record positions as block IDs instead of BGZF virtual file offsets
/// - Indexes built with this strategy CANNOT be used with `bafiq view` command
/// - Only suitable for record counting, not for random access retrieval
/// - All other strategies use proper BGZF block offsets for `bafiq view` compatibility
///
/// **When to Use:**
/// - Verification and cross-checking of other strategies' record counts
/// - Environments where rust-htslib is already a dependency
/// - When implementation simplicity is more important than performance
/// - Temporary fallback during development/debugging
///
/// **Architecture:**
/// - File I/O: rust-htslib handles all BAM/BGZF details internally
/// - Parsing: Library-provided record iteration and flag extraction
/// - Threading: Single-threaded (library limitation)
/// - Index Format: ⚠️ Incompatible with other strategies (uses positions not offsets)
///
/// **Note for Future Development:**
/// - Could be fixed to use proper BGZF virtual file offsets for compatibility
/// - Would require rust-htslib virtual offset API integration
/// - Currently kept for reference/verification purposes only
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
