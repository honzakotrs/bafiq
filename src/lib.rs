use anyhow::Result;
use rust_htslib::bam;
use rust_htslib::bam::{Read, Writer};
use std::path::Path;

/// By SAM spec, the lower 12 bits are commonly used.
const FLAG_MASK: u16 = 0xFFF;
/// 2^12 = 4096 possible combos.
const N_FLAGS: usize = 1 << 12;

/// For each 12-bit flag combination, we store `(block_id, count_of_reads)` pairs.
#[derive(Debug, Default)]
pub struct BinInfo {
    block_summaries: Vec<(i64, u64)>,
}

impl BinInfo {
    fn new() -> Self {
        Self {
            block_summaries: Vec::new(),
        }
    }

    /// Record one read in the given block_id.
    fn add_read(&mut self, block_id: i64) {
        if let Some((last_block, cnt)) = self.block_summaries.last_mut() {
            if *last_block == block_id {
                *cnt += 1;
                return;
            }
        }
        // new block_id
        self.block_summaries.push((block_id, 1));
    }

    /// Total reads across all blocks in this bin.
    fn total_reads(&self) -> u64 {
        self.block_summaries.iter().map(|&(_, c)| c).sum()
    }
}

/// Main index: 4096 bins, each storing info about which blocks contain reads of that flag pattern.
#[derive(Debug)]
pub struct FlagIndex {
    bins: Vec<BinInfo>,
}

impl FlagIndex {
    pub fn new() -> Self {
        let mut bins = Vec::with_capacity(N_FLAGS);
        bins.resize_with(N_FLAGS, BinInfo::new);
        Self { bins }
    }

    /// Build an index by scanning the file once. 
    /// For each record:
    ///   - mask the 12-bit flags
    ///   - compute block_id = ( tell() >> 16 )
    ///   - store in bins[flag].
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut reader = bam::Reader::from_path(path)?;
        let mut record = bam::Record::new();
        let mut index = Self::new();

        // read() -> Option<Result<(), bam::errors::Error>>
        while let Some(res) = reader.read(&mut record) {
            // If there's an I/O error, propagate it.
            res?;

            let f = record.flags() & FLAG_MASK;
            let bin_idx = f as usize;

            let v_offset = reader.tell(); // i64
            let block_id = v_offset >> 16; // shift away sub-block offset

            index.bins[bin_idx].add_read(block_id);
        }

        Ok(index)
    }

    /// Count how many reads match required_bits & forbidden_bits.
    pub fn count(&self, required_bits: u16, forbidden_bits: u16) -> u64 {
        let mut total = 0;
        for bin in 0..N_FLAGS {
            let b = bin as u16;
            if (b & required_bits) == required_bits && (b & forbidden_bits) == 0 {
                total += self.bins[bin].total_reads();
            }
        }
        total
    }

    /// Get all blocks that might contain reads matching the bit criteria.
    pub fn blocks_for(&self, required_bits: u16, forbidden_bits: u16) -> Vec<i64> {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        for bin in 0..N_FLAGS {
            let b = bin as u16;
            if (b & required_bits) == required_bits && (b & forbidden_bits) == 0 {
                for &(block_id, _) in &self.bins[bin].block_summaries {
                    set.insert(block_id);
                }
            }
        }
        let mut v: Vec<i64> = set.into_iter().collect();
        v.sort_unstable();
        v
    }

    /// Retrieve matching reads and write them to `writer`.
    pub fn retrieve_reads<P: AsRef<Path>>(
        &self,
        bam_path: P,
        required_bits: u16,
        forbidden_bits: u16,
        writer: &mut Writer,
    ) -> Result<()> {
        let blocks = self.blocks_for(required_bits, forbidden_bits);

        let mut reader = bam::Reader::from_path(bam_path)?;
        let mut record = bam::Record::new();

        for &block_id in &blocks {
            let offset = block_id << 16;
            reader.seek(offset)?; // takes i64

            while let Some(res) = reader.read(&mut record) {
                res?;
                let cur_block_id = reader.tell() >> 16;
                if cur_block_id != block_id {
                    break;
                }

                let f = record.flags() & FLAG_MASK;
                if (f & required_bits) == required_bits && (f & forbidden_bits) == 0 {
                    writer.write(&record)?;
                }
            }
        }
        Ok(())
    }
}