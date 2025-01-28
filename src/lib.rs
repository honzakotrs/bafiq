use anyhow::Result;
use bincode::{deserialize_from, serialize_into};
use byteorder::{LittleEndian, ReadBytesExt};
use crossbeam::channel::bounded;
use flate2::read::GzDecoder;
use num_cpus;
use rust_htslib::bam;
use rust_htslib::bam::{Read, Reader, Writer};
use rust_htslib::htslib::{bgzf_read_block, bgzf_utell};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read as IoRead};
use std::path::Path;
use std::slice;
use std::sync::{Arc, Mutex};

/// By SAM spec, the lower 12 bits are commonly used.
const FLAG_MASK: u16 = 0xFFF;
/// 2^12 = 4096 possible combos.
const N_FLAGS: usize = 1 << 12;

/// For each 12-bit flag combination, we store `(block_id, count_of_reads)` pairs.
#[derive(Debug, Default, Serialize, Deserialize)]
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
#[derive(Debug, Serialize, Deserialize)]
pub struct FlagIndex {
    /// Each BinInfo is wrapped in a Mutex so multiple threads can update safely
    bins: Vec<Mutex<BinInfo>>,
}

impl FlagIndex {
    pub fn new() -> Self {
        let bins: Vec<Mutex<BinInfo>> = (0..N_FLAGS).map(|_| Mutex::new(BinInfo::new())).collect();
        Self { bins }
    }

    /// Create index from an **unindexed** BAM by scanning each BGZF block in parallel.
    ///
    /// For each BGZF block read:
    ///  - we get `bgzf_tell` -> 64-bit offset, then do `(offset >> 16)` to get `block_id`
    ///  - decompress records in worker threads via `RecordReader`
    ///  - store read counts in `bins[flag].add_read(block_id)`
    pub fn from_path<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<FlagIndex> {
        let reader = Reader::from_path(path)?;
        let index = Arc::new(FlagIndex::new());
        let (sender, receiver) = bounded::<(i64, Vec<u8>)>(10); // Bounded channel
        let n_threads = num_cpus::get();

        // Producer thread
        std::thread::spawn({
            let sender = sender.clone();
            move || {
                let hts_file_ptr = reader.htsfile() as *mut rust_htslib::htslib::htsFile;
                let bgzf_ptr = unsafe { (*hts_file_ptr).fp.bgzf };

                loop {
                    // Get the current virtual offset
                    let offset = unsafe { bgzf_utell(bgzf_ptr) };
                    if offset < 0 {
                        break;
                    }

                    // Read the next compressed BGZF block
                    let ret_code = unsafe { bgzf_read_block(bgzf_ptr) };
                    if ret_code < 0 {
                        eprintln!("Error reading BGZF block, ret_code={}", ret_code);
                        break;
                    }
                    if ret_code == 0 {
                        // End of file
                        break;
                    }

                    // Copy the compressed data into memory
                    let compressed_length = unsafe { (*bgzf_ptr).block_length as usize };
                    if compressed_length == 0 {
                        break;
                    }
                    let compressed_ptr = unsafe { (*bgzf_ptr).compressed_block as *const u8 };
                    let compressed_data =
                        unsafe { slice::from_raw_parts(compressed_ptr, compressed_length) }
                            .to_vec();

                    // Convert offset -> block_id
                    let block_id = offset >> 16;

                    // Send to consumers
                    if sender.send((block_id, compressed_data)).is_err() {
                        break; // Consumers are done
                    }
                }

                // Drop the sender to signal the end of processing
                drop(sender);
            }
        });

        // Consumer threads
        std::thread::scope(|s| {
            for _ in 0..n_threads {
                let rx = receiver.clone();
                let index_ref = Arc::clone(&index);
                s.spawn(move || {
                    while let Ok((block_id, compressed_data)) = rx.recv() {
                        // Decompress the block using flate2
                        let mut decoder = GzDecoder::new(&compressed_data[..]);
                        let mut uncompressed_data = Vec::new();
                        decoder.read_to_end(&mut uncompressed_data)?;

                        // Parse records from the uncompressed block
                        let mut cursor = Cursor::new(uncompressed_data);
                        while cursor.position() < cursor.get_ref().len() as u64 {
                            let block_size = cursor.read_u32::<LittleEndian>()?;
                            let flag = cursor.read_u16::<LittleEndian>()?;
                            let bin_idx = (flag & FLAG_MASK) as usize;

                            // Update the shared index
                            let mut bin_info = index_ref.bins[bin_idx].lock().unwrap();
                            bin_info.add_read(block_id);

                            // Skip the remaining bytes of the record
                            let remaining_bytes = block_size as u64 - 6;
                            cursor.set_position(cursor.position() + remaining_bytes);
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                });
            }
        });

        // Extract the final FlagIndex
        let unwrapped_index = Arc::try_unwrap(index)
            .expect("Arc::try_unwrap failed - multiple references still exist");
        Ok(unwrapped_index)
    }

    /// Count how many reads match required_bits & forbidden_bits.
    pub fn count(&self, required_bits: u16, forbidden_bits: u16) -> u64 {
        let mut total = 0;
        for bin in 0..N_FLAGS {
            let b = bin as u16;
            if (b & required_bits) == required_bits && (b & forbidden_bits) == 0 {
                let bin_info = self.bins[bin].lock().unwrap();
                total += bin_info.total_reads();
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
                let bin_info = self.bins[bin].lock().unwrap();
                for &(block_id, _) in &bin_info.block_summaries {
                    set.insert(block_id);
                }
            }
        }
        let mut v: Vec<i64> = set.into_iter().collect();
        v.sort_unstable();
        v
    }

    /// Retrieve matching reads and write them to `writer`.
    /// (unchanged from your original, except that it uses the block offsets we stored in `block_id`)
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
            let offset = block_id << 16; // shift back to the real virtual offset
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

    pub fn save_to_file(&self, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serialize_into(writer, self)?;
        Ok(())
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let index = deserialize_from(reader)?;
        Ok(index)
    }
}
