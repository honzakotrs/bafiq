use crate::{CompressedFlagIndex, FlagIndex};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

// Removed unused IndexSerializationTrait (legacy trait replaced by concrete SerializableIndex)

/// Version information for backwards compatibility
const BAFIQ_FORMAT_VERSION: u32 = 1;
const BAFIQ_MAGIC: &[u8] = b"BAFIQ\x00\x01";

/// Serializable container for flag indexes with metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct SerializableIndex {
    /// Format version for compatibility checking
    version: u32,

    /// Timestamp when index was created
    created_timestamp: u64,

    /// Source BAM file path (for cache validation)
    source_path: String,

    /// Source BAM file size at index time (for change detection)
    source_size: u64,

    /// Source BAM file modification time (for staleness detection)
    source_mtime: u64,

    /// Index format and data
    format: IndexFormat,
}

/// Index storage format variants
#[derive(Debug, Serialize, Deserialize)]
pub enum IndexFormat {
    /// Uncompressed flag index (legacy compatibility)
    Uncompressed(FlagIndex),

    /// Multi-level compressed flag index (preferred)
    Compressed {
        index: CompressedFlagIndex,
        build_time_ms: u64,
        compression_ratio: f64,
    },
}

impl SerializableIndex {
    /// Create from uncompressed index
    pub fn from_uncompressed(index: FlagIndex, source_path: &str) -> Result<Self> {
        let source_metadata = std::fs::metadata(source_path)?;
        let source_mtime = source_metadata
            .modified()?
            .duration_since(UNIX_EPOCH)?
            .as_secs();

        Ok(Self {
            version: BAFIQ_FORMAT_VERSION,
            created_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            source_path: source_path.to_string(),
            source_size: source_metadata.len(),
            source_mtime,
            format: IndexFormat::Uncompressed(index),
        })
    }

    /// Create from compressed index with build timing
    pub fn from_compressed(
        index: CompressedFlagIndex,
        source_path: &str,
        build_time_ms: u64,
    ) -> Result<Self> {
        let source_metadata = std::fs::metadata(source_path)?;
        let source_mtime = source_metadata
            .modified()?
            .duration_since(UNIX_EPOCH)?
            .as_secs();

        let compression_ratio = index.compression_stats().compression_ratio;

        Ok(Self {
            version: BAFIQ_FORMAT_VERSION,
            created_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            source_path: source_path.to_string(),
            source_size: source_metadata.len(),
            source_mtime,
            format: IndexFormat::Compressed {
                index,
                build_time_ms,
                compression_ratio,
            },
        })
    }

    /// Save to binary file with magic header
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut file = BufWriter::new(File::create(path)?);

        // Write magic header
        file.write_all(BAFIQ_MAGIC)?;

        // Serialize the index
        bincode::serialize_into(file, self).map_err(|e| anyhow!("Serialization failed: {}", e))?;

        Ok(())
    }

    /// Load from binary file with validation
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = BufReader::new(File::open(path)?);

        // Read and validate magic header
        let mut magic = [0u8; 7];
        std::io::Read::read_exact(&mut file, &mut magic)?;

        if magic != BAFIQ_MAGIC {
            return Err(anyhow!("Invalid bafiq index file: wrong magic header"));
        }

        // Deserialize the index
        let index: SerializableIndex = bincode::deserialize_from(file)
            .map_err(|e| anyhow!("Deserialization failed: {}", e))?;

        // Validate version compatibility
        if index.version > BAFIQ_FORMAT_VERSION {
            return Err(anyhow!(
                "Index file version {} is newer than supported version {}. Please update bafiq.",
                index.version,
                BAFIQ_FORMAT_VERSION
            ));
        }

        Ok(index)
    }

    /// Check if this index is stale compared to source BAM file
    pub fn is_stale(&self) -> Result<bool> {
        // Check if source file still exists
        let source_metadata = match std::fs::metadata(&self.source_path) {
            Ok(metadata) => metadata,
            Err(_) => return Ok(true), // Source file missing = stale
        };

        // Check file size change
        if source_metadata.len() != self.source_size {
            return Ok(true);
        }

        // Check modification time
        let current_mtime = source_metadata
            .modified()?
            .duration_since(UNIX_EPOCH)?
            .as_secs();

        if current_mtime > self.source_mtime {
            return Ok(true);
        }

        Ok(false)
    }

    /// Get the underlying index (compressed or uncompressed)
    pub fn get_index(&self) -> IndexAccessor {
        match &self.format {
            IndexFormat::Uncompressed(index) => IndexAccessor::Uncompressed(index),
            IndexFormat::Compressed { index, .. } => IndexAccessor::Compressed(index),
        }
    }

    /// Get format information for display
    pub fn get_format_info(&self) -> IndexFormatInfo {
        match &self.format {
            IndexFormat::Uncompressed(index) => {
                // Estimate memory usage: bins + blocks + overhead
                let num_bins = index.bins().len();
                let total_blocks: usize = index.bins().iter().map(|bin| bin.blocks.len()).sum();
                let estimated_size = num_bins * 32 + total_blocks * 16 + 1024; // rough estimate

                IndexFormatInfo {
                    format_type: "Uncompressed".to_string(),
                    compression_ratio: 1.0,
                    build_time_ms: 0,
                    total_records: index.total_records(),
                    memory_usage_mb: estimated_size as f64 / 1_048_576.0,
                }
            }
            IndexFormat::Compressed {
                index,
                build_time_ms,
                compression_ratio,
            } => {
                let stats = index.compression_stats();
                // Get total records from compressed index
                let total_records = index
                    .used_flags()
                    .iter()
                    .map(|&flag| {
                        index
                            .get_bin_blocks(flag)
                            .iter()
                            .map(|(_, count)| count)
                            .sum::<u64>()
                    })
                    .sum();

                IndexFormatInfo {
                    format_type: "Multi-level Compressed".to_string(),
                    compression_ratio: *compression_ratio,
                    build_time_ms: *build_time_ms,
                    total_records,
                    memory_usage_mb: stats.compressed_size as f64 / 1_048_576.0,
                }
            }
        }
    }

    /// Get cache metadata
    pub fn get_cache_info(&self) -> CacheInfo {
        CacheInfo {
            version: self.version,
            created_timestamp: self.created_timestamp,
            source_path: self.source_path.clone(),
            source_size: self.source_size,
            source_mtime: self.source_mtime,
        }
    }
}

/// Unified access to different index formats
pub enum IndexAccessor<'a> {
    Uncompressed(&'a FlagIndex),
    Compressed(&'a CompressedFlagIndex),
}

impl<'a> IndexAccessor<'a> {
    /// Count reads matching flag criteria (unified interface)
    pub fn count(&self, required_bits: u16, forbidden_bits: u16) -> u64 {
        match self {
            IndexAccessor::Uncompressed(index) => index.count(required_bits, forbidden_bits),
            IndexAccessor::Compressed(index) => index.count(required_bits, forbidden_bits),
        }
    }

    /// Get total record count
    pub fn total_records(&self) -> u64 {
        match self {
            IndexAccessor::Uncompressed(index) => index.total_records(),
            IndexAccessor::Compressed(index) => {
                // Sum across all compressed bins
                index
                    .used_flags()
                    .iter()
                    .map(|&flag| {
                        index
                            .get_bin_blocks(flag)
                            .iter()
                            .map(|(_, count)| count)
                            .sum::<u64>()
                    })
                    .sum()
            }
        }
    }

    /// Check if this is a compressed index
    pub fn is_compressed(&self) -> bool {
        matches!(self, IndexAccessor::Compressed(_))
    }

    /// Get block IDs that contain reads matching the given criteria
    pub fn blocks_for(&self, required_bits: u16, forbidden_bits: u16) -> Vec<i64> {
        match self {
            IndexAccessor::Uncompressed(index) => index.blocks_for(required_bits, forbidden_bits),
            IndexAccessor::Compressed(index) => index.blocks_for(required_bits, forbidden_bits),
        }
    }

    /// Retrieve reads from BAM file that match the criteria and write to output
    pub fn retrieve_reads<P: AsRef<std::path::Path>>(
        &self,
        bam_path: P,
        required_bits: u16,
        forbidden_bits: u16,
        writer: &mut rust_htslib::bam::Writer,
    ) -> anyhow::Result<()> {
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

    /// Parallel retrieve reads from BAM file that match the criteria and write to output
    pub fn retrieve_reads_parallel<P: AsRef<std::path::Path>>(
        &self,
        bam_path: P,
        required_bits: u16,
        forbidden_bits: u16,
        writer: &mut rust_htslib::bam::Writer,
        thread_count: Option<usize>,
    ) -> anyhow::Result<()> {
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
            .ok_or_else(|| anyhow::anyhow!("Invalid BAM path"))?;

        // **LOCK-FREE ARCHITECTURE**: Unbounded channels like winning strategies
        let (sender, receiver): (
            crossbeam::channel::Sender<Vec<rust_htslib::bam::Record>>,
            crossbeam::channel::Receiver<Vec<rust_htslib::bam::Record>>,
        ) = unbounded();

        thread::scope(|s| {
            // **PRODUCER WORKERS**: Parallel block processing with work-stealing
            let num_threads = thread_count.unwrap_or_else(|| rayon::current_num_threads());
            let chunk_size = (sorted_blocks.len() / num_threads).max(1);

            let producer_handles: Vec<_> = sorted_blocks
                .chunks(chunk_size)
                .map(|block_chunk| {
                    let sender_clone = sender.clone();
                    let block_chunk = block_chunk.to_vec();

                    s.spawn(move |_| -> anyhow::Result<usize> {
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
            let consumer_handle = s.spawn(move |_| -> anyhow::Result<usize> {
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
}

/// Display information about index format
#[derive(Debug)]
pub struct IndexFormatInfo {
    pub format_type: String,
    pub compression_ratio: f64,
    pub build_time_ms: u64,
    pub total_records: u64,
    pub memory_usage_mb: f64,
}

/// Cache metadata for staleness detection
#[derive(Debug)]
pub struct CacheInfo {
    pub version: u32,
    pub created_timestamp: u64,
    pub source_path: String,
    pub source_size: u64,
    pub source_mtime: u64,
}

/// Smart index manager for automatic index building/loading  
pub struct IndexManager {
    index_dir: Option<String>,
}

impl IndexManager {
    /// Create new index manager
    pub fn new() -> Self {
        Self { index_dir: None }
    }

    /// Create with custom index directory
    pub fn with_index_dir(index_dir: String) -> Self {
        Self {
            index_dir: Some(index_dir),
        }
    }

    /// Get index file path for a BAM file
    pub fn get_index_path(&self, bam_path: &str) -> String {
        match &self.index_dir {
            Some(dir) => {
                let bam_filename = Path::new(bam_path)
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy();
                format!("{}/{}.bfi", dir, bam_filename)
            }
            None => format!("{}.bfi", bam_path), // Default: alongside BAM file
        }
    }

    /// Load index with automatic building and saving (defaults to uncompressed)
    pub fn load_or_build(&self, bam_path: &str, force_rebuild: bool) -> Result<SerializableIndex> {
        self.load_or_build_with_compression(bam_path, force_rebuild, false)
    }

    /// Load index with automatic building and saving with compression option
    pub fn load_or_build_with_compression(
        &self,
        bam_path: &str,
        force_rebuild: bool,
        use_compression: bool,
    ) -> Result<SerializableIndex> {
        let index_path = self.get_index_path(bam_path);

        // Try to load from saved index if not forcing rebuild
        if !force_rebuild && Path::new(&index_path).exists() {
            match SerializableIndex::load_from_file(&index_path) {
                Ok(saved_index) => {
                    // Check if index is still valid
                    match saved_index.is_stale() {
                        Ok(false) => {
                            eprintln!("Using saved index: {}", index_path);
                            return Ok(saved_index);
                        }
                        Ok(true) => {
                            eprintln!("Saved index is stale, rebuilding...");
                        }
                        Err(e) => {
                            eprintln!("Index validation failed ({}), rebuilding...", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to load index ({}), rebuilding...", e);
                }
            }
        } else if force_rebuild {
            eprintln!("🔄 Force rebuilding index...");
        } else {
            eprintln!("No saved index found, building...");
        }

        // Build new index
        let start_time = std::time::Instant::now();

        let builder = crate::IndexBuilder::new();
        let uncompressed_index = builder.build(bam_path)?;

        let build_time_ms = start_time.elapsed().as_millis() as u64;

        // Create serializable version - compressed only if requested
        let serializable = if use_compression {
            eprintln!("Compressing index...");
            let compressed_index = CompressedFlagIndex::from_uncompressed(&uncompressed_index)?;
            SerializableIndex::from_compressed(compressed_index, bam_path, build_time_ms)?
        } else {
            SerializableIndex::from_uncompressed(uncompressed_index, bam_path)?
        };

        // Save index
        if let Err(e) = serializable.save_to_file(&index_path) {
            eprintln!("Failed to save index: {}", e);
        } else {
            let format_info = serializable.get_format_info();
            eprintln!("Index saved: {} ({})", index_path, format_info.format_type);
        }

        Ok(serializable)
    }

    /// Clear saved index for a specific BAM file
    pub fn clear_index(&self, bam_path: &str) -> Result<()> {
        let index_path = self.get_index_path(bam_path);
        if Path::new(&index_path).exists() {
            std::fs::remove_file(&index_path)?;
            eprintln!("Index cleared: {}", index_path);
        } else {
            eprintln!("No index file to clear: {}", index_path);
        }
        Ok(())
    }
}
