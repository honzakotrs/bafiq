#![allow(dead_code)] // Query iterators are infrastructure for future functionality

use crate::query::{QueryEngine, QueryRequest};
use crate::SerializableIndex;
use anyhow::Result;
use rust_htslib::bam::{self, Read};
use std::path::Path;

/// Iterator over reads matching a query
pub struct ReadIterator {
    reader: bam::Reader,
    request: QueryRequest,
    blocks: Vec<i64>,
    current_block_idx: usize,
    current_record: bam::Record,
    finished: bool,
}

impl ReadIterator {
    /// Create a new read iterator for the given query
    pub fn new<P: AsRef<Path>>(
        index: &SerializableIndex,
        request: QueryRequest,
        bam_path: P,
    ) -> Result<Self> {
        let reader = bam::Reader::from_path(bam_path)?;
        let blocks = QueryEngine::get_blocks(index, &request);

        Ok(Self {
            reader,
            request,
            blocks,
            current_block_idx: 0,
            current_record: bam::Record::new(),
            finished: false,
        })
    }

    /// Get the total number of blocks that will be processed
    pub fn total_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Get the current block index being processed
    pub fn current_block_progress(&self) -> (usize, usize) {
        (self.current_block_idx, self.blocks.len())
    }
}

impl Iterator for ReadIterator {
    type Item = Result<bam::Record>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            // Try to read from current position
            match self.reader.read(&mut self.current_record) {
                Some(Ok(())) => {
                    // Check if we're still in the current block
                    if self.current_block_idx < self.blocks.len() {
                        let current_block = self.blocks[self.current_block_idx];
                        let record_block = self.reader.tell() >> 16;

                        if record_block != current_block {
                            // Moved to next block, advance to next block in our list
                            self.current_block_idx += 1;
                            if self.current_block_idx >= self.blocks.len() {
                                self.finished = true;
                                return None;
                            }

                            // Seek to next block
                            let next_block = self.blocks[self.current_block_idx];
                            let offset = next_block << 16;
                            if let Err(e) = self.reader.seek(offset) {
                                return Some(Err(e.into()));
                            }
                            continue;
                        }
                    }

                    // Check if this record matches our query
                    let flags = self.current_record.flags();
                    if self.request.matches(flags) {
                        // Return a reference to the current record
                        // Note: We can't easily clone bam::Record, so we return the current one
                        // The caller needs to process it immediately before next() is called again
                        let mut result_record = bam::Record::new();
                        result_record.set(
                            self.current_record.qname(),
                            Some(&self.current_record.cigar()),
                            &self.current_record.seq().as_bytes(),
                            self.current_record.qual(),
                        );
                        return Some(Ok(result_record));
                    }
                    // Record doesn't match, continue to next
                }
                Some(Err(e)) => {
                    return Some(Err(e.into()));
                }
                None => {
                    // End of current block, move to next
                    self.current_block_idx += 1;
                    if self.current_block_idx >= self.blocks.len() {
                        self.finished = true;
                        return None;
                    }

                    // Seek to next block
                    let next_block = self.blocks[self.current_block_idx];
                    let offset = next_block << 16;
                    if let Err(e) = self.reader.seek(offset) {
                        return Some(Err(e.into()));
                    }
                }
            }
        }
    }
}

/// Streaming interface for processing reads without loading all into memory
pub struct ReadStream {
    iterator: ReadIterator,
    batch_size: usize,
}

impl ReadStream {
    /// Create a new read stream with default batch size
    pub fn new<P: AsRef<Path>>(
        index: &SerializableIndex,
        request: QueryRequest,
        bam_path: P,
    ) -> Result<Self> {
        let iterator = ReadIterator::new(index, request, bam_path)?;
        Ok(Self {
            iterator,
            batch_size: 1000, // Default batch size
        })
    }

    /// Create a new read stream with custom batch size
    pub fn with_batch_size<P: AsRef<Path>>(
        index: &SerializableIndex,
        request: QueryRequest,
        bam_path: P,
        batch_size: usize,
    ) -> Result<Self> {
        let iterator = ReadIterator::new(index, request, bam_path)?;
        Ok(Self {
            iterator,
            batch_size,
        })
    }

    /// Process reads in batches using a callback function
    pub fn process_batches<F, E>(&mut self, mut processor: F) -> Result<u64>
    where
        F: FnMut(&[bam::Record]) -> Result<(), E>,
        E: Into<anyhow::Error>,
    {
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut total_processed = 0;

        while let Some(record_result) = self.iterator.next() {
            match record_result {
                Ok(record) => {
                    batch.push(record);

                    if batch.len() >= self.batch_size {
                        processor(&batch).map_err(|e| e.into())?;
                        total_processed += batch.len() as u64;
                        batch.clear();
                    }
                }
                Err(e) => return Err(e),
            }
        }

        // Process remaining records
        if !batch.is_empty() {
            processor(&batch).map_err(|e| e.into())?;
            total_processed += batch.len() as u64;
        }

        Ok(total_processed)
    }

    /// Collect reads into a vector (use with caution for large result sets)
    pub fn collect_all(&mut self) -> Result<Vec<bam::Record>> {
        let mut records = Vec::new();

        while let Some(record_result) = self.iterator.next() {
            records.push(record_result?);
        }

        Ok(records)
    }

    /// Get progress information
    pub fn progress(&self) -> (usize, usize) {
        self.iterator.current_block_progress()
    }

    /// Estimate total reads (may not be exact due to filtering)
    pub fn estimated_total_reads(&self, index: &SerializableIndex) -> u64 {
        QueryEngine::count_reads(index, &self.iterator.request)
    }
}

/// Block-level iterator for processing one block at a time
pub struct BlockIterator {
    reader: bam::Reader,
    request: QueryRequest,
    blocks: Vec<i64>,
    current_block_idx: usize,
}

impl BlockIterator {
    /// Create a new block iterator
    pub fn new<P: AsRef<Path>>(
        index: &SerializableIndex,
        request: QueryRequest,
        bam_path: P,
    ) -> Result<Self> {
        let reader = bam::Reader::from_path(bam_path)?;
        let blocks = QueryEngine::get_blocks(index, &request);

        Ok(Self {
            reader,
            request,
            blocks,
            current_block_idx: 0,
        })
    }

    /// Get the total number of blocks
    pub fn total_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Get current progress
    pub fn progress(&self) -> (usize, usize) {
        (self.current_block_idx, self.blocks.len())
    }
}

impl Iterator for BlockIterator {
    type Item = Result<BlockResult>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_block_idx >= self.blocks.len() {
            return None;
        }

        let block_id = self.blocks[self.current_block_idx];
        let offset = block_id << 16;

        // Seek to the block
        if let Err(e) = self.reader.seek(offset) {
            return Some(Err(e.into()));
        }

        let mut records = Vec::new();
        let mut record = bam::Record::new();

        // Read all records from this block
        loop {
            match self.reader.read(&mut record) {
                Some(Ok(())) => {
                    let current_block = self.reader.tell() >> 16;
                    if current_block != block_id {
                        break; // Moved to next block
                    }

                    // Check if this record matches our query
                    let flags = record.flags();
                    if self.request.matches(flags) {
                        let mut result_record = bam::Record::new();
                        result_record.set(
                            record.qname(),
                            Some(&record.cigar()),
                            &record.seq().as_bytes(),
                            record.qual(),
                        );
                        records.push(result_record);
                    }
                }
                Some(Err(e)) => {
                    return Some(Err(e.into()));
                }
                None => break, // End of file
            }
        }

        self.current_block_idx += 1;

        Some(Ok(BlockResult { block_id, records }))
    }
}

/// Result of processing a single block
#[derive(Debug)]
pub struct BlockResult {
    pub block_id: i64,
    pub records: Vec<bam::Record>,
}

impl BlockResult {
    /// Get the number of matching records in this block
    pub fn count(&self) -> usize {
        self.records.len()
    }

    /// Check if this block has any matching records
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}
