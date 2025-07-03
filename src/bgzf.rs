use anyhow::{anyhow, Result};
use libdeflater::Decompressor;
use std::cell::RefCell;

/// BGZF block format constants
pub const BGZF_BLOCK_MAX_SIZE: usize = 65536; // Maximum uncompressed BGZF block size
pub const BGZF_HEADER_SIZE: usize = 18;
pub const BGZF_FOOTER_SIZE: usize = 8;
pub const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// Information about a BGZF block's location in the file
#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub start_pos: usize,
    pub total_size: usize,
}

/// A BGZF block parser that provides utilities for working with BGZF compressed data
pub struct BgzfParser;

impl BgzfParser {
    /// Validate and parse a BGZF block header
    pub fn parse_header(header: &[u8]) -> Result<usize> {
        if header.len() < BGZF_HEADER_SIZE {
            return Err(anyhow!("Insufficient data for BGZF header"));
        }

        // Validate GZIP magic
        if header[0..2] != GZIP_MAGIC {
            return Err(anyhow!("Invalid GZIP magic bytes"));
        }

        // Extract block size (BSIZE field at bytes 16-17)
        let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
        let total_size = bsize + 1;

        // Validate block size
        if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > BGZF_BLOCK_MAX_SIZE {
            return Err(anyhow!("Invalid BGZF block size: {}", total_size));
        }

        Ok(total_size)
    }

    /// Discover all BGZF blocks in the given data
    pub fn discover_blocks(data: &[u8]) -> Result<Vec<BlockInfo>> {
        let mut blocks = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            if pos + BGZF_HEADER_SIZE > data.len() {
                break; // Not enough data for a complete header
            }

            let header = &data[pos..pos + BGZF_HEADER_SIZE];
            let total_size = Self::parse_header(header)?;

            if pos + total_size > data.len() {
                break; // Incomplete block at end
            }

            blocks.push(BlockInfo {
                start_pos: pos,
                total_size,
            });

            pos += total_size;
        }

        Ok(blocks)
    }

    /// Check if a decompressed block is a BAM header block
    pub fn is_header_block(decompressed_data: &[u8]) -> bool {
        decompressed_data.len() >= 4 && &decompressed_data[0..4] == b"BAM\x01"
    }
}

/// Thread-local BGZF decompressor with buffer reuse for optimal performance
pub struct ThreadLocalDecompressor {
    decompressor: RefCell<Decompressor>,
    buffer: RefCell<Vec<u8>>,
}

impl ThreadLocalDecompressor {
    /// Create a new thread-local decompressor
    pub fn new() -> Self {
        Self {
            decompressor: RefCell::new(Decompressor::new()),
            buffer: RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]),
        }
    }

    /// Decompress a BGZF block using thread-local resources
    pub fn decompress(&self, block: &[u8]) -> Result<Vec<u8>> {
        let mut decompressor = self.decompressor.borrow_mut();
        let mut buffer = self.buffer.borrow_mut();

        let decompressed_size = decompressor
            .gzip_decompress(block, &mut buffer)
            .map_err(|e| anyhow!("BGZF decompression failed: {:?}", e))?;

        Ok(buffer[..decompressed_size].to_vec())
    }

    /// Decompress a BGZF block and process records with a callback
    /// This avoids copying the decompressed data for better performance
    pub fn decompress_and_process<F, R>(&self, block: &[u8], mut processor: F) -> Result<R>
    where
        F: FnMut(&[u8]) -> Result<R>,
    {
        let mut decompressor = self.decompressor.borrow_mut();
        let mut buffer = self.buffer.borrow_mut();

        let decompressed_size = decompressor
            .gzip_decompress(block, &mut buffer)
            .map_err(|e| anyhow!("BGZF decompression failed: {:?}", e))?;

        processor(&buffer[..decompressed_size])
    }
}

impl Default for ThreadLocalDecompressor {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    static GLOBAL_DECOMPRESSOR: ThreadLocalDecompressor = ThreadLocalDecompressor::new();
}

/// Convenience function to decompress a BGZF block using the global thread-local decompressor
pub fn decompress_block(block: &[u8]) -> Result<Vec<u8>> {
    GLOBAL_DECOMPRESSOR.with(|decomp| decomp.decompress(block))
}

/// Convenience function to decompress and process a BGZF block using the global thread-local decompressor
pub fn decompress_and_process<F, R>(block: &[u8], processor: F) -> Result<R>
where
    F: FnMut(&[u8]) -> Result<R>,
{
    GLOBAL_DECOMPRESSOR.with(|decomp| decomp.decompress_and_process(block, processor))
}
