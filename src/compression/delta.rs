use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::io::Write;

/// Delta encoder for compressing sequences of increasing integers
/// Uses variable-length encoding for the deltas between consecutive values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaEncoder {
    /// Compressed delta-encoded data
    data: Vec<u8>,
    /// First value in the sequence (stored separately)
    first_value: Option<i64>,
    /// Number of original values
    count: usize,
}

impl DeltaEncoder {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            first_value: None,
            count: 0,
        }
    }

    /// Encode a sequence of block IDs
    pub fn encode(&mut self, values: &[i64]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        self.count = values.len();
        self.first_value = Some(values[0]);

        // Encode deltas between consecutive values
        for window in values.windows(2) {
            let delta = window[1] - window[0];
            encode_varint(&mut self.data, delta as u64)?;
        }

        Ok(())
    }

    // Removed unused compression_ratio method

    /// Convert to bytes for storage
    pub fn to_bytes(&self) -> Vec<u8> {
        self.data.clone()
    }

    /// Get compressed size in bytes
    pub fn compressed_size(&self) -> usize {
        self.data.len()
    }
}

/// Delta decoder for decompressing delta-encoded sequences
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaDecoder {
    data: Vec<u8>,
    first_value: i64,
    count: usize,
}

impl DeltaDecoder {
    /// Create from bytes (alternative constructor)
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        // Simple implementation - in practice this would need more metadata
        Ok(Self {
            data: data.to_vec(),
            first_value: 0,
            count: 0,
        })
    }

    /// Decode the sequence back to original values
    pub fn decode(&self) -> Result<Vec<i64>> {
        if self.count == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(self.count);
        result.push(self.first_value);

        let mut cursor = 0;
        let mut current_value = self.first_value;

        for _ in 1..self.count {
            let (delta, bytes_consumed) = decode_varint(&self.data[cursor..])?;
            cursor += bytes_consumed;
            current_value += delta as i64;
            result.push(current_value);
        }

        Ok(result)
    }
}

/// Encode unsigned integer using variable-length encoding
pub fn encode_varint<W: Write>(writer: &mut W, mut value: u64) -> Result<()> {
    while value >= 128 {
        writer.write_all(&[(value & 0x7F) as u8 | 0x80])?;
        value >>= 7;
    }
    writer.write_all(&[value as u8])?;
    Ok(())
}

/// Decode variable-length encoded unsigned integer
pub fn decode_varint(data: &[u8]) -> Result<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in data {
        bytes_read += 1;
        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            return Ok((result, bytes_read));
        }

        shift += 7;
        if shift >= 64 {
            return Err(anyhow!("Varint too long"));
        }
    }

    Err(anyhow!("Unexpected end of data"))
}

impl Default for DeltaEncoder {
    fn default() -> Self {
        Self::new()
    }
}
