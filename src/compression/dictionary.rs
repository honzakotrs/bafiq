use serde::{Deserialize, Serialize};

/// A compressed sequence that references a dictionary
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompressedSequence {
    /// Dictionary entries: each entry is a subsequence of block IDs
    pub dictionary: Vec<Vec<i64>>,
    /// Compressed representation: mix of dictionary references and raw values
    /// References are encoded as negative values: -1 means dictionary[0], -2 means dictionary[1], etc.
    pub compressed_data: Vec<i64>,
    /// Token representation for compatibility (alias for compressed_data)
    pub tokens: Vec<i64>,
}

impl CompressedSequence {
    // Removed unused new method - use from decompression when needed

    /// Decompress back to original sequence
    pub fn decompress(&self) -> Vec<i64> {
        let mut result = Vec::new();

        for &value in &self.compressed_data {
            if value < 0 {
                // Dictionary reference
                let dict_index = (-value - 1) as usize;
                if let Some(dict_entry) = self.dictionary.get(dict_index) {
                    result.extend(dict_entry);
                }
            } else {
                // Raw value
                result.push(value);
            }
        }

        result
    }

    // Removed unused compression_ratio method
}

/// Dictionary compressor for finding and compressing repeated subsequences
#[derive(Debug, Serialize, Deserialize)]
pub struct DictionaryCompressor {
    min_length: usize,
    min_frequency: usize,
    dictionary: Vec<Vec<i64>>,
}

impl DictionaryCompressor {
    /// Create new compressor with minimum subsequence length and frequency
    pub fn new(min_length: usize, min_frequency: usize) -> Self {
        Self {
            min_length,
            min_frequency,
            dictionary: Vec::new(),
        }
    }

    // Removed unused methods: build_dictionary, compress, conflicts_with_existing

    /// Decompress a compressed sequence using this dictionary
    pub fn decompress(&self, compressed: &CompressedSequence) -> Vec<i64> {
        compressed.decompress()
    }

    // Removed unused stats method
}

/// Statistics about dictionary compression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DictionaryStats {
    pub dictionary_size: usize,
    pub total_entries: usize,
    pub num_subsequences: usize,
}

// Removed unused savings_potential function

impl Default for DictionaryCompressor {
    fn default() -> Self {
        Self::new(3, 2) // Default: min length 3, min frequency 2
    }
}
