use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    /// Create from dictionary and compressed data
    pub fn new(dictionary: Vec<Vec<i64>>, compressed_data: Vec<i64>) -> Self {
        Self {
            dictionary,
            tokens: compressed_data.clone(),
            compressed_data,
        }
    }

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

    /// Get compression ratio
    pub fn compression_ratio(&self, original_size: usize) -> f64 {
        let dict_size: usize = self.dictionary.iter().map(|seq| seq.len()).sum();
        let compressed_size = dict_size + self.compressed_data.len();
        compressed_size as f64 / original_size as f64
    }
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

    /// Build dictionary from a collection of sequences
    pub fn build_dictionary(&mut self, sequences: &[Vec<i64>]) -> Result<()> {
        let mut subsequence_counts = HashMap::new();

        // Count all subsequences of minimum length or longer
        for sequence in sequences {
            for length in self.min_length..=sequence.len() {
                for start in 0..=(sequence.len() - length) {
                    let subseq = sequence[start..start + length].to_vec();
                    *subsequence_counts.entry(subseq).or_insert(0) += 1;
                }
            }
        }

        // Find subsequences that appear frequently enough and are profitable
        let mut candidates: Vec<_> = subsequence_counts
            .into_iter()
            .filter(|(subseq, count)| {
                *count >= self.min_frequency && savings_potential(subseq.len(), *count) > 0
            })
            .collect();

        // Sort by savings potential (descending)
        candidates.sort_by(|a, b| {
            let savings_a = savings_potential(a.0.len(), a.1);
            let savings_b = savings_potential(b.0.len(), b.1);
            savings_b.cmp(&savings_a)
        });

        // Greedily select non-overlapping dictionary entries
        self.dictionary.clear();
        for (subseq, _count) in candidates {
            if !self.conflicts_with_existing(&subseq) {
                self.dictionary.push(subseq);
            }
        }

        Ok(())
    }

    /// Compress a single sequence using the built dictionary
    pub fn compress(&self, sequence: &[i64]) -> CompressedSequence {
        let mut compressed_data = Vec::new();
        let mut i = 0;

        while i < sequence.len() {
            let mut found_match = false;

            // Try to find the longest dictionary match starting at position i
            for (dict_index, dict_entry) in self.dictionary.iter().enumerate() {
                if i + dict_entry.len() <= sequence.len()
                    && sequence[i..i + dict_entry.len()] == *dict_entry
                {
                    // Found a match - encode as negative reference
                    compressed_data.push(-(dict_index as i64 + 1));
                    i += dict_entry.len();
                    found_match = true;
                    break;
                }
            }

            if !found_match {
                // No dictionary match - store raw value
                compressed_data.push(sequence[i]);
                i += 1;
            }
        }

        CompressedSequence::new(self.dictionary.clone(), compressed_data)
    }

    /// Check if a subsequence conflicts with existing dictionary entries
    fn conflicts_with_existing(&self, subseq: &[i64]) -> bool {
        // Simple conflict detection - could be more sophisticated
        for existing in &self.dictionary {
            if existing.iter().any(|&x| subseq.contains(&x)) {
                return true;
            }
        }
        false
    }

    /// Decompress a compressed sequence using this dictionary
    pub fn decompress(&self, compressed: &CompressedSequence) -> Vec<i64> {
        compressed.decompress()
    }

    /// Get statistics about this dictionary compressor
    pub fn stats(&self) -> DictionaryStats {
        DictionaryStats {
            dictionary_size: self.dictionary.len(),
            total_entries: self.dictionary.iter().map(|seq| seq.len()).sum(),
            num_subsequences: self.dictionary.len(),
        }
    }
}

/// Statistics about dictionary compression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DictionaryStats {
    pub dictionary_size: usize,
    pub total_entries: usize,
    pub num_subsequences: usize,
}

/// Calculate potential savings from dictionary compression
/// Returns number of block IDs that could be saved
pub fn savings_potential(subseq_length: usize, frequency: usize) -> usize {
    if frequency < 2 {
        return 0;
    }

    let total_original_size = subseq_length * frequency;
    let dictionary_cost = subseq_length + 1; // subsequence + metadata
    let reference_cost = frequency * 2; // each reference takes ~2 bytes

    if total_original_size > dictionary_cost + reference_cost {
        total_original_size - dictionary_cost - reference_cost
    } else {
        0
    }
}

impl Default for DictionaryCompressor {
    fn default() -> Self {
        Self::new(3, 2) // Default: min length 3, min frequency 2
    }
}