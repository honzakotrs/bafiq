use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparseStorage<T> {
    /// Map from flag value to sparse index
    flag_to_index: std::collections::HashMap<u16, usize>,
    /// Actual flag values that are used (in sparse index order)
    used_flags: Vec<u16>,
    /// Data for each used flag (in sparse index order)
    data: Vec<T>,
}

impl<T> SparseStorage<T> {
    /// Create new sparse storage with the given flag values
    pub fn new(flags: Vec<u16>) -> Self {
        let flag_to_index = flags
            .iter()
            .enumerate()
            .map(|(i, &flag)| (flag, i))
            .collect();

        Self {
            flag_to_index,
            used_flags: flags,
            data: Vec::new(),
        }
    }

    // Removed unused num_flags method

    /// Get sparse index for a flag value
    pub fn get_sparse_index(&self, flag: u16) -> Option<usize> {
        self.flag_to_index.get(&flag).copied()
    }

    /// Ensure data vector has enough capacity
    pub fn ensure_data_capacity(&mut self)
    where
        T: Default,
    {
        self.data.resize_with(self.used_flags.len(), T::default);
    }

    /// Get compression ratio (used flags / total possible flags)
    pub fn compression_ratio(&self) -> f64 {
        self.used_flags.len() as f64 / 65536.0
    }

    /// Get space savings percentage
    pub fn space_savings(&self) -> f64 {
        (1.0 - self.compression_ratio()) * 100.0
    }

    /// Get the used flags
    pub fn used_flags(&self) -> &[u16] {
        &self.used_flags
    }

    /// Get data for a specific flag
    pub fn get_data(&self, flag: u16) -> Option<&T> {
        self.get_sparse_index(flag)
            .and_then(|idx| self.data.get(idx))
    }

    /// Create sparse storage from a flag index
    pub fn from_flag_index(flag_index: &crate::FlagIndex) -> Self {
        let flags: Vec<u16> = flag_index.bins().iter().map(|bin| bin.bin).collect();
        Self::new(flags)
    }

    /// Set data for a specific flag
    pub fn set_data(&mut self, flag: u16, data: T) -> Result<()>
    where
        T: Default,
    {
        if let Some(idx) = self.get_sparse_index(flag) {
            // Ensure data vector is large enough
            if self.data.len() <= idx {
                self.data.resize_with(idx + 1, T::default);
            }
            self.data[idx] = data;
            Ok(())
        } else {
            Err(anyhow!("Flag not found in sparse storage"))
        }
    }

    /// Get statistics about this sparse storage
    pub fn stats(&self) -> SparseStats {
        SparseStats {
            used_flags: self.used_flags.len(),
            total_possible_flags: 65536,
            compression_ratio: self.compression_ratio(),
            space_savings_percent: self.space_savings(),
        }
    }
}

/// Statistics about sparse storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparseStats {
    pub used_flags: usize,
    pub total_possible_flags: usize,
    pub compression_ratio: f64,
    pub space_savings_percent: f64,
}

impl<T> Default for SparseStorage<T> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}
