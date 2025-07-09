pub mod delta;
pub mod sparse;

// Re-export the main types
pub use delta::{DeltaDecoder, DeltaEncoder};
pub use sparse::SparseStorage;
