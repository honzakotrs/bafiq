pub mod delta;
pub mod dictionary;
pub mod sparse;

// Re-export the main types
pub use delta::{DeltaDecoder, DeltaEncoder};
pub use dictionary::{
    CompressedSequence, DictionaryCompressor,
};
pub use sparse::SparseStorage;