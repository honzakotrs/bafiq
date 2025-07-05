pub mod builder;
pub mod compressed;
pub mod format;
pub mod strategies;

// Re-export the main types from their respective modules
pub use builder::{BuildStrategy, IndexBuilder};
pub use compressed::CompressedFlagIndex;
pub use format::{IndexManager, SerializableIndex};
