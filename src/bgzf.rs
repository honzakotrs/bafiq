// Minimal imports for BGZF constants only

/// BGZF block format constants
pub const BGZF_BLOCK_MAX_SIZE: usize = 65536; // Maximum uncompressed BGZF block size
pub const BGZF_HEADER_SIZE: usize = 18;
pub const BGZF_FOOTER_SIZE: usize = 8;
// Removed unused BGZF parsing utilities, thread-local decompressor, and convenience functions
// The active strategies use their own optimized decompression approaches
