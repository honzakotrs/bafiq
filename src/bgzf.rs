// Minimal imports for BGZF constants only

/// BGZF block format constants
pub const BGZF_HEADER_SIZE: usize = 18;
pub const BGZF_FOOTER_SIZE: usize = 8;
pub const BGZF_BLOCK_MAX_SIZE: usize = 65536;

/// Check if the header is a valid GZIP header
pub fn is_bgzf_header(header: &[u8]) -> bool {
    header[0..2] == [0x1f, 0x8b]
}
