use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand, ValueEnum};
use rust_htslib::bam::{Format, Read as BamRead, Writer};
use std::path::{Path, PathBuf};

use bafiq::{BuildStrategy, FlagIndex, IndexBuilder, IndexManager, SerializableIndex};

/// CLI-friendly strategy names that map to BuildStrategy
#[derive(Debug, Clone, ValueEnum)]
pub enum CliStrategy {
    /// Sequential processing - single-threaded baseline for measuring parallel benefits
    #[value(name = "sequential")]
    Sequential,
    /// Streaming parallel processing - simplest high-performance producer-consumer (3.433s)
    #[value(name = "parallel-streaming")]
    ParallelStreaming,

    /// Optimized parallel chunk streaming - producer-consumer with batching (3.709s)
    #[value(name = "parallel-chunk-streaming")]
    ParallelChunkStreaming,
    /// Streaming evolution with work-stealing - hybrid producer-consumer + work-stealing (3.609s)
    #[value(name = "rayon-streaming-optimized")]
    RayonStreamingOptimized,
    /// Wait-free processing - fastest performing approach (3.409s)
    #[value(name = "rayon-wait-free")]
    RayonWaitFree,
}

impl From<CliStrategy> for BuildStrategy {
    fn from(cli_strategy: CliStrategy) -> Self {
        match cli_strategy {
            CliStrategy::Sequential => BuildStrategy::Sequential,
            CliStrategy::ParallelStreaming => BuildStrategy::ParallelStreaming,
            CliStrategy::ParallelChunkStreaming => BuildStrategy::ParallelChunkStreaming,
            CliStrategy::RayonStreamingOptimized => BuildStrategy::RayonStreamingOptimized,
            CliStrategy::RayonWaitFree => BuildStrategy::RayonWaitFree,
        }
    }
}

/// Named flags that map to specific bits in the SAM flag.
/// We follow common bits used by samtools:
///   - read-unmapped (0x4)
///   - mate-unmapped (0x8)
///   - pcr-dup (0x400)
///   - secondary (0x100)
/// etc.
#[derive(Debug, Clone, Copy)]
struct NamedFlag {
    pub name: &'static str,
    /// Bits to *require* if user passes `--flag <NAME>`
    pub bits_set: u16,
    /// Bits to *forbid* if user passes `--no-flag <NAME>`
    pub bits_forbid: u16,
}

static NAMED_FLAGS: &[NamedFlag] = &[
    NamedFlag {
        name: "read-unmapped",
        bits_set: 0x4,
        bits_forbid: 0x4,
    },
    NamedFlag {
        name: "mate-unmapped",
        bits_set: 0x8,
        bits_forbid: 0x8,
    },
    NamedFlag {
        name: "pcr-dup",
        bits_set: 0x400,
        bits_forbid: 0x400,
    },
    NamedFlag {
        name: "secondary",
        bits_set: 0x100,
        bits_forbid: 0x100,
    },
];

fn find_named_flag(name: &str) -> Option<NamedFlag> {
    NAMED_FLAGS.iter().copied().find(|nf| nf.name == name)
}

/// Each subcommand (count/view) can specify:
/// - A single BAM/CRAM input
/// - Optional `-f INT` and `-F INT` for required/forbidden bits
/// - Repeated `--flag <NAME>` or `--no-flag <NAME>`
#[derive(Debug, Parser)]
pub struct SharedArgs {
    /// Include only reads with *all* bits in this INT set (like samtools -f)
    #[arg(short = 'f', long = "include-flags", default_value = "0")]
    pub include_flags: u16,

    /// Exclude reads with *any* bits in this INT (like samtools -F)
    #[arg(short = 'F', long = "exclude-flags", default_value = "0")]
    pub exclude_flags: u16,

    /// Named flags to require (e.g. --flag read-unmapped)
    #[arg(long = "flag", value_name = "NAME", num_args = 0..)]
    pub flags_to_include: Vec<String>,

    /// Named flags to exclude (e.g. --no-flag read-unmapped)
    #[arg(long = "no-flag", value_name = "NAME", num_args = 0..)]
    pub flags_to_exclude: Vec<String>,

    /// The input BAM/CRAM file
    pub input: PathBuf,
}

#[derive(Debug, Parser)]
pub struct IndexArgs {
    /// The input BAM/CRAM file
    pub input: PathBuf,

    /// Index building strategy
    #[arg(
        long = "strategy",
        value_enum,
        default_value = "rayon-wait-free",
        help = "Index building strategy to use (default: wait-free for maximum performance)"
    )]
    pub strategy: CliStrategy,

    /// Enable index compression (slower build, smaller files)
    #[arg(
        short = 'c',
        long = "compress-index",
        help = "Apply compression to reduce index file size"
    )]
    pub compress_index: bool,
}

impl SharedArgs {
    /// Combine the integer flags and named flags into `(required_bits, forbidden_bits)`.
    /// Samtools logic:
    ///   - "-f X" means all bits in X must be set.
    ///   - "-F X" means none of the bits in X may be set.
    ///
    /// Then we also merge in the named flags (like `--flag read-unmapped` => 0x4 in required_bits).
    pub fn gather_bits(&self) -> Result<(u16, u16)> {
        let mut required_bits = self.include_flags;
        let mut forbidden_bits = self.exclude_flags;

        // Merge named flags to include
        for name in &self.flags_to_include {
            if let Some(nf) = find_named_flag(name) {
                required_bits |= nf.bits_set;
            } else {
                return Err(anyhow!("Unrecognized named flag: {}", name));
            }
        }

        // Merge named flags to exclude
        for name in &self.flags_to_exclude {
            if let Some(nf) = find_named_flag(name) {
                forbidden_bits |= nf.bits_forbid;
            } else {
                return Err(anyhow!("Unrecognized named flag: {}", name));
            }
        }

        Ok((required_bits, forbidden_bits))
    }
}

/// The top-level CLI definition with subcommands.
#[derive(Debug, Subcommand)]
enum Commands {
    /// Count how many reads match the given flag criteria
    Count(SharedArgs),

    /// View (i.e., retrieve/print) reads that match the given flag criteria (SAM output to stdout)
    View(SharedArgs),

    /// Build the index for the given BAM/CRAM file
    Index(IndexArgs),

    /// Query BAM file with automatic caching
    Query {
        /// BAM file to query
        input: PathBuf,

        /// Required bits (hex format, e.g., 0x4 for unmapped)
        #[arg(long)]
        required: Option<String>,

        /// Forbidden bits (hex format, e.g., 0x400 for non-duplicates)
        #[arg(long)]
        forbidden: Option<String>,

        /// Force rebuild index (ignore cache)
        #[arg(long)]
        force_rebuild: bool,

        /// Show unmapped reads
        #[arg(long)]
        unmapped: bool,

        /// Show mapped reads only
        #[arg(long)]
        mapped: bool,

        /// Show first in pair
        #[arg(long)]
        first_in_pair: bool,

        /// Show second in pair  
        #[arg(long)]
        second_in_pair: bool,

        /// Show PCR duplicates
        #[arg(long)]
        duplicates: bool,

        /// Show non-duplicates
        #[arg(long)]
        non_duplicates: bool,
    },

    /// Fast count (like samtools view -c -f 0x4) - no index building
    FastCount {
        /// BAM file to scan
        input: PathBuf,

        /// Required bits (hex format, e.g., 0x4 for unmapped)
        #[arg(long)]
        required: Option<String>,

        /// Forbidden bits (hex format, e.g., 0x400 for non-duplicates)
        #[arg(long)]
        forbidden: Option<String>,

        /// Show unmapped reads
        #[arg(long)]
        unmapped: bool,

        /// Show mapped reads only
        #[arg(long)]
        mapped: bool,

        /// Show first in pair
        #[arg(long)]
        first_in_pair: bool,

        /// Show second in pair  
        #[arg(long)]
        second_in_pair: bool,

        /// Show PCR duplicates
        #[arg(long)]
        duplicates: bool,

        /// Show non-duplicates
        #[arg(long)]
        non_duplicates: bool,

        /// Use experimental dual-direction scanning (scan from both ends simultaneously)
        #[arg(long)]
        dual_direction: bool,

        /// Use multi-threaded BGZF decompression (like samtools) - experimental
        #[arg(long)]
        mt_decomp: bool,

        /// Use simplified parallel decompression (Rayon-based)
        #[arg(long)]
        simple_parallel: bool,

        /// Use thread pool parallel decompression (chunked)
        #[arg(long)]
        thread_pool: bool,
    },

    /// Load and query from saved index
    LoadIndex {
        /// Saved index file
        input: PathBuf,

        /// Required bits (hex format)
        #[arg(long)]
        required: Option<String>,

        /// Forbidden bits (hex format)
        #[arg(long)]
        forbidden: Option<String>,
    },

    /// Clear saved index for a BAM file
    ClearIndex(IndexArgs),

    /// Show index information and status
    IndexInfo(IndexArgs),
}

#[derive(Debug, Parser)]
#[command(name = "bafiq", author, version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Commands::Count(args) => cmd_count(args),
        Commands::View(args) => cmd_view(args),
        Commands::Index(args) => cmd_index(args),
        Commands::Query {
            input,
            required,
            forbidden,
            force_rebuild,
            unmapped,
            mapped,
            first_in_pair,
            second_in_pair,
            duplicates,
            non_duplicates,
        } => cmd_query(
            input,
            required,
            forbidden,
            force_rebuild,
            unmapped,
            mapped,
            first_in_pair,
            second_in_pair,
            duplicates,
            non_duplicates,
        ),
        Commands::FastCount {
            input,
            required,
            forbidden,
            unmapped,
            mapped,
            first_in_pair,
            second_in_pair,
            duplicates,
            non_duplicates,
            dual_direction,
            mt_decomp,
            simple_parallel,
            thread_pool,
        } => cmd_fast_count(
            input,
            required,
            forbidden,
            unmapped,
            mapped,
            first_in_pair,
            second_in_pair,
            duplicates,
            non_duplicates,
            dual_direction,
            mt_decomp,
            simple_parallel,
            thread_pool,
        ),
        Commands::LoadIndex {
            input,
            required,
            forbidden,
        } => cmd_load_index(input, required, forbidden),
        Commands::ClearIndex(args) => cmd_clear_index(args),
        Commands::IndexInfo(args) => cmd_index_info(args),
    }
}

/// Get the index path by appending ".bfi" to the original path
fn get_index_path(input: &Path) -> PathBuf {
    let mut index_path = input.to_path_buf();
    let extension = index_path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("");

    // Append .bfi to the original extension (.bam.bfi or .cram.bfi)
    index_path.set_extension(format!("{}.bfi", extension));
    index_path
}

/// `bafiq index <input.bam>`
/// Now uses streaming parallel processing by default for optimal performance
fn cmd_index(args: IndexArgs) -> Result<()> {
    let index_path = get_index_path(&args.input);
    let strategy: BuildStrategy = args.strategy.into();

    eprintln!("Building index using strategy: {:?}...", strategy);
    eprintln!("   Input: {:?}", args.input);
    eprintln!("   Output: {:?}", index_path);

    if args.compress_index {
        eprintln!("   Compression: ENABLED (slower build, smaller files)");

        // Use IndexManager for compressed output
        let index_manager = IndexManager::new();
        let input_str = args
            .input
            .to_str()
            .ok_or_else(|| anyhow!("Invalid file path"))?;
        let serializable_index = index_manager.load_or_build(input_str, true)?; // force rebuild

        // Save compressed index
        serializable_index.save_to_file(&index_path)?;
        let format_info = serializable_index.get_format_info();
        eprintln!("Compressed index saved: {:?}", index_path);
        eprintln!(
            "   Compression ratio: {:.2}x",
            format_info.compression_ratio
        );
    } else {
        eprintln!("   Compression: DISABLED (faster build, larger files)");

        // Use fast uncompressed path
        let builder = IndexBuilder::with_strategy(strategy);
        let index = builder.build(&args.input)?;

        eprintln!("Index built successfully. Saving to file...");
        index.save_to_file(&index_path)?;
        eprintln!("Index saved to: {:?}", index_path);
    }

    Ok(())
}

/// `bafiq count [options] <input.bam>`
fn cmd_count(args: SharedArgs) -> Result<()> {
    let (required_bits, forbidden_bits) = args.gather_bits()?;
    let index_path = get_index_path(&args.input);

    if !index_path.exists() {
        cmd_index(IndexArgs {
            input: args.input,
            strategy: CliStrategy::ParallelStreaming,
            compress_index: false,
        })?;
    };

    eprintln!("Loading existing index from: {:?}", index_path);
    let index = FlagIndex::from_file(&index_path)?;

    let count = index.count(required_bits, forbidden_bits);
    println!(
        "Count of reads [required=0x{:X}, forbidden=0x{:X}]: {}",
        required_bits, forbidden_bits, count
    );

    Ok(())
}

/// `bafiq view [options] <input.bam>`
/// Outputs matching reads in SAM format on stdout.
fn cmd_view(args: SharedArgs) -> Result<()> {
    let (required_bits, forbidden_bits) = args.gather_bits()?;
    let index_path = get_index_path(&args.input);

    let index = if index_path.exists() {
        eprintln!("Loading existing index from: {:?}", index_path);
        FlagIndex::from_file(&index_path)?
    } else {
        eprintln!("Building index from file: {:?}", args.input);
        let index = FlagIndex::from_path(&args.input)?;
        eprintln!("Saving index to: {:?}", index_path);
        index.save_to_file(&index_path)?;
        index
    };

    // Open the file again just to extract the header
    let tmp_reader = rust_htslib::bam::Reader::from_path(&args.input)?;
    let header = rust_htslib::bam::Header::from_template(tmp_reader.header());
    // let header = tmp_reader.header().to_owned();

    // Create a SAM writer to stdout
    let mut writer = Writer::from_stdout(&header, Format::Sam)?;

    // Retrieve matching reads
    index.retrieve_reads(&args.input, required_bits, forbidden_bits, &mut writer)?;

    Ok(())
}

fn cmd_query(
    input: PathBuf,
    required: Option<String>,
    forbidden: Option<String>,
    force_rebuild: bool,
    unmapped: bool,
    mapped: bool,
    first_in_pair: bool,
    second_in_pair: bool,
    duplicates: bool,
    non_duplicates: bool,
) -> Result<()> {
    use std::time::Instant;

    eprintln!("Querying BAM file with automatic caching...");
    eprintln!("   Input: {:?}", input);

    // Parse flag requirements
    let (required_bits, forbidden_bits) = if let (Some(req), Some(forb)) = (required, forbidden) {
        let req_bits = parse_hex_flags(&req)?;
        let forb_bits = parse_hex_flags(&forb)?;
        (req_bits, forb_bits)
    } else {
        // Use convenience flags
        let mut req_bits = 0u16;
        let mut forb_bits = 0u16;

        if unmapped {
            req_bits |= 0x4;
        }
        if mapped {
            forb_bits |= 0x4;
        }
        if first_in_pair {
            req_bits |= 0x40;
        }
        if second_in_pair {
            req_bits |= 0x80;
        }
        if duplicates {
            req_bits |= 0x400;
        }
        if non_duplicates {
            forb_bits |= 0x400;
        }

        (req_bits, forb_bits)
    };

    eprintln!(
        "   Query: required=0x{:x}, forbidden=0x{:x}",
        required_bits, forbidden_bits
    );

    // Load or build index with automatic saving
    let index_manager = IndexManager::new();
    let input_str = input.to_str().ok_or_else(|| anyhow!("Invalid file path"))?;

    let start = Instant::now();
    let serializable_index = index_manager.load_or_build(input_str, force_rebuild)?;
    let load_time = start.elapsed();

    // Get format info
    let format_info = serializable_index.get_format_info();
    eprintln!("   Index format: {}", format_info.format_type);
    eprintln!("   Load time: {:.3}ms", load_time.as_secs_f64() * 1000.0);

    // Query the index
    let start = Instant::now();
    let index = serializable_index.get_index();
    let result = index.count(required_bits, forbidden_bits);
    let query_time = start.elapsed();

    eprintln!("Query Results:");
    eprintln!("   Matching reads: {}", result);
    eprintln!("   Query time: {:.3}ms", query_time.as_secs_f64() * 1000.0);
    eprintln!(
        "   Total time: {:.3}ms",
        (load_time + query_time).as_secs_f64() * 1000.0
    );

    Ok(())
}

fn cmd_load_index(
    input: PathBuf,
    required: Option<String>,
    forbidden: Option<String>,
) -> Result<()> {
    use std::time::Instant;

    eprintln!("Loading saved index...");
    eprintln!("   Input: {:?}", input);

    // Load index
    let start = Instant::now();
    let serializable_index = SerializableIndex::load_from_file(&input)?;
    let load_time = start.elapsed();

    // Show index info
    let format_info = serializable_index.get_format_info();
    let cache_info = serializable_index.get_cache_info();

    eprintln!("Index loaded successfully!");
    eprintln!("   Format: {}", format_info.format_type);
    eprintln!("   Compression: {:.2}x", format_info.compression_ratio);
    eprintln!("   Source: {}", cache_info.source_path);
    eprintln!("   Load time: {:.3}ms", load_time.as_secs_f64() * 1000.0);

    // If query parameters provided, run query
    if let (Some(req), Some(forb)) = (required, forbidden) {
        let required_bits = parse_hex_flags(&req)?;
        let forbidden_bits = parse_hex_flags(&forb)?;

        eprintln!(
            "Querying: required=0x{:x}, forbidden=0x{:x}",
            required_bits, forbidden_bits
        );

        let start = Instant::now();
        let index = serializable_index.get_index();
        let result = index.count(required_bits, forbidden_bits);
        let query_time = start.elapsed();

        eprintln!("Query Results:");
        eprintln!("   Matching reads: {}", result);
        eprintln!("   Query time: {:.3}ms", query_time.as_secs_f64() * 1000.0);
    }

    Ok(())
}

fn cmd_clear_index(args: IndexArgs) -> Result<()> {
    let input_str = args
        .input
        .to_str()
        .ok_or_else(|| anyhow!("Invalid file path"))?;
    let index_manager = IndexManager::new();
    index_manager.clear_index(input_str)?;
    Ok(())
}

fn cmd_index_info(args: IndexArgs) -> Result<()> {
    eprintln!("Index Information");
    eprintln!("{}", "=".repeat(50));

    let input_str = args
        .input
        .to_str()
        .ok_or_else(|| anyhow!("Invalid file path"))?;
    let index_manager = IndexManager::new();
    let index_path = index_manager.get_index_path(input_str);

    eprintln!("   BAM file: {}", input_str);
    eprintln!("   Index path: {}", index_path);

    // Check if BAM file exists
    if !Path::new(input_str).exists() {
        eprintln!("   BAM file does not exist");
        return Ok(());
    }

    let bam_metadata = std::fs::metadata(input_str)?;
    eprintln!(
        "   BAM size: {:.1} MB",
        bam_metadata.len() as f64 / 1_048_576.0
    );

    // Check index status
    if Path::new(&index_path).exists() {
        match SerializableIndex::load_from_file(&index_path) {
            Ok(saved_index) => {
                let format_info = saved_index.get_format_info();
                let cache_info = saved_index.get_cache_info();

                eprintln!("   Index exists");
                eprintln!("   Index format: {}", format_info.format_type);
                eprintln!(
                    "   Index compression: {:.2}x",
                    format_info.compression_ratio
                );

                match saved_index.is_stale() {
                    Ok(false) => eprintln!("   Index is fresh"),
                    Ok(true) => eprintln!("   Index is stale (BAM file was modified)"),
                    Err(e) => eprintln!("   Index validation failed: {}", e),
                }

                eprintln!(
                    "   Created: {}",
                    format_timestamp(cache_info.created_timestamp)
                );
            }
            Err(e) => {
                eprintln!("   Index file exists but failed to load: {}", e);
            }
        }
    } else {
        eprintln!("   No index found");
    }

    Ok(())
}

fn parse_hex_flags(hex_str: &str) -> Result<u16> {
    let cleaned = hex_str.trim_start_matches("0x");
    u16::from_str_radix(cleaned, 16).map_err(|_| anyhow!("Invalid hex format: {}", hex_str))
}

fn format_timestamp(timestamp: u64) -> String {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    let system_time = UNIX_EPOCH + Duration::from_secs(timestamp);
    match system_time.duration_since(SystemTime::now()) {
        Ok(future) => format!("in {:.1}h", future.as_secs_f64() / 3600.0),
        Err(past) => {
            let duration = past.duration();
            if duration.as_secs() < 60 {
                format!("{:.0}s ago", duration.as_secs_f64())
            } else if duration.as_secs() < 3600 {
                format!("{:.1}m ago", duration.as_secs_f64() / 60.0)
            } else if duration.as_secs() < 86400 {
                format!("{:.1}h ago", duration.as_secs_f64() / 3600.0)
            } else {
                format!("{:.1}d ago", duration.as_secs_f64() / 86400.0)
            }
        }
    }
}

fn cmd_fast_count(
    input: PathBuf,
    required: Option<String>,
    forbidden: Option<String>,
    unmapped: bool,
    mapped: bool,
    first_in_pair: bool,
    second_in_pair: bool,
    duplicates: bool,
    non_duplicates: bool,
    dual_direction: bool,
    mt_decomp: bool,
    simple_parallel: bool,
    thread_pool: bool,
) -> Result<()> {
    let input_str = input.to_str().ok_or_else(|| anyhow!("Invalid file path"))?;

    // Parse flags same way as cmd_query
    let mut required_flags = 0u16;
    let mut forbidden_flags = 0u16;

    // Parse hex flags if provided
    if let Some(req_str) = &required {
        required_flags |= parse_hex_flags(req_str)?;
    }
    if let Some(forb_str) = &forbidden {
        forbidden_flags |= parse_hex_flags(forb_str)?;
    }

    // Apply convenience flags
    if unmapped {
        required_flags |= 0x4; // UNMAPPED
    }
    if mapped {
        forbidden_flags |= 0x4; // NOT UNMAPPED
    }
    if first_in_pair {
        required_flags |= 0x40; // FIRST_IN_PAIR
    }
    if second_in_pair {
        required_flags |= 0x80; // SECOND_IN_PAIR
    }
    if duplicates {
        required_flags |= 0x400; // DUPLICATE
    }
    if non_duplicates {
        forbidden_flags |= 0x400; // NOT DUPLICATE
    }

    if simple_parallel {
        eprintln!("Fast count mode - SIMPLE PARALLEL (Rayon-based)");
    } else if thread_pool {
        eprintln!("Fast count mode - THREAD POOL (chunked)");
    } else if mt_decomp {
        eprintln!("Fast count mode - MULTI-THREADED DECOMPRESSION (experimental)");
    } else if dual_direction {
        eprintln!("Fast count mode - DUAL DIRECTION SCAN (experimental)");
    } else {
        eprintln!("Fast count mode (no index building)");
    }
    eprintln!("   File: {}", input_str);
    eprintln!("   Required flags: 0x{:x}", required_flags);
    eprintln!("   Forbidden flags: 0x{:x}", forbidden_flags);

    // Use appropriate scan mode
    let start = std::time::Instant::now();
    let count = if simple_parallel {
        bafiq::benchmark::count_flags_simple_parallel_decompression(
            input_str,
            required_flags,
            forbidden_flags,
        )?
    } else if thread_pool {
        bafiq::benchmark::count_flags_libdeflate_thread_pool(
            input_str,
            required_flags,
            forbidden_flags,
        )?
    } else if mt_decomp {
        bafiq::benchmark::count_flags_multithreaded_decompression(
            input_str,
            required_flags,
            forbidden_flags,
        )?
    } else {
        let builder = IndexBuilder::new();
        if dual_direction {
            builder.scan_count_dual_direction(input_str, required_flags, forbidden_flags)?
        } else {
            builder.scan_count(input_str, required_flags, forbidden_flags)?
        }
    };
    let scan_time = start.elapsed();

    println!("{}", count);
    eprintln!("Scan completed in {:.3}s", scan_time.as_secs_f64());

    Ok(())
}
